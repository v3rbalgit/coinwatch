import asyncio
from typing import Any, Dict, Set

from shared.core.models import SymbolInfo
from shared.core.exceptions import ServiceError, ValidationError
from shared.database.repositories.kline import KlineRepository
from shared.database.repositories.symbol import SymbolRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import (
    MessageType,
    KlineMessage,
    GapMessage,
    ErrorMessage
)
from shared.utils.domain_types import Timeframe, Timestamp
from shared.utils.logger import LoggerSetup
from shared.utils.progress import MarketDataProgress
from shared.utils.retry import RetryConfig, RetryStrategy
from shared.utils.time import TimeUtils
from .adapters.registry import ExchangeAdapterRegistry

logger = LoggerSetup.setup(__name__)

class DataCollector:
    """
    Handles all forms of market data collection through message-driven interface.

    Responsibilities:
    - Collects historical data for new symbols
    - Fills gaps in existing data
    - Handles data refresh requests
    - Reports collection progress
    """

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 message_broker: MessageBroker,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5,
                 batch_size: int = 1000):

        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._message_broker = message_broker
        self._base_timeframe = base_timeframe
        self._batch_size = batch_size

        # Collection management
        self._collection_queue: asyncio.Queue[Dict[str,Any]] = asyncio.Queue(maxsize=1000)
        self._collection_lock = asyncio.Lock()
        self._symbol_progress: Dict[SymbolInfo, MarketDataProgress] = {}
        self._processing_symbols: Set[SymbolInfo] = set()

        # Retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=5.0,
            max_delay=300.0,
            max_retries=3
        ))
        self._configure_retry_strategy()

        self._queue_processor = asyncio.create_task(self._process_queue())
        self._running = True

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior for data collection"""
        # Basic retryable errors
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            ServiceError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )

        # Configure specific delays for different error types
        self._retry_strategy.configure_error_delays({
            ConnectionError: RetryConfig(
                base_delay=10.0,     # Higher base delay for connection issues
                max_delay=300.0,
                max_retries=5,
                jitter_factor=0.2
            ),
            TimeoutError: RetryConfig(
                base_delay=5.0,      # Moderate delay for timeouts
                max_delay=120.0,
                max_retries=3,
                jitter_factor=0.1
            ),
            ServiceError: RetryConfig(
                base_delay=2.0,      # Fast retry for service errors
                max_delay=60.0,
                max_retries=3,
                jitter_factor=0.1
            )
        })

    async def start_collection(self, symbol: SymbolInfo, start_time: int, end_time: int, context: Dict[str, Any]) -> None:
        """Start data collection for a symbol"""
        async with self._collection_lock:
            if symbol not in self._processing_symbols:
                self._processing_symbols.add(symbol)

                if gap_size := context.get('gap_size'):
                    logger.info(f"Filling {gap_size} candles for {symbol}")
                else:
                    logger.info(f"Started data collection for {symbol}")

                self._symbol_progress[symbol] = MarketDataProgress(
                    symbol=symbol,
                    start_time=TimeUtils.get_current_datetime()
                )

                # Queue collection request
                await self._collection_queue.put({
                    "symbol": symbol,
                    "start_time": start_time,
                    "end_time": end_time,
                    "context": context or {}
                })
            else:
                logger.debug(f"Collection already in progress for {symbol}")

    async def handle_delisted(self, symbol: SymbolInfo) -> None:
        """Handle symbol delisting by cleaning up data and cancelling collections"""
        try:
            processing_symbol = next((s for s in self._processing_symbols if s.name == symbol.name), None)

            if not processing_symbol:
                symbol_record = await self._symbol_repository.get_symbol(symbol)
                if symbol_record:
                    processing_symbol = symbol

            if processing_symbol:
                logger.info(f"Processing delisting for {processing_symbol}")

                async with self._collection_lock:
                    if processing_symbol in self._processing_symbols:
                        self._processing_symbols.remove(processing_symbol)
                        logger.info(f"Removed delisted symbol from data collection {processing_symbol}")

                await self._symbol_repository.delete_symbol(processing_symbol)
                await self._kline_repository.delete_symbol_data(processing_symbol)

                logger.info(f"Cleaned up data for delisted symbol {processing_symbol}")
            else:
                logger.warning(f"Received delisting for unknown symbol: {symbol}")

        except Exception as e:
            logger.error(f"Error handling symbol delisting for {symbol}: {e}")
            # Don't raise - we want to continue service operation even if cleanup fails

    async def cleanup(self) -> None:
        """Cleanup background tasks and resources"""
        logger.info("Cleaning up DataCollector")
        self._running = False  # Signal process queue to stop

        # Cancel and await queue processor
        if self._queue_processor:
            self._queue_processor.cancel()
            try:
                await self._queue_processor
            except asyncio.CancelledError:
                pass
            self._queue_processor = None

        # Clear any pending collections
        while not self._collection_queue.empty():
            try:
                self._collection_queue.get_nowait()
                self._collection_queue.task_done()
            except asyncio.QueueEmpty:
                break

        self._processing_symbols.clear()
        logger.info("DataCollector cleanup completed")

    async def _process_queue(self) -> None:
        """
        Continuously processes market data collection requests from the queue.

        Handles each request by:
        - Collecting data via _collect_data()
        - Publishing collection status messages
        - Managing processing state and queue cleanup
        """
        while self._running:
            try:
                request = await self._collection_queue.get()
                try:
                    await self._collect_data(
                        request["symbol"],
                        request["start_time"],
                        request["end_time"],
                        self._batch_size,
                        self._base_timeframe
                    )

                    # Publish collection complete message
                    await self._message_broker.publish(
                        MessageType.COLLECTION_COMPLETE,
                        {
                            "service": "market_data",
                            "type": MessageType.COLLECTION_COMPLETE,
                            "symbol": request["symbol"].name,
                            "exchange": request["symbol"].exchange,
                            "start_time": request["start_time"],
                            "end_time": request["end_time"],
                            "timestamp": TimeUtils.get_current_timestamp()
                        }
                    )

                except Exception as e:
                    # Publish error message
                    await self._message_broker.publish(
                        MessageType.ERROR_REPORTED,
                        ErrorMessage(
                            service="market_data",
                            type=MessageType.ERROR_REPORTED,
                            timestamp=TimeUtils.get_current_timestamp(),
                            error_type="CollectionError",
                            severity="error",
                            message=str(e),
                            context={
                                "symbol": request["symbol"].name,
                                "exchange": request["symbol"].exchange,
                                "start_time": request["start_time"],
                                "end_time": request["end_time"],
                                "collection_type": request.get("context", {}).get("type"),
                                "batch_size": self._batch_size,
                                "timeframe": self._base_timeframe.value,
                                "retry_exhausted": True
                            }
                        ).dict()
                    )

                finally:
                    async with self._collection_lock:
                        self._processing_symbols.remove(request["symbol"])
                    self._collection_queue.task_done()

            except Exception as e:
                logger.error(f"Critical error in collection queue processing: {e}")
                await asyncio.sleep(1)

    async def _collect_data(self,
                       symbol: SymbolInfo,
                       start_time: Timestamp,
                       end_time: Timestamp,
                       limit: int,
                       timeframe: Timeframe) -> None:
        """
        Collects and stores kline/candlestick data for a given symbol and time range.

        Args:
            symbol (SymbolInfo): Trading pair symbol information
            start_time (Timestamp): Collection start timestamp in milliseconds
            end_time (Timestamp): Collection end timestamp in milliseconds
            limit (int): Maximum number of klines per request
            timeframe (Timeframe): Candlestick interval

        Raises:
            ValidationError: If end_time is not greater than start_time
            Exception: If data collection fails

        Notes:
            - Handles historical data collection, gap filling, and single interval collection
            - Aligns timestamps to interval boundaries
            - Implements retry logic for failed requests
            - Tracks collection progress
        """
        try:
            # Validate time range
            if end_time <= start_time:
                raise ValidationError(
                    f"Invalid time range: end_time ({TimeUtils.from_timestamp(end_time)}) "
                    f"must be greater than start_time ({TimeUtils.from_timestamp(start_time)})"
                )

            # Ensure symbol exists
            await self._symbol_repository.get_or_create(symbol)

            # Calculate intervals
            interval_ms = timeframe.to_milliseconds()
            current_time = TimeUtils.get_current_timestamp()

            # Align timestamps to interval boundaries
            aligned_start = start_time - (start_time % interval_ms)
            aligned_end = end_time - (end_time % interval_ms)

            # Calculate total candles for progress tracking
            total_candles = ((aligned_end - aligned_start) // interval_ms) + 1
            processed_candles = 0

            # Update progress with total
            async with self._collection_lock:
                progress = self._symbol_progress[symbol]
                progress.update(processed_candles, total_candles)

            # For historical collection, only collect up to last completed interval
            is_historical = aligned_end >= (current_time - interval_ms)
            if is_historical:
                last_complete = current_time - (current_time % interval_ms) - interval_ms
                aligned_end = min(aligned_end, last_complete)
                logger.debug(
                    f"Adjusted end time for historical collection: "
                    f"{TimeUtils.from_timestamp(Timestamp(aligned_end))}"
                )
            current_start = aligned_start
            attempt = 0

            while current_start < aligned_end:
                try:
                    # Calculate batch end respecting overall end time
                    batch_end = min(
                        aligned_end,
                        current_start + (limit * interval_ms)
                    )

                    # Check for cancellation
                    if symbol not in self._processing_symbols:
                        logger.info(f"Collection cancelled for {symbol}")
                        return

                    adapter = self._adapter_registry.get_adapter(symbol.exchange)
                    klines = await adapter.get_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_time=Timestamp(current_start),
                        end_time=Timestamp(batch_end),
                        limit=limit
                    )

                    if not klines:
                        break

                    # For historical collection, verify completeness
                    if is_historical:
                        klines = [k for k in klines
                                if TimeUtils.is_complete_interval(
                                    Timestamp(k.timestamp),
                                    interval_ms
                                )]

                    if klines:
                        # Store batch
                        processed = await self._kline_repository.insert_batch(
                            symbol,
                            timeframe,
                            [k.to_tuple() for k in klines]
                        )

                        processed_candles += processed
                        async with self._collection_lock:
                            progress.update(processed_candles)
                        logger.info(progress)

                        # Publish kline updates
                        for kline in klines:
                            await self._message_broker.publish(
                                MessageType.KLINE_UPDATED,
                                KlineMessage(
                                    service="market_data",
                                    type=MessageType.KLINE_UPDATED,
                                    timestamp=TimeUtils.get_current_timestamp(),
                                    symbol=symbol.name,
                                    exchange=symbol.exchange,
                                    timeframe=str(timeframe),
                                    kline_timestamp=kline.timestamp,
                                    open_price=float(kline.open_price),
                                    high_price=float(kline.high_price),
                                    low_price=float(kline.low_price),
                                    close_price=float(kline.close_price),
                                    volume=float(kline.volume),
                                    turnover=float(kline.turnover)
                                ).dict()
                            )

                        last_timestamp = klines[-1].timestamp
                        current_start = Timestamp(last_timestamp + interval_ms)
                    else:
                        # No valid data in this batch
                        current_start = batch_end

                except Exception as e:
                    should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                    if should_retry:
                        attempt += 1
                        # Get error-specific delay
                        delay = self._retry_strategy.get_delay(attempt, e)

                        logger.warning(
                            f"Retry {attempt} for {symbol} "
                            f"batch {TimeUtils.from_timestamp(Timestamp(current_start))} "
                            f"after {delay:.2f}s: {e}"
                        )
                        await asyncio.sleep(delay)
                        continue

                    raise  # Non-retryable error occurred

            if progress := self._symbol_progress.get(symbol):
                logger.info(progress.get_completion_summary(TimeUtils.get_current_datetime()))

            self._symbol_progress.pop(symbol, None)

        except Exception as e:
            logger.error(f"Collection failed for {symbol}: {e}")
            raise

    def get_collection_status(self) -> str:
        """Enhanced status reporting"""
        status = [
            "Historical Collection Status:",
            f"Active Collections: {len(self._processing_symbols)}",
            f"Pending Collections: {self._collection_queue.qsize()}",
            f"Batch Size: {self._batch_size}",
            "\nActive Progress:"
        ]

        for progress in sorted(self._symbol_progress.values()):
            status.append(f"  {progress}")

        return "\n".join(status)