# src/services/market_data/collector.py

import asyncio
from typing import Any, Dict, Set

from ...adapters.registry import ExchangeAdapterRegistry
from .progress import CollectionProgress
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...utils.logger import LoggerSetup
from ...utils.time import TimeUtils
from ...utils.domain_types import Timeframe, Timestamp
from ...core.models import SymbolInfo
from ...core.exceptions import ServiceError, ValidationError
from ...utils.retry import RetryConfig, RetryStrategy

logger = LoggerSetup.setup(__name__)

class DataCollector:
    """
    Handles all forms of market data collection through command-driven interface.

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
                 coordinator: ServiceCoordinator,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5,
                 batch_size: int = 1000):

        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._coordinator = coordinator
        self._base_timeframe = base_timeframe
        self._batch_size = batch_size

        # Collection management
        self._collection_queue: asyncio.Queue[Dict[str,Any]] = asyncio.Queue(maxsize=1000)
        self._collection_lock = asyncio.Lock()
        self._symbol_progress: Dict[SymbolInfo, CollectionProgress] = {}
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
        asyncio.create_task(self._register_command_handlers())

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

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior"""
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            ServiceError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )

    async def _register_command_handlers(self) -> None:
        """Register handlers for collection-related commands"""
        handlers = {
            MarketDataCommand.COLLECTION_START: self._handle_collection_start,
            MarketDataCommand.SYMBOL_DELISTED: self._handle_symbol_delisted
        }

        for command, handler in handlers.items():
            await self._coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _handle_collection_start(self, command: Command) -> None:
        """Handle start of data collection request"""
        symbol = command.params["symbol"]
        start_time = command.params["start_time"]
        end_time = command.params["end_time"]
        timestamp = command.params["timestamp"]
        context = command.params.get("context", {})

        async with self._collection_lock:
            if symbol not in self._processing_symbols:
                self._processing_symbols.add(symbol)

                if gap_size := context.get('gap_size'):
                    logger.info(f"Filling {gap_size} candles for {symbol}")
                else:
                    logger.info(f"Started data collection for {symbol}")

                self._symbol_progress[symbol] = CollectionProgress(
                    symbol=symbol,
                    start_time=timestamp
                )

                # Queue collection request
                await self._collection_queue.put({
                    "symbol": symbol,
                    "start_time": start_time,
                    "end_time": end_time
                })
            else:
                logger.debug(f"Collection already in progress for {symbol}")

    async def _handle_symbol_delisted(self, command: Command) -> None:
        """Handle symbol delisting by cleaning up data and cancelling collections"""
        symbol: SymbolInfo = command.params["symbol"]

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

                await self._symbol_repository.delete(processing_symbol)
                await self._kline_repository.delete_symbol_data(processing_symbol)

                logger.info(f"Cleaned up data for delisted symbol {processing_symbol}")
            else:
                logger.warning(f"Received delisting command for unknown symbol: {symbol}")

        except Exception as e:
            logger.error(f"Error handling symbol delisting for {symbol}: {e}")
            # Don't raise - we want to continue service operation even if cleanup fails

    async def _process_queue(self) -> None:
        """Process collection requests from queue"""
        while True:
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
                    # Report completion to BatchSynchronizer
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.COLLECTION_COMPLETE,
                        params={
                            "symbol": request["symbol"],
                            "start_time": request["start_time"],
                            "end_time": request["end_time"],
                            "timestamp": TimeUtils.get_current_timestamp()
                        }
                    ))

                except Exception as e:
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.COLLECTION_ERROR,
                        params={
                            "symbol": request["symbol"],
                            "error": str(e),
                            "error_type": e.__class__.__name__,
                            "timestamp": TimeUtils.get_current_timestamp(),
                            "retry_exhausted": True,  # Indicate retries handled internally
                            "context": {
                                "start_time": request["start_time"],
                                "end_time": request["end_time"],
                                "collection_type": request["type"],
                                "batch_size": self._batch_size,
                                "timeframe": self._base_timeframe.value
                            }
                        }
                    ))

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
        Collect data for specified time range with careful timestamp handling

        Cases to handle:
        1. Historical collection (end_time is current time)
        2. Gap filling (specific start/end range)
        3. Single interval collection
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
            retry_count = 0

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

                        # Move to next batch, ensuring no overlap
                        last_timestamp = klines[-1].timestamp
                        current_start = Timestamp(last_timestamp + interval_ms)
                    else:
                        # No valid data in this batch
                        current_start = batch_end

                except Exception as e:
                    should_retry, reason = self._retry_strategy.should_retry(retry_count, e)
                    if should_retry:
                        retry_count += 1
                        delay = self._retry_strategy.get_delay(retry_count)
                        logger.warning(
                            f"Retry {retry_count} for {symbol} "
                            f"batch {TimeUtils.from_timestamp(Timestamp(current_start))} "
                            f"after {delay:.2f}s: {e}"
                        )
                        await asyncio.sleep(delay)
                        continue  # Retry this batch
                    raise  # Non-retryable error

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