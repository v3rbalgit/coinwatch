# src/services/market_data/collector.py

import asyncio
from typing import Optional, Set

from ...adapters.registry import ExchangeAdapterRegistry
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...utils.logger import LoggerSetup
from ...utils.time import from_timestamp, get_current_timestamp
from ...utils.domain_types import Timeframe, Timestamp
from ...core.models import SymbolInfo
from ...core.exceptions import ServiceError, ValidationError
from ...utils.error import ErrorTracker
from ...utils.retry import RetryConfig, RetryStrategy

logger = LoggerSetup.setup(__name__)

class HistoricalCollector:
    """Handles historical data collection for symbols"""

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 coordinator: ServiceCoordinator,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5,
                 max_retries: int = 3,
                 batch_size: int = 1000):

        # Core dependencies
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._coordinator = coordinator
        self._base_timeframe = base_timeframe
        self._max_retries = max_retries
        self._batch_size = batch_size

        # Task management
        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Collection management
        self._collection_queue: asyncio.Queue[SymbolInfo] = asyncio.Queue(maxsize=1000)
        self._processing_symbols: Set[str] = set()
        self._collection_lock = asyncio.Lock()

        # Error handling
        self._error_tracker = ErrorTracker()
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=300.0,
            max_retries=max_retries
        ))

        # Configure retry behavior
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            ServiceError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )

    async def start(self) -> None:
        """Start the collector"""
        self._active = True
        self._task = asyncio.create_task(self._process_queue())
        logger.info("Historical collector started")

    async def stop(self) -> None:
        """Stop collector with proper cleanup"""
        self._active = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Clean up any pending items
        while not self._collection_queue.empty():
            symbol = await self._collection_queue.get()
            async with self._collection_lock:
                self._processing_symbols.remove(symbol.name)
            self._collection_queue.task_done()
        logger.info("Historical collector stopped")

    async def add_symbol(self, symbol: SymbolInfo) -> None:
        """Add symbol for historical collection"""
        async with self._collection_lock:
            if symbol.name not in self._processing_symbols:
                self._processing_symbols.add(symbol.name)
                await self._collection_queue.put(symbol)

                # Notify about new collection
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.COLLECTION_STARTED,
                    params={
                        "symbol": symbol.name,
                        "start_time": symbol.launch_time
                    }
                ))

                logger.info(f"Added {symbol.name} to historical collection queue")
            else:
                logger.debug(f"Symbol {symbol.name} already in processing queue")

    async def _process_queue(self) -> None:
        """Process symbols in the queue with proper state management"""
        while self._active:
            try:
                symbol = await self._collection_queue.get()
                try:
                    await self._collect_historical_data(symbol, self._base_timeframe)

                    # Notify successful completion
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.COLLECTION_COMPLETE,
                        params={
                            "symbol": symbol.name,
                            "start_time": symbol.launch_time,
                            "end_time": get_current_timestamp()
                        }
                    ))
                except Exception as e:
                    await self._error_tracker.record_error(e, symbol.name, context="collection")
                    logger.error(f"Error collecting data for {symbol.name}: {e}")

                    # Notify about collection error
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.COLLECTION_ERROR,
                        params={
                            "symbol": symbol.name,
                            "error": str(e),
                            "error_type": e.__class__.__name__,
                            "context": {
                                "retry_count": self._error_tracker.get_error_frequency(
                                    e.__class__.__name__,
                                    window_minutes=60
                                ),
                                "process": "historical_collection",
                                "timestamp": get_current_timestamp()
                            }
                        }
                    ))

                finally:
                    async with self._collection_lock:
                        self._processing_symbols.remove(symbol.name)
                    self._collection_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Critical error in collection queue processing: {e}")
                await asyncio.sleep(1)

    async def _collect_historical_data(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Collect historical data with enhanced error handling and progress tracking"""
        retry_count = 0
        processed_candles = 0
        total_candles = None

        while True:
            try:
                # First ensure symbol exists in database
                await self._symbol_repository.get_or_create(symbol)

                # Get collection boundaries
                start_time = await self._kline_repository.get_latest_timestamp(
                    symbol,
                    timeframe
                )
                current_time = get_current_timestamp()
                interval_ms = timeframe.to_milliseconds()

                # Adjust to last completed interval
                last_complete_interval = current_time - (current_time % interval_ms) - interval_ms
                start_time = symbol.launch_time if not start_time else start_time + interval_ms

                # Calculate total expected candles
                total_candles = int((last_complete_interval - start_time) // interval_ms)

                logger.info(
                    f"Starting historical collection for {symbol.name} "
                    f"from {from_timestamp(start_time)} "
                    f"to {from_timestamp(last_complete_interval)}"
                )

                while start_time < last_complete_interval:
                    # Check if collection was cancelled
                    if symbol.name not in self._processing_symbols:
                        logger.info(f"Collection cancelled for {symbol.name}")
                        return

                    # Fetch batch of historical data
                    adapter = self._adapter_registry.get_adapter(symbol.exchange)
                    klines = await adapter.get_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_time=Timestamp(start_time),
                        limit=self._batch_size
                    )

                    if not klines:
                        break

                    # Only store completed candles
                    klines = [k for k in klines if k.timestamp <= last_complete_interval]

                    if klines:
                        # Store klines
                        await self._kline_repository.insert_batch(
                            symbol,
                            timeframe,
                            [k.to_tuple() for k in klines]
                        )

                        # Update progress
                        processed_candles += len(klines)
                        last_timestamp = klines[-1].timestamp

                        # Report progress
                        await self._coordinator.execute(Command(
                            type=MarketDataCommand.COLLECTION_PROGRESS,
                            params={
                                "symbol": symbol.name,
                                "processed": processed_candles,
                                "total": total_candles,
                                "last_timestamp": last_timestamp,
                                "context": {
                                    "timeframe": timeframe.value,
                                    "batch_size": self._batch_size
                                }
                            }
                        ))

                        start_time = last_timestamp + interval_ms

                # Collection complete
                logger.info(
                    f"Completed historical collection for {symbol.name} "
                    f"up to {from_timestamp(last_complete_interval)}"
                )
                break

            except Exception as e:
                # Handle retry logic
                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)
                if should_retry:
                    retry_count += 1
                    delay = self._retry_strategy.get_delay(retry_count)
                    logger.warning(
                        f"Retry {retry_count} for {symbol.name} after {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Failed to collect data for {symbol.name}: {reason}")
                    raise

    def get_collection_status(self) -> str:
        """Get current collection status"""
        status = ["Historical Collection Status:"]
        status.extend([
            f"Active Collections: {len(self._processing_symbols)}",
            f"Pending Collections: {self._collection_queue.qsize()}"
        ])
        return "\n".join(status)