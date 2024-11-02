# src/services/data_sync.py

import asyncio
import random
from typing import Optional, Set

from src.adapters.registry import ExchangeAdapterRegistry
from src.core.exceptions import ServiceError
from src.core.symbol_state import SymbolState, SymbolStateManager
from src.repositories.kline import KlineRepository
from src.repositories.symbol import SymbolRepository
from src.utils.time import from_timestamp, get_current_timestamp

from ..core.models import SymbolInfo
from ..utils.domain_types import Timeframe, Timestamp
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class HistoricalCollector:
    """Handles historical data collection for symbols"""

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 state_manager: SymbolStateManager,
                 max_retries: int = 3):
        self._collection_queue: asyncio.Queue[SymbolInfo] = asyncio.Queue(maxsize=1000)
        self._adapter_registry = adapter_registry
        self._symbol_repository = symbol_repository
        self._kline_repository = kline_repository
        self._state_manager = state_manager
        self._max_retries = max_retries
        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Add synchronization primitives
        self._processing_symbols: Set[SymbolInfo] = set()
        self._collection_lock = asyncio.Lock()

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
                self._processing_symbols.remove(symbol)
            self._collection_queue.task_done()
        logger.info("Historical collector stopped")

    async def add_symbol(self, symbol: SymbolInfo) -> None:
        """Add symbol for historical collection with duplicate prevention"""
        async with self._collection_lock:
            if symbol not in self._processing_symbols:
                await self._state_manager.add_symbol(symbol)
                self._processing_symbols.add(symbol)
                await self._collection_queue.put(symbol)
                logger.info(f"Added {symbol.name} ({symbol.exchange}) to historical collection queue")
            else:
                logger.debug(f"Symbol {symbol.name} ({symbol.exchange}) already in processing queue")

    async def _process_queue(self) -> None:
        """Process symbols in the queue with proper state management"""
        while self._active:
            try:
                symbol = await self._collection_queue.get()

                try:
                    # Update state to HISTORICAL before processing
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.HISTORICAL
                    )

                    await self._collect_historical_data(symbol, Timeframe.MINUTE_5)
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.READY
                    )
                    logger.info(f"Syncing {symbol.name} ({symbol.exchange})...")
                except Exception as e:
                    await self._state_manager.update_state(
                        symbol,
                        SymbolState.ERROR,
                        str(e)
                    )
                finally:
                    # Remove from processing set
                    async with self._collection_lock:
                        self._processing_symbols.remove(symbol)
                    self._collection_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in collection queue processing: {e}")
                await asyncio.sleep(1)

    async def _collect_historical_data(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Collect all historical data for a symbol"""
        adapter = self._adapter_registry.get_adapter(symbol.exchange)
        retry_count = 0

        while retry_count < self._max_retries:
            try:
                # First ensure symbol exists in database
                await self._symbol_repository.get_or_create(symbol)

                # Get start time based on launch or last data
                start_time = await self._kline_repository.get_latest_timestamp(symbol, timeframe)
                current_time = get_current_timestamp()

                if not start_time:
                    start_time = symbol.launch_time
                else:
                    start_time += timeframe.to_milliseconds()  # Ensure we start from the next timeframe

                logger.info(
                    f"Starting historical collection for {symbol.name} ({symbol.exchange})"
                    f"from {from_timestamp(start_time)} "
                    f"to {from_timestamp(current_time)}"
                )

                while start_time < current_time:
                    # Fetch batch of historical data
                    klines = await adapter.get_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_time=Timestamp(start_time),
                        limit=1000
                    )

                    if not klines:
                        break

                    # Store klines and update progress
                    await self._kline_repository.insert_batch(
                        symbol,
                        timeframe,
                        [k.to_tuple() for k in klines]
                    )

                    # Update sync progress using last timestamp from batch
                    last_timestamp = klines[-1].timestamp
                    await self._state_manager.update_sync_time(
                        symbol,
                        Timestamp(last_timestamp)
                    )

                    # Get symbol progress and update it
                    progress = await self._state_manager.get_progress(symbol)
                    if progress:
                        progress.update_progress(
                            Timestamp(current_time),
                            Timestamp(last_timestamp)
                            )
                        logger.info(f"Collection progress: {progress}")

                    start_time = last_timestamp + timeframe.to_milliseconds()

                logger.info(f"Completed historical collection for {symbol.name} ({symbol.exchange})")
                break  # Success - exit retry loop

            except Exception as e:
                retry_count += 1
                if retry_count >= self._max_retries:
                    raise

                # Exponential backoff with jitter
                delay = min(30, 2 ** (retry_count - 1))
                delay = delay * (0.5 + random.random())  # Add jitter
                logger.warning(
                    f"Retry {retry_count}/{self._max_retries} "
                    f"for {symbol.name} ({symbol.exchange}) after {delay:.2f}s: {e}"
                )
                await asyncio.sleep(delay)

class BatchSynchronizer:
    """Handles batch synchronization of symbols with complete historical data"""

    def __init__(self,
                 state_manager: SymbolStateManager,
                 adapter_registry: ExchangeAdapterRegistry,
                 kline_repository: KlineRepository,
                 sync_interval: int = 300,
                 max_concurrent_updates: int = 80,
                 error_threshold: int = 3
                 ):
        self._state_manager = state_manager
        self._adapter_registry = adapter_registry
        self._kline_repository = kline_repository
        self.sync_interval = sync_interval
        self._error_threshold = error_threshold
        self._max_concurrent_updates = max_concurrent_updates

        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Bybit Rate Limit: 600 requests per 5 seconds = 120 requests per second
        # For safety (to account for network latency, response times, etc.):
        # - Let's use 75% of the limit: 120 * 0.75 = 90 requests/second
        # - Round down for extra safety: 80 concurrent updates
        self._sync_semaphore = asyncio.BoundedSemaphore(self.max_concurrent_updates)  # Limit concurrent operations
        self._active_updates = 0
        self._update_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the batch synchronizer"""
        self._active = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info("Batch synchronizer started")

    async def stop(self) -> None:
        """Stop the batch synchronizer"""
        self._active = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Batch synchronizer stopped")

    async def _sync_loop(self) -> None:
        """Enhanced sync loop with health checks"""
        error_count = 0
        while self._active:
            try:
                # Get ready symbols
                ready_symbols = await self._state_manager.get_symbols_by_state(SymbolState.READY)

                if ready_symbols:
                    try:
                        tasks = []
                        for symbol in ready_symbols:
                            await self._sync_semaphore.acquire()
                            task = asyncio.create_task(
                                self._sync_symbol_with_release(symbol)
                            )
                            tasks.append(task)

                        # Wait for all sync operations to complete
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        # Check for errors in results
                        errors = [r for r in results if isinstance(r, Exception)]
                        if errors:
                            error_count += 1
                            logger.error(f"Sync cycle completed with {len(errors)} errors: {errors[0]}")
                        else:
                            error_count = 0  # Reset only on completely successful cycle
                            logger.info(f"Successfully synced {len(ready_symbols)} symbols")

                        # Check error threshold
                        if error_count >= self._error_threshold:
                            logger.error(f"Error threshold ({self._error_threshold}) reached. Stopping synchronizer.")
                            self._active = False
                            break

                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error processing sync cycle: {e}")

                        if error_count >= self._error_threshold:
                            logger.error(f"Error threshold ({self._error_threshold}) reached. Stopping synchronizer.")
                            self._active = False
                            break

                # Adaptive sleep based on error state
                sleep_time = self.sync_interval * (1.5 if error_count > 0 else 1)
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                error_count += 1
                logger.error(f"Critical error in sync loop: {e}")

                if error_count >= self._error_threshold:
                    logger.error(f"Critical error threshold reached. Stopping synchronizer.")
                    self._active = False
                    break

                await asyncio.sleep(10)  # Longer delay on critical errors

    async def _sync_symbol_with_release(self, symbol: SymbolInfo) -> None:
        """Wrapper to ensure semaphore release"""
        try:
            async with self._update_lock:
                self._active_updates += 1
                logger.debug(f"Active updates: {self._active_updates}")

            await self._sync_symbol(symbol)

        except Exception as e:
            logger.error(f"Error syncing {symbol}: {e}")
            await self._state_manager.update_state(
                symbol,
                SymbolState.ERROR,
                str(e)
            )
            raise
        finally:
            self._sync_semaphore.release()
            async with self._update_lock:
                self._active_updates -= 1

    async def _sync_symbol(self, symbol: SymbolInfo) -> None:
        """Synchronize a single symbol"""
        try:
            # Get latest timestamp from database
            latest = await self._kline_repository.get_latest_timestamp(
                symbol,
                Timeframe.MINUTE_5
            )

            if not latest:
                raise ServiceError(f"No data found for {symbol}")

            # Calculate next timeframe start with safety checks
            current_time = get_current_timestamp()
            interval_ms = Timeframe.MINUTE_5.to_milliseconds()

            # Handle case where latest is in future (shouldn't happen, but safety check)
            if latest > current_time:
                logger.warning(f"Latest timestamp {from_timestamp(latest)} is in future for {symbol}")
                latest = current_time - interval_ms

            # Calculate next start time
            next_start = latest + interval_ms

            # Skip if next_start would be in future
            if next_start > current_time:
                logger.debug(f"Next start time {from_timestamp(next_start)} is in future, skipping sync for {symbol}")
                return

            # Get new data from exchange
            adapter = self._adapter_registry.get_adapter(symbol.exchange)
            klines = await adapter.get_klines(
                symbol=symbol,
                timeframe=Timeframe.MINUTE_5,
                start_time=Timestamp(next_start),
                limit=50  # Smaller limit for regular updates
            )

            if klines:
                # Check for data continuity
                first_timestamp = klines[0].timestamp
                if first_timestamp - next_start > interval_ms:
                    logger.warning(
                        f"Data gap detected for {symbol}: "
                        f"Expected {from_timestamp(next_start)}, got {from_timestamp(first_timestamp)}"
                    )

                # Store new klines (will use UPSERT for duplicates)
                processed_count = await self._kline_repository.insert_batch(
                    symbol,
                    Timeframe.MINUTE_5,
                    [k.to_tuple() for k in klines]
                )

                # Update sync time
                last_timestamp = klines[-1].timestamp
                await self._state_manager.update_sync_time(
                    symbol,
                    Timestamp(last_timestamp)
                )

                logger.debug(
                    f"Processed {processed_count} klines for {symbol.name} ({symbol.exchange})"
                    f"from {from_timestamp(first_timestamp)} "
                    f"to {from_timestamp(last_timestamp)}"
                )

        except Exception as e:
            logger.error(f"Error syncing {symbol}: {e}")
            raise
            raise

    @property
    def max_concurrent_updates(self) -> int:
        return self._max_concurrent_updates

    async def set_max_concurrent_updates(self, value: int) -> None:
        """Gradually adjust max concurrent updates"""
        async with self._update_lock:
            old_value = self._max_concurrent_updates
            if value < old_value:
                # Reducing limit - do it gradually
                while self._max_concurrent_updates > value:
                    # Wait until active updates are below next target
                    next_target = max(
                        value,
                        self._max_concurrent_updates - 10
                    )
                    while self._active_updates > next_target:
                        await asyncio.sleep(1)

                    # Safe to reduce limit
                    self._max_concurrent_updates = next_target
                    self._sync_semaphore = asyncio.BoundedSemaphore(next_target)
                    logger.info(f"Gradually reduced concurrent updates to {next_target}")
            else:
                # Increasing limit - can do immediately
                self._max_concurrent_updates = value
                self._sync_semaphore = asyncio.BoundedSemaphore(value)

            logger.info(f"Finished adjusting max concurrent updates: {old_value} -> {value}")

    async def get_sync_status(self) -> str:
        """Get current synchronization status"""
        ready_count = len(await self._state_manager.get_symbols_by_state(SymbolState.READY))

        status = [
            "Synchronizer Status:",
            f"Ready Symbols: {ready_count}",
            f"Max Concurrent Updates: {self._max_concurrent_updates}",
            f"Currently Active Updates: {self._active_updates}",
            "Resource Utilization:",
            f"  {self._active_updates}/{self._max_concurrent_updates} slots in use",
            f"  {(self._active_updates/self._max_concurrent_updates*100):.1f}% capacity utilized",
            "Active Syncs:"
        ]

        if self._active_updates > 0:
            ready_symbols = await self._state_manager.get_symbols_by_state(SymbolState.READY)
            for symbol in ready_symbols:
                progress = await self._state_manager.get_progress(symbol)
                if progress and progress.state not in (SymbolState.ERROR, SymbolState.DELISTED):
                    status.append(f"  {progress}")
        else:
            status.append("  No active syncs")

        return "\n".join(status)