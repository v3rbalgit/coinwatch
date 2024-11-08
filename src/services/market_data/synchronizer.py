# src/services/market_data/synchronizer.py

import asyncio
from datetime import datetime
from typing import Dict, Set

from src.services.market_data.progress import SyncSchedule
from src.utils.retry import RetryConfig, RetryStrategy
from src.utils.time import TimeUtils

from ...adapters.registry import ExchangeAdapterRegistry
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.models import SymbolInfo
from ...core.exceptions import ServiceError, ValidationError
from ...repositories.kline import KlineRepository
from ...utils.logger import LoggerSetup
from ...utils.domain_types import Timeframe, Timestamp

logger = LoggerSetup.setup(__name__)

class BatchSynchronizer:
    """
    Handles batch synchronization of symbols with complete historical data.

    Responsible for:
    - Scheduling and executing data synchronization
    - Managing concurrent updates within rate limits
    - Ensuring data continuity and completeness
    - Handling error recovery and retries
    - Reporting sync progress and status
    """

    def __init__(self,
                 adapter_registry: ExchangeAdapterRegistry,
                 kline_repository: KlineRepository,
                 coordinator: ServiceCoordinator,
                 base_timeframe: Timeframe,
                 max_concurrent_updates: int = 100):
        # Core dependencies
        self._adapter_registry = adapter_registry
        self._kline_repository = kline_repository
        self._coordinator = coordinator
        self._base_timeframe = base_timeframe

        # Sync management
        self._schedules: Dict[SymbolInfo, SyncSchedule] = {}
        self._schedules_lock = asyncio.Lock()
        self._max_concurrent_updates = max_concurrent_updates
        self._update_lock = asyncio.Lock()
        self._sync_semaphore = asyncio.BoundedSemaphore(self._max_concurrent_updates)
        self._processing: Set[SymbolInfo] = set()

        # Error handling
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=300.0,
            max_retries=3
        ))
        self._configure_retry_strategy()

        # Start background tasks
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._running = True
        asyncio.create_task(self._register_command_handlers())

    async def cleanup(self) -> None:
        """Cleanup background tasks and resources"""
        logger.info("Cleaning up BatchSynchronizer")
        self._running = False  # Signal sync loop to stop

        # Cancel and await sync task
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None

        # Clear schedules and processing sets
        async with self._schedules_lock:
            self._schedules.clear()
            self._processing.clear()

        logger.info("BatchSynchronizer cleanup completed")

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
        """Register handlers for sync-related commands"""
        handlers = {
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.ADJUST_BATCH_SIZE: self._handle_adjust_batch_size
        }

        for command, handler in handlers.items():
            await self._coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _handle_collection_complete(self, command: Command) -> None:
        """Handle completion of historical collection"""
        collection_type = command.params.get("collection_type", "initial")

        # Only schedule regular syncs for completed initial collections
        if collection_type == "initial":
            symbol = command.params["symbol"]
            await self._schedule_sync(symbol, self._base_timeframe)

    async def _handle_adjust_batch_size(self, command: Command) -> None:
        """Handle batch size adjustment command"""
        if new_size := command.params.get('size'):
            await self.set_max_concurrent_updates(new_size)

    def _calculate_next_sync(self, timeframe: Timeframe) -> datetime:
        """Calculate the next sync time based on timeframe."""
        current_time = TimeUtils.get_current_datetime()
        minutes = timeframe.to_milliseconds() / (1000 * 60)
        return TimeUtils.align_to_interval(current_time, int(minutes), round_up=True)

    async def _schedule_sync(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Schedule a symbol for synchronized updates."""
        next_sync = self._calculate_next_sync(timeframe)

        async with self._schedules_lock:
            self._schedules[symbol] = SyncSchedule(
                symbol=symbol,
                timeframe=timeframe,
                next_sync=next_sync
            )

            logger.info(
                f"Scheduled {symbol} for sync at {next_sync} "
                f"(timeframe: {timeframe.value})"
            )

    async def _sync_loop(self) -> None:
        """Main sync loop with precise timing."""
        while True:
            try:
                tasks = []

                async with self._schedules_lock:
                    pending_syncs = [
                        schedule for schedule in self._schedules.values()
                        if schedule.is_due() and schedule.symbol not in self._processing
                    ]

                    for schedule in pending_syncs:
                        self._processing.add(schedule.symbol)
                        tasks.append(self._sync_symbol_with_timing(schedule))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                await self._calculate_sleep_interval()

            except Exception as e:
                # Only report true infrastructure errors
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.SYNC_ERROR,
                    params={
                        "error": str(e),
                        "error_type": e.__class__.__name__,
                        "context": {
                            "process": "sync_infrastructure",
                            "active_syncs": len(self._processing),
                            "scheduled_symbols": len(self._schedules),
                            "timestamp": TimeUtils.get_current_timestamp()
                        }
                    },
                    priority=1
                ))
                await asyncio.sleep(1)

    async def _calculate_sleep_interval(self) -> None:
        """Calculate and sleep until next sync is due"""
        next_syncs = [s.next_sync for s in self._schedules.values()]
        if next_syncs:
            sleep_time = max(0.0, (min(next_syncs) - TimeUtils.get_current_datetime()).total_seconds())
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        else:
            await asyncio.sleep(1)

    async def _sync_symbol_with_timing(self, schedule: SyncSchedule) -> None:
        """Sync a symbol with retry logic"""
        retry_count = 0
        while True:
            sync_start = TimeUtils.get_current_timestamp()
            try:
                async with self._sync_semaphore:
                    processed_count = await self._perform_sync(schedule)
                    next_sync = await self._update_schedule(schedule)
                    logger.info(
                        f"Completed sync for {schedule.symbol}: processed {processed_count} candles "
                        f"[Active syncs: {len(self._processing)}/{self._max_concurrent_updates}] "
                        f"(next sync at {next_sync.strftime('%H:%M:%S')})"
                    )
                break  # Success - exit retry loop

            except Exception as e:
                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)
                if should_retry:
                    retry_count += 1
                    delay = self._retry_strategy.get_delay(retry_count)
                    logger.warning(
                        f"Sync retry {retry_count} for {schedule.symbol} "
                        f"after {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                # Report final error after retries exhausted
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.SYNC_ERROR,
                    params={
                        "symbol": schedule.symbol.name,
                        "error": str(e),
                        "error_type": e.__class__.__name__,
                        "retry_exhausted": True,
                        "context": {
                            "sync_start": sync_start,
                            "sync_duration": TimeUtils.get_current_timestamp() - sync_start,
                            "is_resource_error": isinstance(e, (ConnectionError, TimeoutError, ServiceError)),
                            "timeframe": schedule.timeframe.value,
                            "last_sync": TimeUtils.to_timestamp(schedule.last_sync) if schedule.last_sync else None,
                            "retry_count": retry_count,
                            "timestamp": TimeUtils.get_current_timestamp()
                        }
                    }
                ))
                raise  # Re-raise for _sync_loop to handle

            finally:
                async with self._schedules_lock:
                    self._processing.remove(schedule.symbol)

    async def _update_schedule(self, schedule: SyncSchedule) -> datetime:
        """Update schedule and calculate next sync time"""
        next_sync = self._calculate_next_sync(schedule.timeframe)

        async with self._schedules_lock:
            if schedule.symbol in self._schedules:
                self._schedules[schedule.symbol].update(TimeUtils.get_current_datetime())

        return next_sync

    async def _perform_sync(self, schedule: SyncSchedule) -> int:
        """
        Perform actual sync with retry logic.
        Returns:
            int: Number of candles processed
        """
        retry_count = 0
        processed_count = 0

        while True:
            try:
                # Get latest timestamp from database
                latest = await self._kline_repository.get_latest_timestamp(
                    schedule.symbol,
                    schedule.timeframe
                )

                if not latest:
                    raise ServiceError(f"No data found for {schedule.symbol}")

                # Calculate sync boundaries
                interval_ms = schedule.timeframe.to_milliseconds()
                next_start = Timestamp(latest + interval_ms)

                # Get last completed interval
                current_time = TimeUtils.get_current_timestamp()
                last_complete_interval, _ = TimeUtils.get_interval_boundaries(
                    Timestamp(current_time - interval_ms),
                    interval_ms
                )

                if next_start >= last_complete_interval:
                    logger.debug(
                        f"Next start time {TimeUtils.from_timestamp(next_start)} is beyond "
                        f"last complete interval {TimeUtils.from_timestamp(last_complete_interval)}, "
                        f"skipping sync for {schedule.symbol}"
                    )
                    return 0

                # Fetch new data
                adapter = self._adapter_registry.get_adapter(schedule.symbol.exchange)
                klines = await adapter.get_klines(
                    symbol=schedule.symbol,
                    timeframe=schedule.timeframe,
                    start_time=Timestamp(next_start),
                    limit=50
                )

                if klines:
                    # Process only completed candles
                    klines = [k for k in klines
                             if TimeUtils.is_complete_interval(Timestamp(k.timestamp), interval_ms)]

                    if klines:
                        # Store new klines
                        processed_count = await self._kline_repository.insert_batch(
                            schedule.symbol,
                            schedule.timeframe,
                            [k.to_tuple() for k in klines]
                        )

                        logger.debug(
                            f"Processed {processed_count} klines for {schedule.symbol} "
                            f"from {TimeUtils.from_timestamp(Timestamp(klines[0].timestamp))} "
                            f"to {TimeUtils.from_timestamp(Timestamp(klines[-1].timestamp))}"
                        )

                return processed_count

            except Exception as e:
                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)
                if should_retry:
                    retry_count += 1
                    delay = self._retry_strategy.get_delay(retry_count)
                    logger.warning(
                        f"Sync retry {retry_count} for {schedule.symbol} "
                        f"after {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                raise ServiceError(
                    f"Sync failed for {schedule.symbol}: {str(e)}"
                ) from e

    async def set_max_concurrent_updates(self, value: int) -> None:
        """Gradually adjust concurrent updates limit."""
        async with self._update_lock:
            old_value = self._max_concurrent_updates
            target_value = max(10, min(value, 100))  # Ensure reasonable bounds

            if target_value < old_value:
                # Reduce gradually
                while self._max_concurrent_updates > target_value:
                    next_target = max(target_value, self._max_concurrent_updates - 5)
                    self._max_concurrent_updates = next_target
                    self._sync_semaphore = asyncio.BoundedSemaphore(next_target)
                    await asyncio.sleep(1)  # Allow time for adjustments
            else:
                # Increase immediately
                self._max_concurrent_updates = target_value
                self._sync_semaphore = asyncio.BoundedSemaphore(target_value)

            logger.info(f"Adjusted concurrent updates: {old_value} -> {target_value}")

    def get_sync_status(self) -> str:
        """Get synchronization status with copy of state."""
        schedules_count = len(self._schedules)
        processing_count = len(self._processing)
        max_updates = self._max_concurrent_updates

        status = [
            "Synchronization Status:",
            f"Scheduled Symbols: {schedules_count}",
            f"Active Syncs: {processing_count}",
            f"Concurrent Limit: {max_updates}",
            "\nNext Sync Times:"
        ]

        schedule_items = sorted(
            self._schedules.items(),
            key=lambda x: x[1].next_sync
        )[:5]

        for symbol, schedule in schedule_items:
            time_until = schedule.get_time_until_next()
            status.append(
                f"  {symbol}: {schedule.next_sync.strftime('%H:%M:%S')} "
                f"({time_until:.1f}s)"
            )

        return "\n".join(status)