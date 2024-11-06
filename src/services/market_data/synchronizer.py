# src/services/market_data/synchronizer.py

import asyncio
from datetime import datetime
from typing import Dict, Optional, Set

from src.services.market_data.progress import SyncSchedule
from src.utils.error import ErrorTracker
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
                 max_concurrent_updates: int = 80
                 ):
        # Core dependencies
        self._adapter_registry = adapter_registry
        self._kline_repository = kline_repository
        self._coordinator = coordinator
        self._base_timeframe = base_timeframe

        # Sync management
        self._schedules: Dict[str, SyncSchedule] = {}
        self._schedules_lock = asyncio.Lock()
        self._max_concurrent_updates = max_concurrent_updates
        self._update_lock = asyncio.Lock()
        self._sync_semaphore = asyncio.BoundedSemaphore(self._max_concurrent_updates)
        self._processing: Set[str] = set()

        # Task management
        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Error handling
        self._error_tracker = ErrorTracker()
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=300.0,
            max_retries=3
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

    def _calculate_next_sync(self, timeframe: Timeframe) -> datetime:
        """
        Calculate the next sync time based on timeframe.
        Ensures sync times align with candle boundaries.
        """
        current_time = TimeUtils.get_current_datetime()
        minutes = timeframe.to_milliseconds() / (1000 * 60)

        # Find next boundary
        return TimeUtils.align_to_interval(current_time, int(minutes), round_up=True)

    async def schedule_symbol(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Schedule a symbol for synchronized updates."""
        next_sync = self._calculate_next_sync(timeframe)

        self._schedules[symbol.name] = SyncSchedule(
            symbol=symbol,
            timeframe=timeframe,
            next_sync=next_sync
        )

        logger.info(
            f"Scheduled {symbol.name} for sync at {next_sync} "
            f"(timeframe: {timeframe.value})"
        )

        await self._coordinator.execute(Command(
            type=MarketDataCommand.SYNC_SCHEDULED,
            params={
                "symbol": symbol.name,
                "next_sync": TimeUtils.to_timestamp(next_sync),
                "timeframe": timeframe.value,
                "timestamp": TimeUtils.get_current_timestamp()
            }
        ))

    async def start(self) -> None:
        """Start the batch synchronizer."""
        self._active = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info("Batch synchronizer started")

    async def stop(self) -> None:
        """Stop the batch synchronizer with cleanup."""
        self._active = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Batch synchronizer stopped")

    async def _sync_loop(self) -> None:
        """Main sync loop with precise timing."""
        while self._active:
            try:
                current_time = TimeUtils.get_current_datetime()
                tasks = []

                async with self._schedules_lock:
                    # Get pending syncs and update processing set atomically
                    pending_syncs = [
                        schedule for schedule in self._schedules.values()
                        if schedule.is_due() and schedule.symbol.name not in self._processing
                    ]

                    # Update processing set while holding lock
                    for schedule in pending_syncs:
                        self._processing.add(schedule.symbol.name)
                        tasks.append(self._sync_symbol_with_timing(schedule))

                # Execute tasks outside the lock
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process errors
                    for schedule, result in zip(pending_syncs, results):
                        if isinstance(result, Exception):
                            await self._error_tracker.record_error(
                                result,
                                schedule.symbol.name,
                                context="sync",
                                timestamp=TimeUtils.get_current_timestamp()
                            )

                # Calculate next sleep interval
                next_syncs = [s.next_sync for s in self._schedules.values()]
                if next_syncs:
                    sleep_time = max(0.0, (min(next_syncs) - TimeUtils.get_current_datetime()).total_seconds())
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                else:
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                await asyncio.sleep(1)

    async def _sync_symbol_with_timing(self, schedule: SyncSchedule) -> None:
        """Sync a symbol with error tracking and resource management."""
        sync_start = TimeUtils.get_current_timestamp()
        try:
            async with self._sync_semaphore:
                processed_count = await self._perform_sync(schedule)

                # Update schedule and report success
                next_sync = self._calculate_next_sync(schedule.timeframe)

                async with self._schedules_lock:
                    if schedule.symbol.name in self._schedules:
                        self._schedules[schedule.symbol.name].update(TimeUtils.get_current_datetime())

                # Report successful sync
                await self._coordinator.execute(Command(
                    type=MarketDataCommand.SYNC_COMPLETED,
                    params={
                        "symbol": schedule.symbol.name,
                        "timeframe": schedule.timeframe.value,
                        "sync_time": TimeUtils.get_current_timestamp(),
                        "processed": processed_count,
                        "context": {
                            "next_sync": TimeUtils.to_timestamp(next_sync),
                            "active_syncs": len(self._processing),
                            "resource_usage": {
                                "concurrent_syncs": len(self._processing),
                                "max_allowed": self._max_concurrent_updates
                            }
                        }
                    }
                ))

        except Exception as e:
            # Record error with timing context
            await self._error_tracker.record_error(
                e,
                schedule.symbol.name,
                context={
                    "sync_start": sync_start,
                    "sync_duration": TimeUtils.get_current_timestamp() - sync_start
                }
            )

            # Check if this is a resource-related error
            is_resource_error = isinstance(e, (
                ConnectionError,
                TimeoutError,
                ServiceError
            ))

            # Get error frequency for this symbol
            error_frequency = self._error_tracker.get_error_frequency(
                e.__class__.__name__,
                window_minutes=60
            )

            # Notify about sync error with context
            await self._coordinator.execute(Command(
                type=MarketDataCommand.SYNC_ERROR,
                params={
                    "symbol": schedule.symbol.name,
                    "error": str(e),
                    "error_type": e.__class__.__name__,
                    "context": {
                        "is_resource_error": is_resource_error,
                        "error_frequency": error_frequency,
                        "active_syncs": len(self._processing),
                        "concurrent_limit": self._max_concurrent_updates,
                        "timeframe": schedule.timeframe.value,
                        "last_sync": TimeUtils.to_timestamp(schedule.last_sync) if schedule.last_sync else None,
                        "timestamp": TimeUtils.get_current_timestamp()
                    }
                },
                priority=1 if is_resource_error else 0
            ))

        finally:
            async with self._schedules_lock:
                self._processing.remove(schedule.symbol.name)

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
                        f"skipping sync for {schedule.symbol.name}"
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
                            f"Processed {processed_count} klines for {schedule.symbol.name} "
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
                        f"Sync retry {retry_count} for {schedule.symbol.name} "
                        f"after {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                raise ServiceError(
                    f"Sync failed for {schedule.symbol.name}: {str(e)}"
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
        # Take snapshots of state to avoid locks in string formatting
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

        # Create a sorted copy of schedules for status
        schedule_items = sorted(
            self._schedules.items(),
            key=lambda x: x[1].next_sync
        )[:5]

        for symbol_name, schedule in schedule_items:
            time_until = schedule.get_time_until_next()
            status.append(
                f"  {symbol_name}: {schedule.next_sync.strftime('%H:%M:%S')} "
                f"({time_until:.1f}s)"
            )

        return "\n".join(status)