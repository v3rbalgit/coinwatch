# src/services/market_data/synchronizer.py

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

from src.services.market_data.progress import SyncSchedule
from src.utils.error import ErrorTracker
from src.utils.retry import RetryConfig, RetryStrategy

from ...adapters.registry import ExchangeAdapterRegistry
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.models import SymbolInfo
from ...core.exceptions import ServiceError, ValidationError
from ...repositories.kline import KlineRepository
from ...utils.logger import LoggerSetup
from ...utils.domain_types import Timeframe, Timestamp

logger = LoggerSetup.setup(__name__)

class BatchSynchronizer:
    """Handles batch synchronization of symbols with complete historical data"""

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
        self._update_lock = asyncio.Lock()
        # Bybit Rate Limit: 600 requests per 5 seconds = 120 requests per second
        # For safety (to account for network latency, response times, etc.):
        # - Let's use 75% of the limit: 120 * 0.75 = 90 requests/second
        # - Round down for extra safety: 80 concurrent updates
        self._max_concurrent_updates = max_concurrent_updates
        self._sync_semaphore = asyncio.BoundedSemaphore(self._max_concurrent_updates)  # Limit concurrent operations
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

    def calculate_next_sync(self, timeframe: Timeframe, current_time: Optional[datetime] = None) -> datetime:
        """Calculate the next sync time based on timeframe"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        interval_minutes = timeframe.to_milliseconds() / (1000 * 60)
        boundary_minutes = (current_time.minute // interval_minutes) * interval_minutes

        next_sync = current_time.replace(
            minute=int(boundary_minutes),
            second=0,
            microsecond=0
        )

        if next_sync <= current_time:
            next_sync += timedelta(minutes=int(interval_minutes))

        return next_sync

    async def schedule_symbol(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Schedule a symbol for synchronized updates"""
        next_sync = self.calculate_next_sync(timeframe)

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
                "next_sync": next_sync.timestamp(),
                "timeframe": timeframe.value
            }
        ))

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
        """Main sync loop with precise timing"""
        while self._active:
            try:
                current_time = datetime.now(timezone.utc)

                # Find symbols that need syncing
                pending_syncs = [
                    schedule for schedule in self._schedules.values()
                    if current_time >= schedule.next_sync
                    and schedule.symbol.name not in self._processing
                ]

                if pending_syncs:
                    # Process all pending syncs concurrently
                    tasks = []
                    for schedule in pending_syncs:
                        self._processing.add(schedule.symbol.name)
                        tasks.append(self._sync_symbol_with_timing(schedule))

                    # Wait for all syncs to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process any errors
                    for schedule, result in zip(pending_syncs, results):
                        if isinstance(result, Exception):
                            self._error_tracker.record_error(
                                result,
                                schedule.symbol.name,
                                context="sync"
                            )

                # Calculate time to next sync
                next_syncs = [s.next_sync for s in self._schedules.values()]
                if next_syncs:
                    next_sync = min(next_syncs)
                    sleep_time = (next_sync - datetime.now(timezone.utc)).total_seconds()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                else:
                    await asyncio.sleep(1)  # No schedules, check again in 1 second

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                await asyncio.sleep(1)

    async def _sync_symbol_with_timing(self, schedule: SyncSchedule) -> None:
        """Sync a symbol with error tracking and resource management"""
        retry_count = 0

        try:
            async with self._sync_semaphore:
                try:
                    processed_count = await self._perform_sync(schedule)

                    # Report successful sync
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.SYNC_COMPLETED,
                        params={
                            "symbol": schedule.symbol.name,
                            "timeframe": schedule.timeframe.value,
                            "sync_time": datetime.now(timezone.utc).timestamp(),
                            "processed": processed_count,
                            "context": {
                                "next_sync": schedule.next_sync.timestamp(),
                                "active_syncs": len(self._processing),
                                "resource_usage": {
                                    "concurrent_syncs": len(self._processing),
                                    "max_allowed": self._max_concurrent_updates
                                }
                            }
                        }
                    ))

                except Exception as e:
                    self._error_tracker.record_error(
                        e,
                        schedule.symbol.name,
                        context="sync"
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
                                "retry_count": retry_count,
                                "error_frequency": error_frequency,
                                "active_syncs": len(self._processing),
                                "concurrent_limit": self._max_concurrent_updates,
                                "timeframe": schedule.timeframe.value,
                                "last_sync": schedule.last_sync.timestamp() if schedule.last_sync else None
                            }
                        },
                        priority=1 if is_resource_error else 0
                    ))

        finally:
            self._processing.remove(schedule.symbol.name)

    async def _perform_sync(self, schedule: SyncSchedule) -> int:
        """
        Perform actual sync with retry logic
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
                next_start = latest + interval_ms
                current_time = datetime.now(timezone.utc)
                current_ms = int(current_time.timestamp() * 1000)

                if next_start < current_ms:
                    # Fetch and process data
                    adapter = self._adapter_registry.get_adapter(schedule.symbol.exchange)
                    klines = await adapter.get_klines(
                        symbol=schedule.symbol,
                        timeframe=schedule.timeframe,
                        start_time=Timestamp(next_start),
                        limit=50
                    )

                    if klines:
                        # Process only completed candles
                        klines = [k for k in klines if k.timestamp + interval_ms <= current_ms]

                        if klines:
                            # Store new klines
                            processed_count = await self._kline_repository.insert_batch(
                                schedule.symbol,
                                schedule.timeframe,
                                [k.to_tuple() for k in klines]
                            )

                            last_timestamp = klines[-1].timestamp
                            logger.debug(
                                f"Processed {processed_count} klines for {schedule.symbol.name} "
                                f"up to {datetime.fromtimestamp(last_timestamp/1000, timezone.utc)}"
                            )

                # Update schedule
                schedule.update(current_time)
                schedule.next_sync = self.calculate_next_sync(
                    schedule.timeframe,
                    current_time
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
        """Gradually adjust concurrent updates limit"""
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
        """Get detailed synchronization status"""
        current_time = datetime.now(timezone.utc)

        status = [
            "Synchronization Status:",
            f"Scheduled Symbols: {len(self._schedules)}",
            f"Active Syncs: {len(self._processing)}",
            f"Concurrent Limit: {self._max_concurrent_updates}",
            "\nNext Sync Times:"
        ]

        # Show next 5 pending syncs
        next_syncs = sorted(
            self._schedules.items(),
            key=lambda x: x[1].next_sync
        )[:5]

        for symbol_name, schedule in next_syncs:
            time_until = (schedule.next_sync - current_time).total_seconds()
            status.append(
                f"  {symbol_name}: {schedule.next_sync.strftime('%H:%M:%S')} "
                f"({time_until:.1f}s)"
            )

        return "\n".join(status)