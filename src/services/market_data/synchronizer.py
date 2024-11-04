# src/services/market_data/synchronizer.py

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

from .symbol_state import SymbolState, SymbolStateManager
from ...adapters.registry import ExchangeAdapterRegistry
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.models import SymbolInfo
from ...core.exceptions import ServiceError
from ...repositories.kline import KlineRepository
from ...utils.logger import LoggerSetup
from ...utils.domain_types import Timeframe, Timestamp

logger = LoggerSetup.setup(__name__)

@dataclass
class SyncSchedule:
    """Tracks sync schedule for a symbol"""
    symbol: SymbolInfo
    timeframe: Timeframe
    next_sync: datetime
    last_sync: Optional[datetime] = None
    error_count: int = 0

class BatchSynchronizer:
    """Handles batch synchronization of symbols with complete historical data"""

    def __init__(self,
                 state_manager: SymbolStateManager,
                 adapter_registry: ExchangeAdapterRegistry,
                 kline_repository: KlineRepository,
                 coordinator: ServiceCoordinator,
                 max_concurrent_updates: int = 80
                 ):
        self._state_manager = state_manager
        self._adapter_registry = adapter_registry
        self._kline_repository = kline_repository
        self._coordinator = coordinator

        self._max_concurrent_updates = max_concurrent_updates

        self._schedules: Dict[str, SyncSchedule] = {}
        self._active = False
        self._task: Optional[asyncio.Task] = None

        # Bybit Rate Limit: 600 requests per 5 seconds = 120 requests per second
        # For safety (to account for network latency, response times, etc.):
        # - Let's use 75% of the limit: 120 * 0.75 = 90 requests/second
        # - Round down for extra safety: 80 concurrent updates
        self._sync_semaphore = asyncio.BoundedSemaphore(self.max_concurrent_updates)  # Limit concurrent operations
        self._active_updates = 0
        self._update_lock = asyncio.Lock()

        # Track symbols being processed to prevent duplicates
        self._processing: Set[str] = set()

    def _calculate_next_sync(self, timeframe: Timeframe, current_time: Optional[datetime] = None) -> datetime:
        """Calculate the next sync time based on timeframe"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        interval_minutes = timeframe.to_milliseconds() / (1000 * 60)  # Convert to minutes

        # Round down to the last timeframe boundary
        minutes = current_time.minute
        boundary_minutes = (minutes // interval_minutes) * interval_minutes

        next_sync = current_time.replace(
            minute=int(boundary_minutes),
            second=0,
            microsecond=0
        )

        # If we're past the current interval, move to next one
        if next_sync <= current_time:
            next_sync += timedelta(minutes=int(interval_minutes))

        return next_sync

    async def schedule_symbol(self, symbol: SymbolInfo, timeframe: Timeframe) -> None:
        """Schedule a symbol for synchronized updates"""
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

        # Notify coordinator of new schedule
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
                    await asyncio.gather(*tasks, return_exceptions=True)

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
        """Sync a symbol with precise timing"""
        try:
            async with self._sync_semaphore:
                async with self._update_lock:
                    self._active_updates += 1

                try:
                    # Get latest timestamp from database
                    latest = await self._kline_repository.get_latest_timestamp(
                        schedule.symbol,
                        schedule.timeframe
                    )

                    if not latest:
                        raise ServiceError(f"No data found for {schedule.symbol}")

                    # Calculate exactly what we need
                    interval_ms = schedule.timeframe.to_milliseconds()
                    next_start = latest + interval_ms

                    # Get current timestamp at microsecond precision
                    current_time = datetime.now(timezone.utc)
                    current_ms = int(current_time.timestamp() * 1000)

                    # Only fetch if we're not asking for future data
                    if next_start < current_ms:
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

                                # Update sync time
                                last_timestamp = klines[-1].timestamp
                                await self._state_manager.update_sync_time(
                                    schedule.symbol,
                                    Timestamp(last_timestamp)
                                )

                                logger.debug(
                                    f"Processed {processed_count} klines for {schedule.symbol} "
                                    f"up to {datetime.fromtimestamp(last_timestamp/1000, timezone.utc)}"
                                )

                    # Schedule next sync
                    schedule.last_sync = current_time
                    schedule.next_sync = self._calculate_next_sync(
                        schedule.timeframe,
                        current_time
                    )
                    schedule.error_count = 0

                except Exception as e:
                    schedule.error_count += 1
                    logger.error(f"Error syncing {schedule.symbol}: {e}")

                    # Notify coordinator of sync error
                    await self._coordinator.execute(Command(
                        type=MarketDataCommand.SYNC_ERROR,
                        params={
                            "symbol": schedule.symbol.name,
                            "error": str(e),
                            "error_count": schedule.error_count
                        }
                    ))

                finally:
                    async with self._update_lock:
                        self._active_updates -= 1
                    self._processing.remove(schedule.symbol.name)

        except Exception as e:
            logger.error(f"Critical error in sync operation: {e}")
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