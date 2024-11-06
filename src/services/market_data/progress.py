# src/services/market_data/progress.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ...utils.domain_types import Timeframe
from ...utils.time import TimeUtils
from ...core.models import SymbolInfo

@dataclass
class CollectionProgress:
    """Tracks historical data collection progress"""
    symbol: SymbolInfo
    start_time: datetime  # Stored as UTC datetime
    processed_candles: int = 0
    total_candles: Optional[int] = None
    last_processed_time: Optional[datetime] = None

    def update(self, processed: int, total: Optional[int] = None) -> None:
        """Update progress with new counts"""
        self.processed_candles = processed
        if total is not None:
            self.total_candles = total
        self.last_processed_time = TimeUtils.get_current_datetime()

    def get_percentage(self) -> Optional[float]:
        """Calculate completion percentage"""
        if self.total_candles:
            return min(100.0, (self.processed_candles / self.total_candles) * 100)
        return None

    def __str__(self) -> str:
        """Human-readable progress representation"""
        status = [f"Collection Progress for {self.symbol.name}"]

        if percentage := self.get_percentage():
            status.append(f"{percentage:.1f}%")

        if self.total_candles:
            status.append(f"({self.processed_candles}/{self.total_candles} candles)")

        if self.last_processed_time:
            elapsed = (TimeUtils.get_current_datetime() - self.start_time).total_seconds()
            status.append(f"elapsed: {elapsed:.1f}s")

        return " | ".join(status)


@dataclass
class SyncSchedule:
    """Tracks synchronization schedule and progress"""
    symbol: SymbolInfo
    timeframe: Timeframe
    next_sync: datetime  # Stored as UTC datetime
    last_sync: Optional[datetime] = None  # Stored as UTC datetime

    def update(self, sync_time: datetime) -> None:
        """Update last sync time"""
        self.last_sync = sync_time

    def is_due(self) -> bool:
        """Check if sync is due based on current time"""
        return TimeUtils.get_current_datetime() >= self.next_sync

    def get_time_until_next(self) -> float:
        """Get seconds until next scheduled sync"""
        return max(0.0, (self.next_sync - TimeUtils.get_current_datetime()).total_seconds())

    def __str__(self) -> str:
        """Human-readable schedule representation"""
        status = [f"Sync Schedule for {self.symbol.name}"]

        if self.last_sync:
            time_since = (TimeUtils.get_current_datetime() - self.last_sync).total_seconds()
            status.append(f"Last: {time_since:.1f}s ago")

        time_until = self.get_time_until_next()
        status.append(f"Next: {time_until:.1f}s")

        return " | ".join(status)