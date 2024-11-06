# src/services/market_data/progress.py

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ...utils.domain_types import Timeframe
from ...core.models import SymbolInfo

@dataclass
class CollectionProgress:
    """Tracks historical data collection progress"""
    symbol: SymbolInfo
    start_time: datetime
    processed_candles: int = 0
    total_candles: Optional[int] = None
    last_processed_time: Optional[datetime] = None

    def update(self, processed: int, total: Optional[int] = None) -> None:
        self.processed_candles = processed
        if total is not None:
            self.total_candles = total
        self.last_processed_time = datetime.now(timezone.utc)

    def get_percentage(self) -> Optional[float]:
        if self.total_candles:
            return min(100.0, (self.processed_candles / self.total_candles) * 100)
        return None

    def __str__(self) -> str:
        status = [f"Collection Progress for {self.symbol.name}"]
        if percentage := self.get_percentage():
            status.append(f"{percentage:.1f}%")
        if self.total_candles:
            status.append(f"({self.processed_candles}/{self.total_candles} candles)")
        return " | ".join(status)

@dataclass
class SyncSchedule:
    """Tracks synchronization schedule and progress"""
    symbol: SymbolInfo
    timeframe: Timeframe
    next_sync: datetime
    last_sync: Optional[datetime] = None

    def update(self, sync_time: datetime) -> None:
        self.last_sync = sync_time

    def __str__(self) -> str:
        status = [f"Sync Schedule for {self.symbol.name}"]
        if self.last_sync:
            time_since = (datetime.now(timezone.utc) - self.last_sync).total_seconds()
            status.append(f"Last: {time_since:.1f}s ago")
        time_until = (self.next_sync - datetime.now(timezone.utc)).total_seconds()
        status.append(f"Next: {time_until:.1f}s")
        return " | ".join(status)