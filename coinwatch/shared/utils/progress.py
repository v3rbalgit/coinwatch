# src/utils/progress.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .time import TimeUtils
from ..core.models import SymbolInfo

@dataclass
class MarketDataProgress:
    """Tracks data collection progress of a symbol"""
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

    def get_completion_summary(self, end_time: datetime) -> str:
        """Generate detailed completion summary"""
        elapsed = (end_time - self.start_time).total_seconds()
        rate = self.processed_candles / elapsed if elapsed > 0 else 0

        summary = [
            f"Collection completed for {self.symbol}",
            f"Processed: {self.processed_candles:,} candles",
            f"Time range: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} → {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Duration: {elapsed:.1f}s",
            f"Rate: {rate:.1f} candles/s"
        ]

        if self.total_candles:
            percentage = self.get_percentage()
            summary.insert(1, f"Progress: {percentage:.1f}% ({self.processed_candles:,}/{self.total_candles:,})")

        return " | ".join(summary)

    def __lt__(self, other: 'SymbolInfo') -> bool:
      """Enable sorting by symbol"""
      if not isinstance(other, MarketDataProgress):
          return NotImplemented
      return self.symbol < other.symbol

    def __eq__(self, other: object) -> bool:
        """Equality comparison"""
        if not isinstance(other, MarketDataProgress):
            return NotImplemented
        return self.symbol == other.symbol

@dataclass
class FundamentalDataProgress:
    """Generic progress tracking for data collection"""
    symbol: str
    collector_type: str
    start_time: datetime
    items_total: Optional[int] = None
    items_processed: int = 0
    status: str = "pending"
    error: Optional[str] = None
    last_update: Optional[datetime] = None

    def update(self, processed: int, total: Optional[int] = None) -> None:
        """Update progress"""
        self.items_processed = processed
        if total is not None:
            self.items_total = total
        self.last_update = TimeUtils.get_current_datetime()

    def get_completion_summary(self, end_time: datetime) -> str:
        """Generate detailed completion summary"""
        elapsed = (end_time - self.start_time).total_seconds()

        summary = [
            f"Collection completed for {self.symbol}",
            f"Type: {self.collector_type}",
            f"Duration: {elapsed:.1f}s",
            f"Status: {self.status}"
        ]

        if self.items_total:
            percentage = (self.items_processed / self.items_total) * 100
            summary.append(
                f"Progress: {percentage:.1f}% "
                f"({self.items_processed}/{self.items_total} items)"
            )

        if self.error:
            summary.append(f"Error: {self.error}")

        return " | ".join(summary)

    def __str__(self) -> str:
        """Human-readable progress representation"""
        if self.items_total:
            percentage = (self.items_processed / self.items_total) * 100
            return (f"{self.collector_type} Progress: {percentage:.1f}% "
                   f"({self.items_processed}/{self.items_total})")
        return f"{self.collector_type} Progress: {self.status}"
