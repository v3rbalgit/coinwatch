# src/utils/progress.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

from .time import TimeUtils
from shared.core.models import SymbolInfo
from shared.core.enums import Timeframe


@dataclass
class MarketDataProgress:
    """Tracks data collection progress of a symbol"""

    symbol: SymbolInfo
    timeframe: Timeframe
    processed_candles: int
    total_candles: int
    start_time: datetime
    last_processed_time: Optional[datetime]

    def __init__(self, symbol: SymbolInfo, time_range: Tuple[int,int], timeframe: Timeframe) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.processed_candles = 0
        self.total_candles = self.calculate_total_candles(time_range)
        self.start_time = TimeUtils.get_current_datetime()
        self.last_processed_time: Optional[datetime] = None

    def calculate_total_candles(self, time_range: Tuple[int,int]) -> int:
        """
        Calculate total number of candles between aligned timestamps

        Args:
            time_range (Tuple[int,int]): Tuple of start and end timestamps aligned to interval.

        Returns:
            int: Total number of candles to process.

        """
        interval_ms = self.timeframe.to_milliseconds()
        return ((time_range[1] - time_range[0]) // interval_ms) + 1

    def update(self, processed: int) -> None:
        """
        Update progress with new counts

        Args:
            processed (int): Number of processed candles.
        """
        self.processed_candles = processed
        self.last_processed_time = TimeUtils.get_current_datetime()

    def get_percentage(self) -> float:
        """
        Calculate completion percentage

        Returns:
            float: Completion percentage.
        """
        return min(100.0, (self.processed_candles / self.total_candles) * 100)

    def __str__(self) -> str:
        """Human-readable progress representation"""
        status = [f"Collection progress for {self.symbol.name} on {self.symbol.exchange}"]

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
            f"Collection completed for {self.symbol.name} on {self.symbol.exchange}",
            f"Processed: {self.processed_candles:,} candles",
            f"Duration: {elapsed:.1f}s",
            f"Rate: {rate:.1f} candles/s"
        ]

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
