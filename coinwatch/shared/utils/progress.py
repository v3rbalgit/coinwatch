from dataclasses import dataclass
from datetime import datetime

from shared.core.models import SymbolModel
from shared.core.enums import Interval
from shared.utils.time import get_current_datetime


@dataclass
class MarketDataProgress:
    """Tracks data collection progress of a symbol"""

    symbol: SymbolModel
    interval: Interval
    processed_candles: int
    total_candles: int
    start_time: datetime
    last_processed_time: datetime | None

    def __init__(self, symbol: SymbolModel, time_range: tuple[int,int], interval: Interval) -> None:
        self.symbol = symbol
        self.interval = interval
        self.processed_candles = 0
        self.total_candles = self.calculate_total_candles(time_range)
        self.start_time = get_current_datetime()
        self.last_processed_time: datetime | None = None

    def calculate_total_candles(self, time_range: tuple[int,int]) -> int:
        """
        Calculate total number of candles between aligned timestamps

        Args:
            time_range (Tuple[int,int]): Tuple of start and end timestamps aligned to interval.

        Returns:
            int: Total number of candles in the time range.
        """
        interval_ms = self.interval.to_milliseconds()
        return ((time_range[1] - time_range[0]) // interval_ms) + 1

    def update(self, processed: int, gap: int) -> None:
        """
        Update progress with new counts.

        Args:
            processed (int): Number of processed candles in this batch.
        """
        if gap > 0:
            self.total_candles -= gap
        self.processed_candles += processed  # Accumulate processed count
        self.last_processed_time = get_current_datetime()

    def get_percentage(self) -> float:
        """
        Calculate completion percentage

        Returns:
            float: Completion percentage.
        """
        return min(100.0, (self.processed_candles / self.total_candles) * 100)

    def __str__(self) -> str:
        """Human-readable progress representation"""
        status = [f"Collection progress for {str(self.symbol)}"]

        if percentage := self.get_percentage():
            status.append(f"{percentage:.1f}%")

        if self.total_candles:
            status.append(f"{self.processed_candles}/{self.total_candles} klines")

        if self.last_processed_time:
            elapsed = (get_current_datetime() - self.start_time).total_seconds()
            status.append(f"elapsed: {elapsed:.1f}s")

        return " | ".join(status)

    def get_completion_summary(self) -> str:
        """Generate detailed completion summary"""
        percentage = self.get_percentage()
        elapsed = (get_current_datetime() - self.start_time).total_seconds()
        rate = self.processed_candles / elapsed if elapsed > 0 else 0

        summary = [
            f"Collection completed for {str(self.symbol)}",
            f"{percentage:.1f}%",
            f"Processed: {self.processed_candles:,} klines",
            f"Duration: {elapsed:.1f}s",
            f"Rate: {rate:.1f} klines/s"
        ]

        return " | ".join(summary)

    def __lt__(self, other: 'SymbolModel') -> bool:
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
    collector_type: str
    start_time: datetime
    total_tokens: int
    processed_tokens: int = 0
    last_update: datetime | None = None
    last_processed_token: str | None = None

    def update(self, symbol: str):
        """Update progress"""
        self.processed_tokens += 1
        self.last_processed_token = symbol

    def get_percentage(self) -> float:
        """Calculate percentage of processed vs total tokens"""
        return (self.processed_tokens / self.total_tokens) * 100 if self.total_tokens > 0 else 0

    def get_completion_summary(self) -> str:
        """Generate detailed completion summary"""
        elapsed = (get_current_datetime() - self.start_time).total_seconds()
        percentage = self.get_percentage()

        summary = [
            f"Collection completed",
            f"{percentage:.1f}%",
            f"Type: {self.collector_type}",
            f"Duration: {elapsed:.1f}s",
            f"Progress: {self.processed_tokens}/{self.total_tokens} tokens"
        ]

        return " | ".join(summary)

    def __str__(self) -> str:
        """Human-readable progress representation"""
        percentage = self.get_percentage()
        status = [
            f"{self.collector_type} progress: {percentage:.1f}%",
            f"{self.processed_tokens}/{self.total_tokens} tokens"
        ]
        if self.last_processed_token:
            status.append(f"Last: {self.last_processed_token}")
        return " | ".join(status)
