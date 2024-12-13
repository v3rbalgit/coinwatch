# src/utils/time.py

from datetime import datetime, timedelta, timezone
import time
from typing import Tuple

class TimeUtils:
    """Enhanced time utilities for consistent timestamp handling"""

    @staticmethod
    def get_current_timestamp() -> int:
        """Get current time as millisecond timestamp"""
        return int(time.time() * 1000)

    @staticmethod
    def get_current_datetime() -> datetime:
        """Get current time as UTC datetime"""
        return datetime.now(timezone.utc)

    @staticmethod
    def from_timestamp(timestamp: int) -> datetime:
        """Convert millisecond timestamp to UTC datetime"""
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

    @staticmethod
    def to_timestamp(dt: datetime) -> int:
        """Convert datetime to millisecond timestamp"""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)

    @staticmethod
    def align_to_interval(
        dt: datetime,
        interval_minutes: int,
        round_up: bool = True
    ) -> datetime:
        """
        Align datetime to interval boundary

        Args:
            dt: Datetime to align
            interval_minutes: Interval in minutes
            round_up: Round up to next interval if past boundary
        """
        boundary_minutes = (dt.minute // interval_minutes) * interval_minutes
        aligned = dt.replace(
            minute=boundary_minutes,
            second=0,
            microsecond=0
        )

        if round_up and aligned <= dt:
            aligned += timedelta(minutes=interval_minutes)

        return aligned

    @staticmethod
    def get_interval_boundaries(
        timestamp: int,
        interval_ms: int
    ) -> Tuple[int, int]:
        """
        Get interval boundaries for a timestamp

        Returns:
            Tuple[start_timestamp, end_timestamp]
        """
        start = timestamp - (timestamp % interval_ms)
        return start, start + interval_ms

    @staticmethod
    def is_complete_interval(
        timestamp: int,
        interval_ms: int,
        buffer_ms: int = 0
    ) -> bool:
        """
        Check if interval is complete (with optional buffer)
        """
        current = TimeUtils.get_current_timestamp()
        interval_end = timestamp + interval_ms
        return current >= (interval_end + buffer_ms)

    @staticmethod
    def format_time_difference(time_difference_ms: int) -> str:
        """
        Format a time difference in milliseconds to a human-readable string.

        Args:
            time_difference_ms (int): Time difference in milliseconds.

        Returns:
            str: Formatted time difference string.
        """
        seconds = time_difference_ms // 1000
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)

        if days > 0:
            return f"{days}d {hours}h"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"