# src/utils/time.py

from datetime import datetime, timedelta, timezone
import time
from typing import Tuple

from .domain_types import Timestamp

class TimeUtils:
    """Enhanced time utilities for consistent timestamp handling"""

    @staticmethod
    def get_current_timestamp() -> Timestamp:
        """Get current time as millisecond timestamp"""
        return Timestamp(int(time.time() * 1000))

    @staticmethod
    def get_current_datetime() -> datetime:
        """Get current time as UTC datetime"""
        return datetime.now(timezone.utc)

    @staticmethod
    def from_timestamp(timestamp: Timestamp) -> datetime:
        """Convert millisecond timestamp to UTC datetime"""
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

    @staticmethod
    def to_timestamp(dt: datetime) -> Timestamp:
        """Convert datetime to millisecond timestamp"""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return Timestamp(int(dt.timestamp() * 1000))

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
        timestamp: Timestamp,
        interval_ms: int
    ) -> Tuple[Timestamp, Timestamp]:
        """
        Get interval boundaries for a timestamp

        Returns:
            Tuple[start_timestamp, end_timestamp]
        """
        start = timestamp - (timestamp % interval_ms)
        return Timestamp(start), Timestamp(start + interval_ms)

    @staticmethod
    def is_complete_interval(
        timestamp: Timestamp,
        interval_ms: int,
        buffer_ms: int = 0
    ) -> bool:
        """
        Check if interval is complete (with optional buffer)
        """
        current = TimeUtils.get_current_timestamp()
        interval_end = timestamp + interval_ms
        return current >= (interval_end + buffer_ms)