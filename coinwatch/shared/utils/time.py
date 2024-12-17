from datetime import datetime, timedelta, timezone
import time
from typing import Tuple

from shared.core.enums import Timeframe


def get_current_timestamp() -> int:
    """Get current time as millisecond timestamp"""
    return int(time.time() * 1000)


def get_current_datetime() -> datetime:
    """Get current time as UTC datetime"""
    return datetime.now(timezone.utc)


def from_timestamp(timestamp: int) -> datetime:
    """Convert millisecond timestamp to UTC datetime"""
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)


def to_timestamp(dt: datetime) -> int:
    """Convert datetime to millisecond timestamp"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)

def align_time_range_to_interval(start_time: int, end_time: int, base_timeframe: Timeframe) -> Tuple[int, int]:
    """
    Align start and end timestamps to base timeframe taking into account the last closed timestamp.

    Args:
        start_time (int): Start timestamp of the time range.
        end_time (int): End timestamp of the time range.
        interval_ms (int): Interval in milliseconds to base the calculation on.

    Returns:
        Tuple[int,int]: Aligned range of timestamps.
    """
    interval_ms = base_timeframe.to_milliseconds()
    aligned_start = start_time - (start_time % interval_ms)
    current_time = get_current_timestamp()
    aligned_end = min(
        end_time - (end_time % interval_ms),
        current_time - (current_time % interval_ms) - interval_ms
    )
    return aligned_start, aligned_end

def align_timestamp_to_interval(timestamp: int, timeframe: Timeframe, round_up: bool = False) -> int:
        """
        Align timestamp to timeframe boundary ensuring proper interval alignment.

        Examples:
        - 15min: :00, :15, :30, :45
        - 30min: :00, :30
        - 1h: :00
        - 4h: :00, 04:00, 08:00, etc.
        - 1d: 00:00 UTC

        Args:
            timestamp (int): Timestamp to align to interval.
            timeframe (Timeframe): Interval (timeframe) to align to.
            round_up (bool): Whether to align up or down (for end timestamps).

        Returns:
            int: Timestamp aligned to nearest interval boundary.
        """

        # Convert to datetime for easier manipulation
        dt = from_timestamp(timestamp)

        if timeframe == Timeframe.DAY_1:
            # Align to UTC midnight
            aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == Timeframe.WEEK_1:
            # Align to UTC midnight Monday
            aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            days_since_monday = dt.weekday()
            aligned -= timedelta(days=days_since_monday)
        else:
            # For minute-based timeframes
            minutes = int(timeframe.value) if timeframe.value.isdigit() else 0
            total_minutes = dt.hour * 60 + dt.minute
            aligned_minutes = (total_minutes // minutes) * minutes

            aligned = dt.replace(
                hour=aligned_minutes // 60,
                minute=aligned_minutes % 60,
                second=0,
                microsecond=0
            )

        if round_up and aligned < dt:
            if timeframe == Timeframe.WEEK_1:
                aligned += timedelta(days=7)
            elif timeframe == Timeframe.DAY_1:
                aligned += timedelta(days=1)
            else:
                aligned += timedelta(minutes=minutes)

        return to_timestamp(aligned)

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