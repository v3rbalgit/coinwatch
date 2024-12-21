from datetime import datetime, timedelta, timezone
import time
from typing import Tuple

from shared.core.enums import Interval


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

def align_time_range(start_time: int, end_time: int, base_interval: Interval) -> Tuple[int, int]:
    """
    Align start and end timestamps to base interval taking into account the last closed timestamp.

    Args:
        start_time (int): Start timestamp of the time range.
        end_time (int): End timestamp of the time range.
        interval_ms (int): Interval in milliseconds to base the calculation on.

    Returns:
        Tuple[int,int]: Aligned range of timestamps.
            Both timestamps are rounded down and end time is aligned to the last complete interval
    """
    interval_ms = base_interval.to_milliseconds()
    aligned_start = start_time - (start_time % interval_ms)
    current_time = get_current_timestamp()
    aligned_end = min(
        end_time - (end_time % interval_ms),
        current_time - (current_time % interval_ms) - interval_ms
    )
    return aligned_start, aligned_end

def align_timestamp_to_interval(timestamp: int, interval: Interval, round_up: bool = False) -> int:
    """
    Align timestamp to interval boundary ensuring proper interval alignment.
    If round_up is True and the timestamp is not already aligned, rounds up to the next interval boundary.
    If round_up is False, always rounds down to the current interval boundary.

    Examples:
        With round_up=False:
        - 14:05 aligned to 15min → 14:00
        - 14:17 aligned to 15min → 14:15
        - 14:02 aligned to 1h → 14:00
        - 2024-01-09 15:30 aligned to 1d → 2024-01-09 00:00
        - 2024-01-09 aligned to 1w → 2024-01-08 (Monday)

        With round_up=True:
        - 14:05 aligned to 15min → 14:15
        - 14:17 aligned to 15min → 14:30
        - 14:02 aligned to 1h → 15:00
        - 2024-01-09 15:30 aligned to 1d → 2024-01-10 00:00
        - 2024-01-09 aligned to 1w → 2024-01-15 (next Monday)

    Args:
        timestamp (int): Timestamp to align to interval.
        interval (Interval): Interval to align to.
        round_up (bool): Whether to align to the next interval boundary if not already aligned.

    Returns:
        int: Timestamp aligned to interval boundary.
    """
    # Convert to datetime for easier manipulation
    dt = from_timestamp(timestamp)

    if interval == Interval.DAY_1:
        # Align to UTC midnight
        aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == Interval.WEEK_1:
        # Align to UTC midnight Monday
        aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        days_since_monday = dt.weekday()
        aligned -= timedelta(days=days_since_monday)
    else:
        # For minute-based intervals
        minutes = int(interval.value) if interval.value.isdigit() else 0
        total_minutes = dt.hour * 60 + dt.minute
        aligned_minutes = (total_minutes // minutes) * minutes

        aligned = dt.replace(
            hour=aligned_minutes // 60,
            minute=aligned_minutes % 60,
            second=0,
            microsecond=0
        )

    # If timestamp is not already aligned and round_up is True,
    # move to the next interval boundary
    if round_up and aligned < dt:
        if interval == Interval.WEEK_1:
            aligned += timedelta(days=7)
        elif interval == Interval.DAY_1:
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

def format_timestamp(timestamp: int) -> str:
    """
    Helper function for displaying timestamps as formatted datetime string.

    Args:
        timestamp (int): Timestamp to format.

    Returns:
        str: Timestamp formatted as a datetime string (e.g., 06-10-2024 15:20).
    """
    return from_timestamp(timestamp).strftime('%d-%m-%Y %H:%M')