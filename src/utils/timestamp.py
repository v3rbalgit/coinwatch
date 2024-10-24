# src/utils/timestamp.py

import time
from datetime import datetime, timedelta, timezone

def get_current_timestamp() -> int:
    """Get the current timestamp in milliseconds."""
    return int(time.time() * 1000)

def get_past_timestamp(days: int) -> int:
    """Get a timestamp from 'days' ago in milliseconds."""
    past_date = datetime.now(timezone.utc) - timedelta(days=days)
    return to_timestamp(past_date)

def from_timestamp(timestamp: int) -> datetime:
    """Convert millisecond timestamp to datetime object."""
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

def to_timestamp(dt: datetime) -> int:
    """Convert datetime object to millisecond timestamp."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)