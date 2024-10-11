import time
from datetime import datetime, timedelta

def get_current_timestamp() -> int:
    """Get the current timestamp in milliseconds."""
    return int(time.time() * 1000)

def get_past_timestamp(days: int) -> int:
    """Get a timestamp from 'days' ago in milliseconds."""
    past_date = datetime.now() - timedelta(days=days)
    return to_timestamp(past_date)

def calculate_hours_between(start_time: int, end_time: int) -> int:
    """Calculate the number of hours between two millisecond timestamps."""
    time_difference = end_time - start_time
    return int(time_difference / (1000 * 60 * 60))  # Convert milliseconds to hours

def from_timestamp(timestamp: int) -> datetime:
    """Convert millisecond timestamp to datetime object."""
    return datetime.fromtimestamp(timestamp / 1000)

def to_timestamp(dt: datetime) -> int:
    """Convert datetime object to millisecond timestamp."""
    return int(dt.timestamp() * 1000)