from datetime import datetime, timedelta

def get_current_timestamp() -> int:
    return int(datetime.now().timestamp() * 1000)

def get_past_timestamp(days: int) -> int:
    return int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

def calculate_hours_between(start_time: int, end_time: int) -> int:
    start_dt = datetime.fromtimestamp(start_time / 1000)
    end_dt = datetime.fromtimestamp(end_time / 1000)
    delta = end_dt - start_dt
    return int(delta.total_seconds() // 3600)
