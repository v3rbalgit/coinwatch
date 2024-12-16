from enum import Enum
from typing import Optional

from sqlalchemy import Tuple


# Timeframe type
class Timeframe(Enum):
    MINUTE_1 = "1"
    MINUTE_3 = "3"
    MINUTE_5 = "5"
    MINUTE_15 = "15"
    MINUTE_30 = "30"
    HOUR_1 = "60"
    HOUR_2 = "120"
    HOUR_4 = "240"
    HOUR_6 = "360"
    HOUR_12 = "720"
    DAY_1 = "D"
    WEEK_1 = "W"

    @property
    def continuous_aggregate_view(self) -> Optional[str]:
        """Get the corresponding continuous aggregate view name if it exists"""
        view_mapping = {
            self.HOUR_1: "kline_1h",
            self.HOUR_4: "kline_4h",
            self.DAY_1: "kline_1d"
        }
        return view_mapping.get(self)

    def is_stored_timeframe(self) -> bool:
        """Check if this timeframe has a continuous aggregate view"""
        return self.continuous_aggregate_view is not None

    def get_bucket_interval(self) -> str:
        """Get the appropriate bucket interval for TimescaleDB time_bucket function"""
        if self == self.DAY_1:
            return '1 day'
        elif self == self.WEEK_1:
            return '7 days'
        else:
            # Convert minutes to seconds for sub-day timeframes
            minutes = int(self.value) if self.value.isdigit() else 0
            return f'{minutes * 60} seconds'

    def to_milliseconds(self) -> int:
        """Convert timeframe to milliseconds"""
        mapping = {
            "1": 1 * 60 * 1000,
            "3": 3 * 60 * 1000,
            "5": 5 * 60 * 1000,
            "15": 15 * 60 * 1000,
            "60": 60 * 60 * 1000,
            "120": 2 * 60 * 60 * 1000,
            "240": 4 * 60 * 60 * 1000,
            "360": 6 * 60 * 60 * 1000,
            "720": 12 * 60 * 60 * 1000,
            "D": 24 * 60 * 60 * 1000,
            "W": 7 * 24 * 60 * 60 * 1000
        }
        return mapping[self.value]

# Service status types
class ServiceStatus(Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

class IsolationLevel(str, Enum):
    """Transaction isolation levels"""
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

class DataSource(str, Enum):
    """Types of fundamental data sources"""
    COINGECKO = "coingecko"
    GITHUB = "github"
    ETHERSCAN = "etherscan"
    BSCSCAN = "bscscan"
    INTERNAL = "internal"  # For internally computed metrics like sentiment analysis
