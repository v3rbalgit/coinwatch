from datetime import timedelta
from enum import Enum


class Interval(str, Enum):
    """
    Trading interval enumeration with built-in validation and utility methods.

    This enum represents valid trading intervals and provides methods for:
    - Interval validation and comparison
    - Time conversion and calculations
    - Database view mapping
    """
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
    def continuous_aggregate_view(self) -> str | None:
        """Get the corresponding continuous aggregate view name if it exists"""
        view_mapping = {
            self.HOUR_1: "kline_1h",
            self.HOUR_4: "kline_4h",
            self.DAY_1: "kline_1d"
        }
        return view_mapping.get(self)

    def is_stored_interval(self) -> bool:
        """Check if this interval has a continuous aggregate view"""
        return self.continuous_aggregate_view is not None

    def get_bucket_interval(self) -> timedelta:
        """Get the appropriate bucket interval for TimescaleDB time_bucket function"""
        if self == self.DAY_1:
            return timedelta(days=1)
        elif self == self.WEEK_1:
            return timedelta(days=7)
        else:
            minutes = int(self.value) if self.value.isdigit() else 0
            return timedelta(minutes=minutes)

    def to_milliseconds(self) -> int:
        """Convert interval to milliseconds"""
        if self.value == "D":
            return 24 * 60 * 60 * 1000
        elif self.value == "W":
            return 7 * 24 * 60 * 60 * 1000
        else:
            return int(self.value) * 60 * 1000

    def is_valid_for_base(self, base_interval: 'Interval') -> bool:
        """
        Check if this interval can be calculated from the given base interval.
        An interval is valid if it's a multiple of the base interval.

        Args:
            base_interval: The base interval to check against

        Returns:
            bool: True if this interval can be calculated from the base interval
        """
        return (self.to_milliseconds() >= base_interval.to_milliseconds() and
                self.to_milliseconds() % base_interval.to_milliseconds() == 0)

    @classmethod
    def get_valid_intervals(cls, base_interval: 'Interval') -> set['Interval']:
        """
        Get all valid intervals that can be calculated from a base interval.

        Args:
            base_interval: The base interval to calculate from

        Returns:
            Set of valid intervals that can be derived from the base interval
        """
        return {interval for interval in cls if interval.is_valid_for_base(base_interval)}


class ServiceStatus(str, Enum):
    """Service statuses"""
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    UNKNOWN = "unknown"


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
