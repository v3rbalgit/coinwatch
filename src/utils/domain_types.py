# src/utils/domain_types.py

from enum import Enum
from typing import Any, Dict, NewType, Literal, Optional, TypedDict

# Universal domain types
SymbolName = NewType('SymbolName', str)
ExchangeName = NewType('ExchangeName', str)
Timestamp = NewType('Timestamp', int)
Price = NewType('Price', float)

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

# System monitoring types
CriticalConditionType = Literal[
    "service_error",
    "collection_failure",
    "sync_failure",
    "connection_overflow",
    "connection_timeout",
    "deadlock",
    "maintenance_required",
    "query_timeout",
    "replication_lag",
    "lock_timeout"
]

# Critical condition
class CriticalCondition(TypedDict):
    type: CriticalConditionType
    severity: Literal["warning", "error", "critical"]
    message: str
    timestamp: int
    error_type: str
    context: Dict[str, Any]

# Service status types
class ServiceStatus(Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

# Isolation level types
class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "READ COMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

# Database error types
class DatabaseErrorType(str, Enum):
    """Specific database error categories"""
    CONNECTION_OVERFLOW = "connection_overflow"
    CONNECTION_TIMEOUT = "connection_timeout"
    DEADLOCK = "deadlock"
    QUERY_TIMEOUT = "query_timeout"
    REPLICATION_LAG = "replication_lag"
    LOCK_TIMEOUT = "lock_timeout"
    MAINTENANCE_REQUIRED = "maintenance_required"
    EMERGENCY = "emergency"

class DataSource(str, Enum):
    """Types of fundamental data sources"""
    COINGECKO = "coingecko"
    GITHUB = "github"
    ETHERSCAN = "etherscan"
    BSCSCAN = "bscscan"