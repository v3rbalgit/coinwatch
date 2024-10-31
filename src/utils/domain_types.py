# src/domain_types.py

from enum import Enum
from typing import NewType, Literal, TypedDict

SymbolName = NewType('SymbolName', str)
ExchangeName = NewType('ExchangeName', str)
Timestamp = NewType('Timestamp', int)
Price = NewType('Price', float)

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
CriticalConditionType = Literal["connection_overflow", "connection_timeout", "storage_full", "memory_high"]

class CriticalCondition(TypedDict):
    type: CriticalConditionType
    message: str
    severity: Literal["warning", "error", "critical"]
    timestamp: float

# Service status types
class ServiceStatus(Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"