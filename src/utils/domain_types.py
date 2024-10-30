# src/domain_types.py

from enum import Enum
from typing import NewType, Literal, TypedDict

SymbolName = NewType('SymbolName', str)
ExchangeName = NewType('ExchangeName', str)
Timestamp = NewType('Timestamp', int)
Price = NewType('Price', float)

class Timeframe(Enum):
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1d"

    def to_milliseconds(self) -> int:
        """Convert timeframe to milliseconds"""
        mapping = {
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000
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