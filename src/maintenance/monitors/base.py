
# src/maintenance/monitors/base.py
from abc import ABC, abstractmethod
from typing import Optional
from dataclasses import dataclass

@dataclass
class MonitorMetrics:
    """Base metrics for monitoring"""
    timestamp: float
    status: str
    details: Optional[dict] = None

class Monitor(ABC):
    """Base monitor class"""
    def __init__(self):
        self.metrics: Optional[MonitorMetrics] = None

    @abstractmethod
    async def observe(self) -> None:
        """Observe system metrics"""
        pass

    @abstractmethod
    async def handle_critical_condition(self) -> None:
        """Handle critical conditions"""
        pass