# src/monitoring/observers/base.py

from abc import ABC, abstractmethod
from typing import Set, Dict

from ...core.models import Observation
from ...core.protocols import ObserverProtocol

class ThresholdConfig:
    """Base configuration for thresholds"""
    warning: float
    critical: float

    def __init__(self, warning: float, critical: float):
        if not 0 <= warning <= 1:
            raise ValueError("Warning threshold must be between 0 and 1")
        if not 0 <= critical <= 1:
            raise ValueError("Critical threshold must be between 0 and 1")
        if warning >= critical:
            raise ValueError("Warning threshold must be less than critical")

        self.warning = warning
        self.critical = critical

class SystemObserver(ABC):
    """Base observer for all monitoring components"""
    def __init__(self):
        self._thresholds: Dict[str, Dict[str, float]] = {}
        self._observers: Set[ObserverProtocol] = set()
        self._active = False
        self.interval: int = 60  # Default interval in seconds

    @abstractmethod
    async def observe(self) -> Observation:
        """Perform system observation"""
        pass

    async def notify(self, observation: Observation) -> None:
        """Notify all observers of new observation"""
        for observer in self._observers:
            await observer.on_observation(observation)

    def add_observer(self, observer: ObserverProtocol) -> None:
        """Add new observer"""
        self._observers.add(observer)

    def remove_observer(self, observer: ObserverProtocol) -> None:
        """Remove observer"""
        self._observers.discard(observer)

    def set_threshold(self,
                     resource: str,
                     warning: float,
                     critical: float) -> None:
        """Set thresholds for a resource"""
        config = ThresholdConfig(warning, critical)
        self._thresholds[resource] = {
            'warning': config.warning,
            'critical': config.critical
        }

    def get_threshold(self, resource: str) -> Dict[str, float]:
        """Get thresholds for a resource"""
        return self._thresholds.get(resource, {
            'warning': 0.75,  # Default warning at 75%
            'critical': 0.9   # Default critical at 90%
        })

    async def cleanup(self) -> None:
      """Clean up observer resources"""
      self._observers.clear()