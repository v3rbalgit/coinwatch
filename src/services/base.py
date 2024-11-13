# src/services/base.py

from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Generic

from ..utils.domain_types import CriticalCondition
from ..config import MarketDataConfig, MonitoringConfig, DatabaseConfig

ConfigType = TypeVar('ConfigType', MarketDataConfig, MonitoringConfig, DatabaseConfig, Dict[str, Any])

class ServiceBase(ABC, Generic[ConfigType]):
    """Base class for all services"""
    def __init__(self, config: ConfigType):
        self._initialized: bool = False
        self._config: ConfigType = config or {}

    @abstractmethod
    async def start(self) -> None:
        """Start the service"""
        self._initialized = True

    @abstractmethod
    async def stop(self) -> None:
        """Stop the service"""
        self._initialized = False

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical system conditions"""
        pass