# src/services/base.py

from abc import ABC, abstractmethod
from typing import TypeVar, Dict, Any
from ..domain_types import CriticalCondition

T = TypeVar('T')

class ServiceBase(ABC):
    """Base class for all services"""
    def __init__(self):
        self._initialized: bool = False
        self._config: Dict[str, Any] = {}

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
