# src/monitoring/actions/base.py

from abc import ABC, abstractmethod
from typing import Dict, Any

class Action(ABC):
    """Base class for all system actions"""
    @abstractmethod
    async def execute(self, context: Dict[str, Any]) -> bool:
        """Execute the action"""
        pass

    @abstractmethod
    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate action can be executed"""
        pass

    async def rollback(self) -> None:
        """Rollback the action if possible"""
        pass