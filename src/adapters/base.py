# src/adapters/base.py
from abc import ABC
from typing import Optional
from ..core.protocols import ExchangeAdapter

class BaseExchangeAdapter(ABC, ExchangeAdapter):
    """Base class for exchange adapters with shared functionality"""

    def __init__(self):
        self._last_error: Optional[Exception] = None
        self._connected: bool = False

    async def connect(self) -> None:
        """Shared connection logic"""
        self._connected = True

    async def disconnect(self) -> None:
        """Shared disconnection logic"""
        self._connected = False

    def is_connected(self) -> bool:
        """Connection status check"""
        return self._connected

    def last_error(self) -> Optional[Exception]:
        """Get last error"""
        return self._last_error