# src/adapters/registry.py

from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, List, Optional

from shared.core.adapter import APIAdapter
from shared.core.enums import Timeframe
from shared.core.exceptions import AdapterError
from shared.core.models import KlineData, SymbolInfo
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class ExchangeAdapter(APIAdapter):
    """
    Base class for exchange adapters extending APIAdapter.
    Defines required methods for exchange data collection.
    """

    async def get_symbols(self, symbol: Optional[str] = None) -> List[SymbolInfo]:
        """Get available trading pairs"""
        ...

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: int,
                        end_time: int,
                        limit: Optional[int] = None) -> AsyncGenerator[List[KlineData], None]:
        """Get kline data"""
        ...

    async def subscribe_klines(self,
                             symbol: SymbolInfo,
                             timeframe: Timeframe,
                             handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """
        Subscribe to real-time kline updates for a symbol.
        """
        ...

    async def unsubscribe_klines(self,
                                symbol: SymbolInfo,
                                timeframe: Timeframe) -> None:
        """
        Unsubscribe from kline updates for a symbol.
        """
        ...


class ExchangeAdapterRegistry:
    """Registry for managing exchange adapters"""

    def __init__(self):
        self._adapters: Dict[str, ExchangeAdapter] = {}

    def register(self, name: str, adapter: ExchangeAdapter) -> None:
        """Register a new exchange adapter"""
        try:
            if name in self._adapters:
                raise AdapterError(f"Adapter already registered for exchange: {name}")

            self._adapters[name] = adapter
            logger.info(f"Registered adapter for exchange: {name}")

        except Exception as e:
            logger.error(f"Failed to register adapter for {name}: {e}")
            raise AdapterError(f"Adapter registration failed: {str(e)}")

    def unregister(self, name: str) -> None:
        """Unregister an exchange adapter"""
        try:
            if name not in self._adapters:
                raise AdapterError(f"Adapter already unregistered for exchange: {name}")

            del self._adapters[name]
            logger.info(f"Unregistered adapter for exchange: {name}")

        except Exception as e:
            logger.error(f"Failed to unregister adapter for {name}: {e}")
            raise AdapterError(f"Adapter unregistration failed: {str(e)}")

    def get_adapter(self, name: str) -> ExchangeAdapter:
        """Get a registered adapter"""
        adapter = self._adapters.get(name)
        if not adapter:
            raise AdapterError(f"No adapter registered for exchange: {name}")

        return adapter

    def get_registered(self) -> List[str]:
        """Get list of registered exchanges"""
        return list(self._adapters.keys())