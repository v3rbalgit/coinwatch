from typing import AsyncGenerator, Callable, Coroutine, Any

from shared.core.protocols import APIAdapter
from shared.core.enums import Interval
from shared.core.exceptions import AdapterError
from shared.core.models import KlineData, SymbolInfo
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class ExchangeAdapter(APIAdapter):
    """
    Base class for exchange adapters extending APIAdapter.
    Defines required methods for exchange data collection.
    """

    async def get_symbols(self, symbol: str | None = None) -> list[SymbolInfo]:
        """Get available trading pairs"""
        ...

    async def get_klines(self,
                        symbol: SymbolInfo,
                        interval: Interval,
                        start_time: int,
                        end_time: int,
                        limit: int | None = None) -> AsyncGenerator[list[KlineData], None]:
        """Get kline data"""
        ...

    async def subscribe_klines(self,
                             symbol: SymbolInfo,
                             interval: Interval,
                             handler: Callable[[dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Subscribe to real-time kline updates for a symbol"""
        ...

    async def unsubscribe_klines(self,
                                symbol: SymbolInfo,
                                interval: Interval) -> None:
        """Unsubscribe from kline updates for a symbol"""
        ...


class ExchangeAdapterRegistry:
    """Registry for managing exchange adapters"""

    def __init__(self):
        self._adapters: dict[str, ExchangeAdapter] = {}

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

    def get_registered(self) -> list[str]:
        """Get list of registered exchanges"""
        return list(self._adapters.keys())