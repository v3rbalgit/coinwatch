# src/adapters/registry.py

from typing import Dict, List, Optional
from abc import abstractmethod

from shared.core.models import KlineData, SymbolInfo
from shared.core.adapter import APIAdapter
from shared.utils.domain_types import Timeframe
from shared.core.exceptions import AdapterError
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class ExchangeAdapter(APIAdapter):
    """
    Base class for exchange adapters extending APIAdapter.
    Defines required methods for exchange data collection.
    """

    @abstractmethod
    async def get_symbols(self) -> List[SymbolInfo]:
        """Get available trading pairs"""
        pass

    @abstractmethod
    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
                        limit: Optional[int] = None) -> List[KlineData]:
        """Get kline data"""
        pass

class ExchangeAdapterRegistry:
    """Registry for managing exchange adapters"""

    def __init__(self):
        self._adapters: Dict[str, ExchangeAdapter] = {}
        self._initialized_adapters: Dict[str, bool] = {}

    async def register(self, name: str, adapter: ExchangeAdapter) -> None:
        """Register a new exchange adapter"""
        try:
            if name in self._adapters:
                raise AdapterError(f"Adapter already registered for exchange: {name}")

            self._adapters[name] = adapter
            self._initialized_adapters[name] = False
            logger.info(f"Registered adapter for exchange: {name}")

        except Exception as e:
            logger.error(f"Failed to register adapter for {name}: {e}")
            raise AdapterError(f"Adapter registration failed: {str(e)}")

    async def unregister(self, name: str) -> None:
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

        if not self._initialized_adapters.get(name, False):
            raise AdapterError(f"Adapter not initialized for exchange: {name}")

        return adapter

    def get_registered(self) -> List[str]:
        """Get list of registered exchanges"""
        return list(self._adapters.keys())