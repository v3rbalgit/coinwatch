# src/adapters/registry.py

from typing import Dict, List
from ..domain_types import ExchangeName
from ..core.protocols import ExchangeAdapter
from ..core.exceptions import AdapterError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class ExchangeAdapterRegistry:
    """Registry for managing exchange adapters"""

    def __init__(self):
        self._adapters: Dict[ExchangeName, ExchangeAdapter] = {}
        self._initialized_adapters: Dict[ExchangeName, bool] = {}

    async def register(self, name: ExchangeName, adapter: ExchangeAdapter) -> None:
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

    async def initialize(self, name: ExchangeName) -> None:
        """Initialize a specific adapter"""
        try:
            adapter = self._adapters.get(name)
            if not adapter:
                raise AdapterError(f"No adapter registered for exchange: {name}")

            if not self._initialized_adapters.get(name, False):
                await adapter.initialize()
                self._initialized_adapters[name] = True
                logger.info(f"Initialized adapter for exchange: {name}")

        except Exception as e:
            logger.error(f"Failed to initialize adapter for {name}: {e}")
            raise AdapterError(f"Adapter initialization failed: {str(e)}")

    async def initialize_all(self) -> None:
        """Initialize all registered adapters"""
        for name in self._adapters.keys():
            await self.initialize(name)

    def get_adapter(self, name: ExchangeName) -> ExchangeAdapter:
        """Get a registered adapter"""
        adapter = self._adapters.get(name)
        if not adapter:
            raise AdapterError(f"No adapter registered for exchange: {name}")

        if not self._initialized_adapters.get(name, False):
            raise AdapterError(f"Adapter not initialized for exchange: {name}")

        return adapter

    def get_registered(self) -> List[ExchangeName]:
        """Get list of registered exchanges"""
        return list(self._adapters.keys())

    def is_initialized(self, name: ExchangeName) -> bool:
        """Check if an adapter is initialized"""
        return self._initialized_adapters.get(name, False)

    async def close(self, name: ExchangeName) -> None:
        """Close specific adapter connection"""
        try:
            adapter = self._adapters.get(name)
            if adapter:
                await adapter.close()
                self._initialized_adapters[name] = False
                logger.info(f"Closed adapter for exchange: {name}")

        except Exception as e:
            logger.error(f"Error closing adapter for {name}: {e}")
            raise AdapterError(f"Failed to close adapter: {str(e)}")

    async def close_all(self) -> None:
        """Close all adapter connections"""
        for name in self._adapters.keys():
            await self.close(name)