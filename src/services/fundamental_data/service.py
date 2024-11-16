# src/services/fundamental_data/service.py

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from ...adapters.registry import ExchangeAdapterRegistry
from ...adapters.coingecko import CoinGeckoAdapter
from ...repositories import MarketMetricsRepository, MetadataRepository
from ...services.base import ServiceBase
from ...services.fundamental_data.collector import FundamentalCollector
from ...services.fundamental_data.metadata_collector import MetadataCollector
from ...services.fundamental_data.market_metrics_collector import MarketMetricsCollector
from ...core.models import SymbolInfo
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.exceptions import ServiceError
from ...utils.domain_types import ServiceStatus
from ...utils.logger import LoggerSetup
from ...utils.error import ErrorTracker

logger = LoggerSetup.setup(__name__)

@dataclass
class FundamentalDataConfig:
    """Configuration for fundamental data collection intervals and batch sizes."""
    collection_intervals: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 86400 * 30,  # Monthly
        'market': 3600,          # Hourly
        'blockchain': 86400,     # Daily
        'sentiment': 86400       # Daily
    })
    batch_sizes: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 10,   # Metadata collection is API heavy
        'market': 100,    # Market data can be batched more
        'blockchain': 50,
        'sentiment': 50
    })

class FundamentalDataService(ServiceBase):
    """
    Service for collecting and managing fundamental cryptocurrency data.

    Coordinates different metric collectors, manages collection schedules,
    handles resource allocation, provides metrics aggregation, and monitors collection health.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 exchange_registry: ExchangeAdapterRegistry,
                 metadata_repository: MetadataRepository,
                 market_metrics_repository: MarketMetricsRepository,
                 coingecko_adapter: CoinGeckoAdapter,
                 config: FundamentalDataConfig):
        """
        Initialize the FundamentalDataService.

        Args:
            coordinator (ServiceCoordinator): Service coordination manager.
            metadata_repository (MetadataRepository): Repository for metadata storage.
            coingecko_adapter (CoinGeckoAdapter): Adapter for CoinGecko API.
            config (FundamentalDataConfig): Service configuration.
        """
        super().__init__(config)
        self.coordinator = coordinator
        self.exchange_registry = exchange_registry
        self.metadata_repository = metadata_repository
        self.market_metrics_repository = market_metrics_repository
        self.coingecko_adapter = coingecko_adapter

        self._status = ServiceStatus.STOPPED
        self._active_tokens: Set[str] = set()

        # Error tracking
        self._error_tracker = ErrorTracker()
        self._last_error: Optional[Exception] = None

        # Collection management
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolInfo] = set()

        # Initialize collectors (will add as we implement them)
        self._collectors: Dict[str, FundamentalCollector] = {
            'metadata': MetadataCollector(
                self.metadata_repository,
                self.coingecko_adapter,
                config.collection_intervals['metadata']
                ),
            'market': MarketMetricsCollector(
                self.market_metrics_repository,
                self.coingecko_adapter,
                config.collection_intervals['market']
            )
            # 'blockchain': BlockchainMetricsCollector(...),
            # 'sentiment': SentimentMetricsCollector(...)
        }

    async def _register_command_handlers(self) -> None:
        """Register command handlers with the service coordinator."""
        handlers = {
            MarketDataCommand.SYMBOL_DELISTED: self._handle_symbol_delisted,
            MarketDataCommand.SYMBOL_ADDED: self._handle_symbol_added
        }
        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)

    async def _unregister_command_handlers(self) -> None:
        """Unregister command handlers from the service coordinator."""
        handlers = {
            MarketDataCommand.SYMBOL_DELISTED: self._handle_symbol_delisted,
            MarketDataCommand.SYMBOL_ADDED: self._handle_symbol_added
        }
        for command, handler in handlers.items():
            await self.coordinator.unregister_handler(command, handler)

    async def _handle_symbol_delisted(self, command: Command) -> None:
        """Handle symbol delisting"""
        symbol: SymbolInfo = command.params["symbol"]
        base_token: str = symbol.name.lower().replace('usdt', '')

        # Check if token is still traded on other exchanges
        still_active = False
        for exchange in self.exchange_registry.get_registered():
            adapter = self.exchange_registry.get_adapter(exchange)
            symbols = await adapter.get_symbols()
            if any(s.name.lower().replace('usdt', '') == base_token for s in symbols):
                still_active = True
                break

        if not still_active:
            self._active_tokens.discard(base_token)
            logger.info(f"Stopped tracking {base_token} - no longer traded")

    async def _handle_symbol_added(self, command: Command) -> None:
        """
        Handle new symbol addition.
        Only schedule collection if we're not already tracking this token.
        """
        symbol: SymbolInfo = command.params["symbol"]
        base_token: str = symbol.name.lower().replace('usdt', '')

        async with self._collection_lock:
            if base_token not in self._active_tokens:
                self._active_tokens.add(base_token)

                # Schedule collection for new token
                for collector in self._collectors.values():
                    await collector.schedule_collection({base_token})

                logger.info(f"Started tracking new token: {base_token}")

    async def _collect_active_symbols(self) -> None:
        """Initial collection of unique base tokens from all exchanges"""
        unique_tokens: Set[str] = set()

        for exchange in self.exchange_registry.get_registered():
            adapter = self.exchange_registry.get_adapter(exchange)
            symbols = await adapter.get_symbols()

            # Extract unique base tokens and keep one symbol instance
            for symbol in symbols:
                base_token = symbol.name.lower().replace('usdt', '')
                unique_tokens.add(base_token)

        async with self._collection_lock:
            self._active_tokens.update(unique_tokens)

        logger.info(f"Collecting fundamental data for {len(unique_tokens)} unique tokens")

        for collector in self._collectors.values():
            await collector.schedule_collection(unique_tokens)

    async def start(self) -> None:
        """Start the service"""
        try:
            self._status = ServiceStatus.STARTING
            await self._register_command_handlers()

            # Initial collection of all active symbols
            await self._collect_active_symbols()

            self._status = ServiceStatus.RUNNING
            logger.info("Fundamental data service started successfully")
        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Failed to start fundamental data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop the service"""
        try:
            self._status = ServiceStatus.STOPPING
            await self._unregister_command_handlers()

            for collector in self._collectors.values():
                await collector.cleanup()

            self._status = ServiceStatus.STOPPED
        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping fundamental data service: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")

    def get_service_status(self) -> str:
        """
        Get a comprehensive status report of the service.

        Returns:
            str: A formatted string containing the service status.
        """
        status_lines = [
            "Fundamental Data Service Status:",
            f"Status: {self._status.value}",
            f"Active Tokens: {len(self._active_tokens)}",
            "\nCollection Status:"
        ]

        for name, collector in self._collectors.items():
            status_lines.append(f"\n{name.title()} Collector:")
            status_lines.append(collector.get_collection_status())

        return "\n".join(status_lines)