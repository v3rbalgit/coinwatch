# src/services/fundamental_data/service.py

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from ...adapters.coingecko import CoinGeckoAdapter
from ...repositories.metadata import MetadataRepository
from ...services.fundamental_data.metadata_collector import MetadataCollector
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.exceptions import ServiceError
from ...utils.domain_types import ServiceStatus
from ...services.base import ServiceBase
from ...utils.logger import LoggerSetup
from ...core.models import SymbolInfo
from ...utils.error import ErrorTracker

logger = LoggerSetup.setup(__name__)

@dataclass
class FundamentalDataConfig:
    """Configuration for fundamental data collection intervals and batch sizes."""
    collection_intervals: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 86400 * 30,  # Monthly
        'market': 86400,         # Daily
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
                 metadata_repository: MetadataRepository,
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
        self.metadata_repository = metadata_repository
        self.coingecko_adapter = coingecko_adapter

        self._status = ServiceStatus.STOPPED
        self._active_tokens: Set[str] = set()
        self._token_exchanges: Dict[str, Set[str]] = {}

        # Error tracking
        self._error_tracker = ErrorTracker()
        self._last_error: Optional[Exception] = None

        # Collection management
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolInfo] = set()

        # Initialize collectors (will add as we implement them)
        self._collectors = {
            'metadata': MetadataCollector(
                self.coordinator,
                metadata_repository,
                coingecko_adapter,
                config.collection_intervals['metadata'],
                config.batch_sizes['metadata']
                ),
            # 'market': MarketMetricsCollector(...),
            # 'blockchain': BlockchainMetricsCollector(...),
            # 'sentiment': SentimentMetricsCollector(...)
        }

    async def _register_command_handlers(self) -> None:
        """Register command handlers with the service coordinator."""
        handlers = {
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.SYMBOL_DELISTED: self._handle_symbol_delisted
        }
        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)

    async def _unregister_command_handlers(self) -> None:
        """Unregister command handlers from the service coordinator."""
        handlers = {
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.SYMBOL_DELISTED: self._handle_symbol_delisted
        }
        for command, handler in handlers.items():
            await self.coordinator.unregister_handler(command, handler)

    async def _handle_collection_complete(self, command: Command) -> None:
        """
        Handle the completion of market data collection.

        Args:
            command (Command): The collection complete command.
        """
        symbol = command.params["symbol"]
        base_token = symbol.symbol_name  # Using property from SymbolInfo

        self._active_tokens.add(base_token)
        if base_token not in self._token_exchanges:
            self._token_exchanges[base_token] = set()
        self._token_exchanges[base_token].add(symbol.exchange)

        # Schedule collectors only if this is first listing of token
        if len(self._token_exchanges[base_token]) == 1:
            for collector in self._collectors.values():
                await collector.schedule_collection(symbol)

    async def _handle_symbol_delisted(self, command: Command) -> None:
        """
        Handle symbol delisting.

        Args:
            command (Command): The symbol delisted command.
        """
        symbol = command.params["symbol"]
        base_token = symbol.symbol_name

        if base_token in self._token_exchanges:
            self._token_exchanges[base_token].discard(symbol.exchange)
            # Only remove from active tokens if no exchanges list it
            if not self._token_exchanges[base_token]:
                self._active_tokens.discard(base_token)
                self._token_exchanges.pop(base_token)

    async def start(self) -> None:
        """Start the service"""
        try:
            self._status = ServiceStatus.STARTING
            await self._register_command_handlers()
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

            # Cleanup collectors
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
            "\nToken Listings:",
            *[f"  {token}: {len(exchanges)} exchange(s)"
              for token, exchanges in self._token_exchanges.items()],
            "\nCollection Status:"
        ]

        # Add collector statuses
        for name, collector in self._collectors.items():
            status_lines.append(f"\n{name.title()} Collector:")
            status_lines.append(collector.get_collection_status())

        return "\n".join(status_lines)