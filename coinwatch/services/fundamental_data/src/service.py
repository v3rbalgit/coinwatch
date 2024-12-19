import asyncio
from typing import Dict, Set

from .collectors import FundamentalCollector, MetadataCollector, MarketMetricsCollector, SentimentMetricsCollector
from shared.clients.coingecko import CoinGeckoAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import FundamentalDataConfig
from shared.core.enums import ServiceStatus
from shared.core.exceptions import ServiceError
from shared.core.models import SymbolModel
from shared.core.protocols import Service
from shared.database.repositories import MarketMetricsRepository, MetadataRepository, SentimentRepository
from shared.messaging.schemas import MessageType, SymbolMessage
from shared.messaging.broker import MessageBroker
from shared.utils.logger import LoggerSetup
from shared.utils.error import ErrorTracker
import shared.utils.time as TimeUtils


class FundamentalDataService(Service):
    """
    Service for collecting and managing fundamental cryptocurrency data.

    Coordinates different metric collectors, manages collection schedules,
    handles resource allocation, provides metrics aggregation, and monitors collection health.
    """

    def __init__(self,
                 metadata_repository: MetadataRepository,
                 market_metrics_repository: MarketMetricsRepository,
                 sentiment_repository: SentimentRepository,
                 exchange_registry: ExchangeAdapterRegistry,
                 coingecko_adapter: CoinGeckoAdapter,
                 message_broker: MessageBroker,
                 config: FundamentalDataConfig):
        """
        Initialize the FundamentalDataService.

        Args:
            metadata_repository (MetadataRepository): Repository for metadata storage.
            market_metrics_repository (MarketMetricsRepository): Repository for market metrics storage.
            sentiment_repository (SentimentRepository): Repository for sentiment metrics storage.
            exchange_registry (ExchangeAdapterRegistry): Registry of API adapters for exchanges.
            coingecko_adapter (CoinGeckoAdapter): Adapter for CoinGecko API.
            message_broker (MessageBroker): Message broker for service communication.
            config (FundamentalDataConfig): Service configuration.
        """
        self.exchange_registry = exchange_registry
        self.metadata_repository = metadata_repository
        self.market_metrics_repository = market_metrics_repository
        self.sentiment_repository = sentiment_repository
        self.coingecko_adapter = coingecko_adapter
        self.message_broker = message_broker

        # Service state
        self._status: ServiceStatus = ServiceStatus.STOPPED
        self._active_tokens: Set[str] = set()
        self._start_time = TimeUtils.get_current_timestamp()

        # Error tracking
        self._error_tracker = ErrorTracker()
        self._last_error: Exception | None = None

        # Collection management
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolModel] = set()

        # Initialize collectors
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
            ),
            'sentiment': SentimentMetricsCollector(
                self.sentiment_repository,
                self.metadata_repository,
                config.sentiment,
                config.collection_intervals['sentiment']
            )
        }

        self.logger = LoggerSetup.setup(__class__.__name__)

    async def _setup_message_handlers(self) -> None:
        """Setup message handlers for market data events"""
        await self.message_broker.subscribe(
            MessageType.SYMBOL_ADDED,
            self._handle_symbol_added
        )
        await self.message_broker.subscribe(
            MessageType.SYMBOL_DELISTED,
            self._handle_symbol_delisted
        )

    async def _handle_symbol_delisted(self, message: Dict) -> None:
        """Handle symbol delisting"""
        symbol = SymbolMessage(**message)
        base_token = symbol.base_asset

        # Check if token is still traded on other exchanges
        still_active = False
        for exchange in self.exchange_registry.get_registered():
            adapter = self.exchange_registry.get_adapter(exchange)
            symbols = await adapter.get_symbols()
            if any(s.token_name == base_token for s in symbols):
                still_active = True
                break

        if not still_active:
            async with self._collection_lock:
                self._active_tokens.discard(base_token)
                for collector in self._collectors.values():
                    await collector.delete_symbol_data(base_token)
                self.logger.info(f"Stopped tracking {base_token} - no longer traded")

    async def _handle_symbol_added(self, message: Dict) -> None:
        """
        Handle new symbol addition.
        Only schedule collection if we're not already tracking this token.
        """
        symbol = SymbolMessage(**message)
        base_token = symbol.base_asset

        if base_token not in self._active_tokens:
            async with self._collection_lock:
                self._active_tokens.add(base_token)

                # Schedule collection for new token
                for collector in self._collectors.values():
                    await collector.schedule_collection({base_token})

                self.logger.info(f"Started fundamental data tracking for new token: {base_token}")

    async def _collect_active_symbols(self) -> None:
        """Initial collection of unique base tokens from all exchanges"""
        unique_tokens: Set[str] = set()

        for exchange in self.exchange_registry.get_registered():
            adapter = self.exchange_registry.get_adapter(exchange)
            symbols = await adapter.get_symbols()

            # Extract unique base tokens and keep one symbol instance
            for symbol in symbols:
                base_token = symbol.token_name
                unique_tokens.add(base_token)

        async with self._collection_lock:
            self._active_tokens.update(unique_tokens)

        self.logger.info(f"Collecting fundamental data for {len(unique_tokens)} unique tokens")

        for collector in self._collectors.values():
            await collector.schedule_collection(unique_tokens)

    async def start(self) -> None:
        """Start the service"""
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = TimeUtils.get_current_timestamp()

            # Connect to the message broker
            await self.message_broker.connect()

            # Setup message handlers
            await self._setup_message_handlers()

            # Initial collection of all active symbols
            await self._collect_active_symbols()

            self._status = ServiceStatus.RUNNING
            self.logger.info("Fundamental data service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            self.logger.error(f"Failed to start fundamental data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop the service"""
        try:
            self._status = ServiceStatus.STOPPING

            # Stop collectors
            for collector in self._collectors.values():
                await collector.cleanup()

            # Close message broker connection
            await self.message_broker.close()

            self._status = ServiceStatus.STOPPED
            self.logger.info("Fundamental data service stopped successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self.logger.error(f"Error stopping fundamental data service: {e}")
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
