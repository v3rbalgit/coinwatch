import asyncio

from .collectors import FundamentalCollector, MetadataCollector, MarketMetricsCollector, SentimentMetricsCollector
from shared.clients.coingecko import CoinGeckoAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import FundamentalDataConfig
from shared.core.enums import ServiceStatus
from shared.core.exceptions import ServiceError
from shared.core.protocols import Service
from shared.database.repositories import MarketMetricsRepository, MetadataRepository, SentimentRepository
from shared.utils.logger import LoggerSetup
from shared.utils.error import ErrorTracker
from shared.utils.time import get_current_timestamp



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

        # Service state
        self._status: ServiceStatus = ServiceStatus.STOPPED
        self._active_tokens: set[str] = set()
        self._start_time = get_current_timestamp()

        # Error tracking
        self._error_tracker = ErrorTracker()
        self._last_error: Exception | None = None

        # Collection management
        self._collection_lock = asyncio.Lock()
        self._collector_tasks: dict[str, asyncio.Task] = {}
        self._monitor_task: asyncio.Task | None = None
        self._monitor_running = asyncio.Event()
        self._symbol_check_interval = 3600  # 1 hour

        # Initialize collectors
        self._collectors: dict[str, FundamentalCollector] = {
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
            # 'sentiment': SentimentMetricsCollector(
            #     self.sentiment_repository,
            #     self.metadata_repository,
            #     config.sentiment,
            #     config.collection_intervals['sentiment']
            # )
        }

        self.logger = LoggerSetup.setup(__class__.__name__)


    async def _monitor_symbols(self) -> None:
        """Monitor trading symbols across exchanges"""
        try:
            while self._monitor_running.is_set():
                async with self._collection_lock:
                    # Get currently available tokens across all exchanges
                    current_tokens = set()
                    for exchange in self.exchange_registry.get_registered():
                        adapter = self.exchange_registry.get_adapter(exchange)
                        symbols = await adapter.get_symbols()
                        current_tokens.update({symbol.token_name for symbol in symbols})

                    # Detect new and delisted tokens
                    new_tokens = current_tokens - self._active_tokens
                    delisted_tokens = self._active_tokens - current_tokens

                    if new_tokens:
                        self.logger.info(f"New tokens: {new_tokens}")
                        self._active_tokens.update(new_tokens)
                        # Add new tokens to all collectors
                        for name, collector in self._collectors.items():
                            await collector.add_tokens(new_tokens)
                            if not collector._running and name not in self._collector_tasks:
                                self._collector_tasks[name] = asyncio.create_task(collector.run())

                    if delisted_tokens:
                        self.logger.info(f"Delisted tokens: {delisted_tokens}")
                        self._active_tokens.difference_update(delisted_tokens)
                        # Remove delisted tokens from all collectors
                        for collector in self._collectors.values():
                            await collector.remove_tokens(delisted_tokens)

                await asyncio.sleep(self._symbol_check_interval)

        except asyncio.CancelledError:
            self.logger.info("Symbol monitoring cancelled")
            raise

        except Exception as e:
            self.logger.error(f"Error in symbol monitoring cycle: {e}")

    async def start(self) -> None:
        """Start the service"""
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = get_current_timestamp()

            # Start monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(self._monitor_symbols())

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
            self._monitor_running.clear()

            # Stop monitor task
            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
                self._monitor_task = None

            # Stop collectors
            for collector in self._collectors.values():
                await collector.cleanup()

            # Cleanup Coingecko adapter
            await self.coingecko_adapter.cleanup()

            # Cancel collector tasks
            for name, task in self._collector_tasks.items():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self._collector_tasks.clear()

            # Clear state
            self._active_tokens.clear()

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
