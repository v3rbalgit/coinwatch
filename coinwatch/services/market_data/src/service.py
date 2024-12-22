import asyncio

from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import MarketDataConfig
from shared.core.enums import ServiceStatus, Interval
from shared.core.models import SymbolModel
from shared.core.exceptions import ServiceError
from shared.core.protocols import Service
from shared.database.repositories import KlineRepository, SymbolRepository
from shared.utils.logger import LoggerSetup
from .collector import KlineCollector
from shared.utils.time import get_current_timestamp



class MarketDataService(Service):
    """
    Core service managing market data collection and real-time streaming.

    Features:
    - Historical and real-time price data collection
    - Multi-exchange symbol monitoring and lifecycle management
    - Automated gap detection and recovery
    - Error tracking and retry mechanisms
    - Real-time data streaming via websocket
    """
    def __init__(self,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 exchange_registry: ExchangeAdapterRegistry,
                 config: MarketDataConfig):

        # Core dependencies
        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry
        self.base_interval = Interval(config.default_interval)
        self._retention_days = config.retention_days

        # Core components
        self.kline_collector = KlineCollector(
            adapter_registry=exchange_registry,
            symbol_repository=symbol_repository,
            kline_repository=kline_repository,
            base_interval=self.base_interval
        )

        # Service state
        self._active_symbols: set[SymbolModel] = set()
        self._status: ServiceStatus = ServiceStatus.STOPPED
        self._start_time: int | None = None
        self._last_error: Exception | None = None
        self._symbol_check_interval = 3600  # 1 hour
        self._symbol_lock = asyncio.Lock()

        # Concurrency control
        self._max_concurrent_collections = config.batch_size // 2
        self._batch_size = config.batch_size
        self._collection_semaphore = asyncio.Semaphore(self._max_concurrent_collections)

        # Task management
        self._monitor_task: asyncio.Task | None = None
        self._monitor_running = asyncio.Event()

        self.logger = LoggerSetup.setup(__class__.__name__)


    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = get_current_timestamp()
            self.logger.info("Starting market data service")

            # Start monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(self._monitor_symbols())

            self._status = ServiceStatus.RUNNING
            self.logger.info("Market data service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            self.logger.error(f"Failed to start market data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")


    async def stop(self) -> None:
        """Stop market data service"""
        try:
            self._status = ServiceStatus.STOPPING
            self.logger.info("Stopping market data service")

            # Stop monitoring
            self._monitor_running.clear()
            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
                self._monitor_task = None

            # Cleanup components
            await self.kline_collector.cleanup()

            # Clear service state
            self._active_symbols.clear()
            self._last_error = None

            self._status = ServiceStatus.STOPPED
            self.logger.info("Market data service stopped successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            self.logger.error(f"Error during service shutdown: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")


    async def _process_symbol(self, symbol: SymbolModel) -> None:
        """Process a single symbol with semaphore control"""
        async with self._collection_semaphore:
            symbol = symbol.adjust_launch_time(self._retention_days)
            self._active_symbols.add(symbol)

            if not await self.symbol_repository.get_symbol(symbol):
                await self.symbol_repository.create_symbol(symbol)

            # Get timestamps of oldest and newest kline records of a symbol
            timestamp_range = await self.kline_repository.get_timestamp_range(symbol)

            # Backfill if launch time is older than current time range
            if timestamp_range:
                start_time = symbol.launch_time if symbol.launch_time < timestamp_range[0] else timestamp_range[1]
                end_time = timestamp_range[0] if symbol.launch_time < timestamp_range[0] else get_current_timestamp()
            else:
                start_time = symbol.launch_time
                end_time = get_current_timestamp()

            await self.kline_collector.collect(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )


    async def _process_symbols(self, symbols: list[SymbolModel]) -> None:
        """Process symbols in batches for better memory management"""
        for i in range(0, len(symbols), self._batch_size):
            batch = symbols[i:i + self._batch_size]
            tasks = [self._process_symbol(symbol) for symbol in batch]

            await asyncio.gather(*tasks, return_exceptions=True)

            # Add small delay between batches to prevent overwhelming
            await asyncio.sleep(0.05)


    async def _monitor_symbols(self) -> None:
        """Monitor trading symbols across exchanges"""
        try:
            while self._monitor_running.is_set():
                async with self._symbol_lock:
                    for exchange in self.exchange_registry.get_registered():
                        adapter = self.exchange_registry.get_adapter(exchange)

                        symbols = await adapter.get_symbols()

                        # Process new symbols in batches with limited concurrency
                        new_symbols = [s for s in symbols if s not in self._active_symbols]
                        if new_symbols:
                            self.logger.info(f"New symbols: {len(new_symbols)}")
                            await self._process_symbols(new_symbols)

                        # Handle delisted symbols
                        delisted = self._active_symbols - set(symbols)
                        for symbol in delisted:
                            self.logger.info(f"Delisted symbols: {len(delisted)}")
                            self._active_symbols.remove(symbol)
                            await self.kline_collector.delist(symbol)

                    await asyncio.sleep(self._symbol_check_interval)

        except asyncio.CancelledError:
            self.logger.info("Symbol monitoring cancelled")
            raise

        except Exception as e:
            self.logger.error(f"Error in symbol monitoring cycle: {e}")
            self._last_error = e


    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status_lines = [
            "Market Data Service Status:",
            f"Service State: {self._status.value}",
            f"Active Symbols: {len(self._active_symbols)}",
            "",
            "Collection Status:",
            self.kline_collector.get_collection_status(),
            "Recent Error:",
            f"{type(self._last_error).__name__} {str(self._last_error)}"
        ]

        return "\n".join(status_lines)