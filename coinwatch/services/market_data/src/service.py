import asyncio
from typing import Optional, Set, Dict, Any, List
from sqlalchemy.exc import DBAPIError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import MarketDataConfig
from shared.core.enums import ServiceStatus, Timeframe
from shared.core.models import SymbolInfo
from shared.core.exceptions import ServiceError
from shared.core.service import ServiceBase
from shared.database.repositories import KlineRepository, SymbolRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, SymbolMessage, ErrorMessage
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils
from shared.utils.error import ErrorTracker

from .collector import DataCollector

logger = LoggerSetup.setup(__name__)


class MarketDataService(ServiceBase):
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
                 message_broker: MessageBroker,
                 config: MarketDataConfig):
        super().__init__(config)

        # Core dependencies
        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry
        self.message_broker = message_broker
        self.base_timeframe = Timeframe(config.default_timeframe)
        self._retention_days = 90

        # Core components
        self.data_collector = DataCollector(
            adapter_registry=exchange_registry,
            symbol_repository=symbol_repository,
            kline_repository=kline_repository,
            message_broker=message_broker,
            base_timeframe=self.base_timeframe
        )

        # Service state
        self._active_symbols: Set[SymbolInfo] = set()
        self._status: ServiceStatus = ServiceStatus.STOPPED
        self._start_time: Optional[int] = None
        self._last_error: Optional[Exception] = None
        self._symbol_check_interval = 3600  # 1 hour
        self._symbol_lock = asyncio.Lock()

        # Concurrency control
        self._max_concurrent_collections = 5  # Limit concurrent collections
        self._batch_size = self._max_concurrent_collections * 2
        self._collection_semaphore = asyncio.Semaphore(self._max_concurrent_collections)

        # Task management
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_running = asyncio.Event()

        # Error tracking and retry strategy
        self._error_tracker = ErrorTracker()

    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = TimeUtils.get_current_timestamp()
            logger.info("Starting market data service")

            # Connect to message broker and register handlers
            await self.message_broker.connect()
            await self.message_broker.subscribe(MessageType.GAP_DETECTED, self._handle_gap_message)

            # Start monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(self._monitor_symbols())

            self._status = ServiceStatus.RUNNING
            logger.info("Market data service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Failed to start market data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop market data service"""
        try:
            self._status = ServiceStatus.STOPPING
            logger.info("Stopping market data service")

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
            await self.data_collector.cleanup()
            await self.message_broker.close()

            # Clear service state
            self._active_symbols.clear()
            self._last_error = None

            self._status = ServiceStatus.STOPPED
            logger.info("Market data service stopped successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Error during service shutdown: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")

    async def _process_symbol(self, symbol: SymbolInfo) -> None:
        """Process a single symbol with semaphore control"""
        async with self._collection_semaphore:
            checked_symbol = symbol.check_retention_time(self._retention_days)
            self._active_symbols.add(checked_symbol)

            @retry(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=1, max=10),
                retry=retry_if_exception_type(DBAPIError),
                reraise=True
            )
            async def ensure_symbol_exists():
                if not await self.symbol_repository.get_symbol(checked_symbol):
                    await self.symbol_repository.create_symbol(checked_symbol)
                    await self._publish_symbol_added(checked_symbol)

            await ensure_symbol_exists()

            latest_timestamp = await self.kline_repository.get_latest_timestamp(checked_symbol)

            await self.data_collector.collect(
                symbol=checked_symbol,
                start_time=latest_timestamp if latest_timestamp else checked_symbol.launch_time,
                end_time=TimeUtils.get_current_timestamp(),
                context={"type": "initial"}
            )

    async def _process_symbols(self, symbols: List[SymbolInfo]) -> None:
        """Process symbols in batches for better memory management"""
        for i in range(0, len(symbols), self._batch_size):
            batch = symbols[i:i + self._batch_size]
            tasks = [self._process_symbol(symbol) for symbol in batch]

            # Process batch concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

            # Optional: Add small delay between batches to prevent overwhelming
            await asyncio.sleep(self._batch_size / 20)

    async def _monitor_symbols(self) -> None:
        """Monitor trading symbols across exchanges"""
        try:
            while self._monitor_running.is_set():
                try:
                    async with self._symbol_lock:
                        for exchange in self.exchange_registry.get_registered():
                            adapter = self.exchange_registry.get_adapter(exchange)
                            try:
                                symbols = await adapter.get_symbols()

                                # Process new symbols in batches with limited concurrency
                                new_symbols = [s for s in symbols if s not in self._active_symbols]
                                if new_symbols:
                                    await self._process_symbols(new_symbols)

                                # Handle delisted symbols
                                delisted = self._active_symbols - set(symbols)
                                for symbol in delisted:
                                    self._active_symbols.remove(symbol)
                                    await self.data_collector.delist(symbol)
                                    await self._publish_symbol_delisted(symbol)

                            except Exception as e:
                                await self._handle_exchange_error(e, exchange)

                    await asyncio.sleep(self._symbol_check_interval)

                except Exception as e:
                    logger.error(f"Error in symbol monitoring cycle: {e}")
                    raise

        except asyncio.CancelledError:
            logger.info("Symbol monitoring cancelled")
            raise

        except Exception as e:
            await self._handle_critical_error(e)

    async def _handle_gap_message(self, message: Dict[str, Any]) -> None:
        """Handle gap detection message"""
        try:
            # Get symbol info from exchange
            adapter = self.exchange_registry.get_adapter(message["exchange"])
            symbols = await adapter.get_symbols(message["symbol"])
            if not symbols:
                logger.warning(f"Symbol {message['symbol']} not found on {message['exchange']}")
                return
            symbol = symbols[0]  # get_symbols returns a list, but with symbol param it should have only one item

            # Only process if symbol is active and not in historical collection
            if (symbol in self._active_symbols and
                symbol not in self.data_collector._processing_symbols):

                gaps = message["gaps"]
                for start, end in gaps:
                    await self.data_collector.collect(
                        symbol=symbol,
                        start_time=start,
                        end_time=end,
                        context={
                            "type": "gap_fill",
                            "gap_size": (end - start) // self.base_timeframe.to_milliseconds()
                        })

        except Exception as e:
            logger.error(f"Error handling gap for {message['symbol']}: {e}")

    async def _publish_symbol_added(self, symbol: SymbolInfo) -> None:
        """Publish symbol added event"""
        await self.message_broker.publish(
            MessageType.SYMBOL_ADDED,
            SymbolMessage(
                service="market_data",
                type=MessageType.SYMBOL_ADDED,
                timestamp=TimeUtils.get_current_timestamp(),
                symbol=symbol.name,
                exchange=symbol.exchange,
                base_asset=symbol.base_asset,
                quote_asset=symbol.quote_asset,
                first_trade_time=symbol.launch_time
            ).model_dump()
        )

    async def _publish_symbol_delisted(self, symbol: SymbolInfo) -> None:
        """Publish symbol delisted event"""
        await self.message_broker.publish(
            MessageType.SYMBOL_DELISTED,
            SymbolMessage(
                service="market_data",
                type=MessageType.SYMBOL_DELISTED,
                timestamp=TimeUtils.get_current_timestamp(),
                symbol=symbol.name,
                exchange=symbol.exchange,
                base_asset=symbol.base_asset,
                quote_asset=symbol.quote_asset
            ).model_dump()
        )

    async def _handle_exchange_error(self, error: Exception, exchange: str) -> None:
        """Handle exchange-specific errors"""
        await self._error_tracker.record_error(error, exchange)
        await self.message_broker.publish(
            MessageType.ERROR_REPORTED,
            ErrorMessage(
                service="market_data",
                type=MessageType.ERROR_REPORTED,
                timestamp=TimeUtils.get_current_timestamp(),
                error_type="ExchangeError",
                severity="error",
                message=str(error),
                context={
                    "exchange": exchange
                }
            ).model_dump()
        )

    async def _handle_critical_error(self, error: Exception) -> None:
        """Handle critical service errors"""
        self._status = ServiceStatus.ERROR
        self._last_error = error
        await self.message_broker.publish(
            MessageType.ERROR_REPORTED,
            ErrorMessage(
                service="market_data",
                type=MessageType.ERROR_REPORTED,
                timestamp=TimeUtils.get_current_timestamp(),
                error_type="CriticalError",
                severity="critical",
                message=str(error),
                context={
                    "component": "monitor",
                    "active_symbols": len(self._active_symbols)
                }
            ).model_dump()
        )

    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status_lines = [
            "Market Data Service Status:",
            f"Service State: {self._status.value}",
            f"Active Symbols: {len(self._active_symbols)}",
            "",
            "Collection Status:",
            self.data_collector.get_collection_status()
        ]

        error_summary = self._error_tracker.get_error_summary(window_minutes=60)
        if error_summary:
            status_lines.extend([
                "",
                "Recent Errors:"
            ])
            for error_type, count in error_summary.items():
                status_lines.append(f"  {error_type}: {count} in last hour")

        return "\n".join(status_lines)