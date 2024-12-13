import os
import asyncio
from typing import Optional, Set

from shared.core.config import MarketDataConfig
from shared.core.models import SymbolInfo
from shared.core.exceptions import ServiceError, ValidationError
from shared.core.service import ServiceBase
from shared.database.repositories.kline import KlineRepository
from shared.database.repositories.symbol import SymbolRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import (
    MessageType,
    SymbolMessage,
    ServiceStatusMessage,
    ErrorMessage
)
from shared.utils.logger import LoggerSetup
from shared.utils.domain_types import ServiceStatus, Timeframe
from shared.utils.time import TimeUtils
from shared.utils.error import ErrorTracker
from shared.utils.retry import RetryConfig, RetryStrategy

from .collector import DataCollector
from .synchronizer import BatchSynchronizer
from .adapters.registry import ExchangeAdapterRegistry

logger = LoggerSetup.setup(__name__)

class MarketDataService(ServiceBase):
    """
    Core service managing market data collection and synchronization across exchanges.

    Features:
    - Historical and real-time price data collection
    - Multi-exchange symbol monitoring and lifecycle management
    - Automated gap detection and recovery
    - Error tracking and retry mechanisms

    Components:
    - DataCollector: Historical data collection
    - BatchSynchronizer: Real-time synchronization
    - ExchangeAdapterRegistry: Exchange connections
    - ErrorTracker: Error monitoring
    - RetryStrategy: Retry logic
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
        self._retention_days = int(os.getenv('TIMESCALE_RETENTION_DAYS', '0'))

        # Core components
        self.data_collector = DataCollector(
            adapter_registry=exchange_registry,
            symbol_repository=symbol_repository,
            kline_repository=kline_repository,
            message_broker=message_broker,
            base_timeframe=self.base_timeframe
        )
        self.batch_synchronizer = BatchSynchronizer(
            adapter_registry=exchange_registry,
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

        # Task management
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_running = asyncio.Event()

        # Error tracking and retry strategy
        self._error_tracker = ErrorTracker()
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=5.0,
            max_delay=300.0,
            max_retries=3,
            jitter_factor=0.25
        ))
        self._configure_retry_strategy()

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior for market data service"""
        # Basic retryable errors
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            ServiceError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )

        # Configure specific delays for different error types
        self._retry_strategy.configure_error_delays({
            ConnectionError: RetryConfig(
                base_delay=5.0,      # Moderate base delay for connection issues
                max_delay=180.0,
                max_retries=4,
                jitter_factor=0.25
            ),
            TimeoutError: RetryConfig(
                base_delay=3.0,      # Balanced delay for timeouts
                max_delay=90.0,
                max_retries=3,
                jitter_factor=0.2
            ),
            ServiceError: RetryConfig(
                base_delay=2.0,      # Quick retry for service errors
                max_delay=45.0,
                max_retries=4,
                jitter_factor=0.15
            )
        })

    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = TimeUtils.get_current_timestamp()
            logger.info("Starting market data service")

            # Connect to message broker
            await self.message_broker.connect()

            # Start monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(self._monitor_symbols())

            self._status = ServiceStatus.RUNNING
            logger.info("Market data service started successfully")

            # Publish service status
            await self.message_broker.publish(
                MessageType.SERVICE_STATUS,
                ServiceStatusMessage(
                    service="market_data",
                    timestamp=TimeUtils.get_current_timestamp(),
                    type=MessageType.SERVICE_STATUS,
                    status=self._status,
                    uptime=0.0,
                    error_count=0,
                    warning_count=0,
                    metrics={}
                ).dict()
            )

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
            cleanup_tasks = [
                self.data_collector.cleanup(),
                self.batch_synchronizer.cleanup(),
                self.message_broker.close()
            ]

            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

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

    async def _monitor_symbols(self) -> None:
        """
        Continuously monitors trading symbols across registered exchanges.

        Core business logic:
        - Discovers new trading symbols and initiates data collection
        - Detects and handles delisted symbols
        - Manages symbol lifecycle and active symbol set
        - Implements retry logic for exchange API failures
        - Reports errors and service status
        """
        retry_count = 0

        try:
            while self._monitor_running.is_set():
                try:
                    async with self._symbol_lock:
                        for exchange in self.exchange_registry.get_registered():
                            adapter = self.exchange_registry.get_adapter(exchange)
                            try:
                                symbols = await adapter.get_symbols()
                                retry_count = 0  # Reset on success

                                # Process new symbols
                                for symbol in symbols:
                                    if symbol not in self._active_symbols:
                                        checked_symbol = symbol.check_retention_time(self._retention_days)
                                        self._active_symbols.add(checked_symbol)

                                        # Publish symbol added event
                                        await self.message_broker.publish(
                                            MessageType.SYMBOL_ADDED,
                                            SymbolMessage(
                                                service="market_data",
                                                type=MessageType.SYMBOL_ADDED,
                                                timestamp=TimeUtils.get_current_timestamp(),
                                                symbol=checked_symbol.name,
                                                exchange=checked_symbol.exchange,
                                                base_asset=checked_symbol.base_asset,
                                                quote_asset=checked_symbol.quote_asset,
                                                first_trade_time=checked_symbol.launch_time
                                            ).dict()
                                        )

                                        # Start collection
                                        await self.data_collector.start_collection(
                                            symbol=checked_symbol,
                                            start_time=checked_symbol.launch_time,
                                            end_time=TimeUtils.get_current_timestamp(),
                                            context={"type": "initial"}
                                        )

                                # Handle delisted symbols
                                delisted = self._active_symbols - set(symbols)
                                for symbol in delisted:
                                    self._active_symbols.remove(symbol)

                                    # Publish symbol delisted event
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
                                        ).dict()
                                    )

                                    # Handle delisting in collector
                                    await self.data_collector.handle_delisted(symbol)

                            except Exception as e:
                                # Track error
                                await self._error_tracker.record_error(e, exchange)

                                # Check if should retry
                                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)

                                if should_retry:
                                    retry_count += 1
                                    delay = self._retry_strategy.get_delay(retry_count)
                                    logger.warning(
                                        f"Exchange {exchange} error ({reason}), "
                                        f"retry {retry_count} after {delay:.2f}s: {e}"
                                    )
                                    await asyncio.sleep(delay)
                                else:
                                    # Publish error event
                                    await self.message_broker.publish(
                                        MessageType.ERROR_REPORTED,
                                        ErrorMessage(
                                            service="market_data",
                                            type=MessageType.ERROR_REPORTED,
                                            timestamp=TimeUtils.get_current_timestamp(),
                                            error_type="ExchangeError",
                                            severity="error",
                                            message=str(e),
                                            context={
                                                "exchange": exchange,
                                                "retry_count": retry_count,
                                                "reason": reason
                                            }
                                        ).dict()
                                    )

                    await asyncio.sleep(self._symbol_check_interval)

                except Exception as e:
                    logger.error(f"Error in symbol monitoring cycle: {e}")
                    raise

        except asyncio.CancelledError:
            logger.info("Symbol monitoring cancelled")
            raise

        except Exception as e:
            logger.error(f"Critical error in symbol monitoring: {e}")
            self._status = ServiceStatus.ERROR
            self._last_error = e

            # Publish critical error
            await self.message_broker.publish(
                MessageType.ERROR_REPORTED,
                ErrorMessage(
                    service="market_data",
                    type=MessageType.ERROR_REPORTED,
                    timestamp=TimeUtils.get_current_timestamp(),
                    error_type="CriticalError",
                    severity="critical",
                    message=str(e),
                    context={
                        "component": "monitor",
                        "retry_count": retry_count,
                        "active_symbols": len(self._active_symbols)
                    }
                ).dict()
            )

    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status_lines = [
            "Market Data Service Status:",
            f"Service State: {self._status.value}",
            f"Active Symbols: {len(self._active_symbols)}",
            "",
            "Collection Status:",
            self.data_collector.get_collection_status(),
            "",
            "Synchronization Status:",
            self.batch_synchronizer.get_sync_status()
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
