# src/services/market_data/service.py

import asyncio
from typing import Optional, Set

from .collector import DataCollector
from .synchronizer import BatchSynchronizer

from ...adapters.registry import ExchangeAdapterRegistry
from ...config import MarketDataConfig
from ...core.models import SymbolInfo
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.exceptions import ServiceError, ValidationError
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...services.base import ServiceBase
from ...utils.logger import LoggerSetup
from ...utils.domain_types import CriticalCondition, ServiceStatus, Timeframe
from ...utils.time import TimeUtils
from ...utils.error import ErrorTracker
from ...utils.retry import RetryConfig, RetryStrategy


logger = LoggerSetup.setup(__name__)

class MarketDataService(ServiceBase):
    """
    Core market data service managing historical and real-time price data collection.

    Responsibilities:
    - Monitors available trading symbols
    - Manages historical data collection
    - Coordinates real-time data synchronization
    - Handles resource optimization
    - Provides system health monitoring

    The service uses a command-based architecture for inter-component communication
    and maintains symbol states through a centralized state manager.
    """
    def __init__(self,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 exchange_registry: ExchangeAdapterRegistry,
                 coordinator: ServiceCoordinator,
                 config: MarketDataConfig):
        super().__init__(config)

        self.coordinator = coordinator
        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry
        self.config = config
        self.base_timeframe = Timeframe(config.default_timeframe)

        # Core components
        self.data_collector = DataCollector(
            exchange_registry,
            symbol_repository,
            kline_repository,
            self.coordinator,
            self.base_timeframe
        )
        self.batch_synchronizer = BatchSynchronizer(
            exchange_registry,
            kline_repository,
            self.coordinator,
            self.base_timeframe
        )

        # Progress tracking
        self._active_symbols: Set[SymbolInfo] = set()

        # Service state
        self._status = ServiceStatus.STOPPED
        self._last_error: Optional[Exception] = None
        self._symbol_check_interval = 3600  # 1 hour
        self._symbol_lock = asyncio.Lock()  # Protect symbol state changes

        # Task management
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_running = asyncio.Event()

        # Initialize error tracking and retry strategy
        self._error_tracker = ErrorTracker()
        retry_config = RetryConfig(
            base_delay=5.0,
            max_delay=300.0,
            max_retries=3,
            jitter_factor=0.25
        )
        self._retry_strategy = RetryStrategy(retry_config)
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            ServiceError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )

    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting market data service")

            await self._register_command_handlers()

            await self.exchange_registry.initialize_all()

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
      """Stop market data service with proper cleanup"""
      try:
          self._status = ServiceStatus.STOPPING
          logger.info("Stopping market data service")

          # Stop monitoring first
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
              self.batch_synchronizer.cleanup()
          ]

          # Wait for all cleanup tasks to complete
          await asyncio.gather(*cleanup_tasks, return_exceptions=True)

          # Close exchange connections
          await self.exchange_registry.close_all()

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

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical system conditions with recovery strategies"""
        condition_type = condition["type"]
        severity = condition["severity"]

        try:
            logger.error(
                f"Handling critical condition: {condition_type} "
                f"(severity: {severity})"
            )

            if condition_type == "service_error":
                # Service-wide issues
                await self._handle_service_recovery(condition)

            elif condition_type == "collection_failure":
                # DataCollector issues
                await self._handle_collection_recovery(condition)

            elif condition_type == "sync_failure":
                # BatchSynchronizer issues
                await self._handle_sync_recovery(condition)

        except Exception as e:
            logger.error(f"Recovery failed for {condition_type}: {e}")
            # Escalate if recovery fails
            if severity == "critical":
                self._status = ServiceStatus.ERROR
                self._last_error = e
                raise ServiceError(f"Critical recovery failed: {str(e)}")

    async def _register_command_handlers(self) -> None:
        """Register command handlers for service monitoring"""
        handlers = {
            MarketDataCommand.GAP_DETECTED: self._handle_gap_detected,
            MarketDataCommand.COLLECTION_ERROR: self._handle_collection_error,
            MarketDataCommand.SYNC_ERROR: self._handle_sync_error
        }

        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _handle_gap_detected(self, command: Command) -> None:
        """Handle detected data gaps"""
        symbol = command.params["symbol"]
        gaps = command.params["gaps"]
        timeframe = command.params["timeframe"]

        # Track gap occurrence
        await self._error_tracker.record_error(
            Exception("Data gap detected"),
            symbol,
            gaps=gaps,
            timeframe=timeframe
        )

        # Check gap frequency
        frequency = await self._error_tracker.get_error_frequency(
            "Data gap detected",
            window_minutes=60
        )

        if frequency > 5:
            # Too many gaps might indicate system issues
            await self.handle_critical_condition({
                "type": "collection_failure",
                "severity": "warning",
                "message": f"High gap detection frequency: {frequency}/hour",
                "timestamp": TimeUtils.get_current_timestamp(),
                "error_type": "DataGapDetected",
                "context": {
                    "affected_symbols": [symbol],
                    "timeframe": timeframe,
                    "gap_count": len(gaps),
                    "frequency": frequency
                }
            })

        # Request collection for each gap
        for start, end in gaps:
            await self.coordinator.execute(Command(
                type=MarketDataCommand.COLLECTION_STARTED,
                params={
                    "symbol": symbol,
                    "start_time": start,
                    "end_time": end,
                    "timestamp": TimeUtils.get_current_timestamp(),
                    "context": {
                        "timeframe": timeframe,
                        "gap_size": (end - start) / (int(timeframe) * 60 * 1000)  # Convert to candle count
                    }
                }
            ))

        logger.warning(
            f"Data gaps detected for {symbol} ({timeframe}): "
            f"{len(gaps)} gaps found"
        )

    async def _handle_collection_error(self, command: Command) -> None:
        """Handle collection errors with focus on resource management"""
        symbol = command.params['symbol']
        error = command.params['error']
        error_type = command.params['error_type']
        context = command.params.get('context', {})
        timestamp = command.params.get('timestamp')

        await self._error_tracker.record_error(
            Exception(error),
            symbol,
            **context
        )

        frequency = await self._error_tracker.get_error_frequency(
            error_type,
            window_minutes=60
        )

        if frequency > 5:
            await self.handle_critical_condition({
                "type": "collection_failure",
                "severity": "error" if frequency > 10 else "warning",
                "message": f"High collection error frequency: {frequency}/hour",
                "timestamp": timestamp or TimeUtils.get_current_timestamp(),
                "error_type": error_type,
                "context": {
                    "affected_symbols": [symbol],
                    "last_successful": context.get('last_timestamp'),
                    "frequency": frequency,
                    **context  # Include original context
                }
            })
        elif command.params.get('retry_exhausted'):
            await self.handle_critical_condition({
                "type": "collection_failure",
                "severity": "warning",
                "message": f"Collection retries exhausted for {symbol}",
                "timestamp": timestamp or TimeUtils.get_current_timestamp(),
                "error_type": error_type,
                "context": {
                    "affected_symbols": [symbol],
                    "last_successful": context.get('last_timestamp'),
                    "retry_exhausted": True,
                    **context
                }
            })

    async def _handle_sync_error(self, command: Command) -> None:
        """Handle sync errors with focus on service health"""
        error = command.params['error']
        error_type = command.params['error_type']
        context = command.params.get('context', {})

        if context.get('process') == 'sync_infrastructure':
            await self.handle_critical_condition({
                "type": "sync_failure",
                "severity": "critical",
                "message": f"Sync infrastructure error: {error}",
                "timestamp": TimeUtils.get_current_timestamp(),
                "error_type": error_type,
                "context": {
                    "active_syncs": context.get('active_syncs', 0),
                    "scheduled_symbols": context.get('scheduled_symbols', 0),
                    **context
                }
            })
            return

        symbol = command.params.get('symbol')
        if not symbol:
            logger.error(f"Received sync error without symbol: {error}")
            return

        await self._error_tracker.record_error(
            Exception(error),
            symbol,
            **context
        )

        frequency = await self._error_tracker.get_error_frequency(
            error_type,
            window_minutes=60
        )

        if frequency > 5:
            await self.handle_critical_condition({
                "type": "sync_failure",
                "severity": "error" if frequency > 10 else "warning",
                "message": f"High sync error frequency: {frequency}/hour",
                "timestamp": TimeUtils.get_current_timestamp(),
                "error_type": error_type,
                "context": {
                    "affected_symbols": [symbol],
                    "last_sync": context.get('last_sync'),
                    "frequency": frequency,
                    **context
                }
            })

    async def _handle_service_recovery(self, condition: CriticalCondition) -> None:
        """Handle service-wide recovery based on condition context"""
        severity = condition["severity"]
        context = condition["context"]

        # Check if this is an exchange-specific issue
        if "exchange" in context:
            # Handle exchange-specific recovery
            exchange = context["exchange"]
            logger.warning(f"Attempting recovery for exchange {exchange}")

            try:
                # Reinitialize just this exchange
                adapter = self.exchange_registry.get_adapter(exchange)
                await adapter.initialize()
                logger.info(f"Successfully reinitialized exchange {exchange}")

            except Exception as e:
                logger.error(f"Failed to recover exchange {exchange}: {e}")
                if severity == "critical":
                    # If critical, fall through to full service recovery
                    logger.error("Critical exchange failure, attempting full service recovery")
                else:
                    return  # For non-critical exchange issues, don't do full recovery

        # Full service recovery
        logger.info("Initiating full service recovery")
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
        await self.batch_synchronizer.cleanup()

        # Adaptive retry interval based on error frequency
        retry_interval = self.config.retry_interval
        if await self._error_tracker.get_error_frequency(
            condition["error_type"],
            window_minutes=60
        ) > 10:
            retry_interval *= 2

        await asyncio.sleep(retry_interval)

        # Attempt restart
        await self.exchange_registry.initialize_all()
        self._monitor_running.set()
        self._monitor_task = asyncio.create_task(self._monitor_symbols())

    async def _handle_collection_recovery(self, condition: CriticalCondition) -> None:
        """Handle DataCollector recovery"""
        # Get affected symbols from context
        affected_symbols = condition["context"].get("affected_symbols", [])

        for symbol in affected_symbols:
            await self.coordinator.execute(Command(
                type=MarketDataCommand.COLLECTION_STARTED,
                params={
                    "symbol": symbol,
                    "start_time": condition["context"].get("last_successful"),
                    "end_time": TimeUtils.get_current_timestamp()
                }
            ))

    async def _handle_sync_recovery(self, condition: CriticalCondition) -> None:
        """Handle BatchSynchronizer recovery"""
        # Get affected symbols from context
        affected_symbols = condition["context"].get("affected_symbols", [])

        for symbol in affected_symbols:
            await self.coordinator.execute(Command(
                type=MarketDataCommand.COLLECTION_STARTED,
                params={
                    "symbol": symbol,
                    "start_time": condition["context"].get("last_sync"),
                    "end_time": TimeUtils.get_current_timestamp()
                }
            ))

    async def _monitor_symbols(self) -> None:
        """Monitor available trading symbols and manage their lifecycle"""
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
                                        self._active_symbols.add(symbol)
                                        # Start collection via command
                                        await self.coordinator.execute(Command(
                                            type=MarketDataCommand.COLLECTION_STARTED,
                                            params={
                                                "symbol": symbol,
                                                "start_time": symbol.launch_time,
                                                "end_time": TimeUtils.get_current_timestamp(),
                                                "timestamp": TimeUtils.get_current_timestamp()
                                            }
                                        ))

                                # Handle delisted symbols
                                delisted = self._active_symbols - set(symbols)

                                for symbol in delisted:
                                    self._active_symbols.remove(symbol)
                                    await self.coordinator.execute(Command(
                                        type=MarketDataCommand.SYMBOL_DELISTED,
                                        params={
                                            "symbol": symbol,
                                            "timestamp": TimeUtils.get_current_timestamp()
                                        }
                                    ))

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
                                    # Handle exchange failure as critical condition
                                    await self.handle_critical_condition({
                                        "type": "service_error",
                                        "severity": "error",
                                        "message": f"Exchange {exchange} failed: {str(e)}",
                                        "timestamp": TimeUtils.get_current_timestamp(),
                                        "error_type": e.__class__.__name__,
                                        "context": {
                                            "component": "monitor",
                                            "exchange": exchange,
                                            "retry_count": retry_count,
                                            "reason": reason,
                                            "active_symbols": len(self._active_symbols)
                                        }
                                    })

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
            await self.handle_critical_condition({
                "type": "service_error",
                "severity": "critical",
                "message": f"Critical error in symbol monitoring: {str(e)}",
                "timestamp": TimeUtils.get_current_timestamp(),
                "error_type": e.__class__.__name__,
                "context": {
                    "component": "monitor",
                    "retry_count": retry_count,
                    "active_symbols": len(self._active_symbols)
                }
            })

    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status = [
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
            status.extend([
                "",
                "Recent Errors:"
            ])
            for error_type, count in error_summary.items():
                status.append(f"  {error_type}: {count} in last hour")

        return "\n".join(status)