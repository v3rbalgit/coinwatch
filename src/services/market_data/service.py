# src/services/market_data/service.py

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from .collector import HistoricalCollector
from .synchronizer import BatchSynchronizer

from ...adapters.registry import ExchangeAdapterRegistry
from ...config import MarketDataConfig
from ...core.models import SymbolInfo
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.exceptions import ServiceError, ValidationError
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...services.base import ServiceBase
from ...services.market_data.progress import CollectionProgress, SyncSchedule
from ...utils.logger import LoggerSetup
from ...utils.domain_types import CriticalCondition, ServiceStatus, Timeframe, Timestamp
from ...utils.time import get_current_timestamp
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
                 config: MarketDataConfig,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5):
        super().__init__(config)

        self.coordinator = coordinator
        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry
        self.config = config
        self.base_timeframe = base_timeframe

        # Core components
        self.historical_collector = HistoricalCollector(
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
        self._collection_progress: Dict[str, CollectionProgress] = {}
        self._sync_schedules: Dict[str, SyncSchedule] = {}
        self._active_symbols: Dict[str, SymbolInfo] = {}

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
            base_delay=1.0,
            max_delay=300.0,
            max_retries=3
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

            # Register command handlers
            await self._register_command_handlers()

            # Initialize exchange adapters
            await self.exchange_registry.initialize_all()

            # Start components
            await self.historical_collector.start()
            await self.batch_synchronizer.start()

            # Start symbol monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(
                self._monitor_symbols()
            )

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

            # Cancel the task - it's okay since we're shutting down
            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
                self._monitor_task = None

            # Stop components in order
            await self.batch_synchronizer.stop()
            await self.historical_collector.stop()
            await self.exchange_registry.close_all()

            self._status = ServiceStatus.STOPPED
            logger.info("Market data service stopped successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Error during service shutdown: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")

    async def _register_command_handlers(self) -> None:
        """Register command handlers for all supported operations"""
        handlers = {
            # Collection events
            MarketDataCommand.COLLECTION_STARTED: self._handle_collection_start,
            MarketDataCommand.COLLECTION_PROGRESS: self._handle_collection_progress,
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.COLLECTION_ERROR: self._handle_collection_error,

            # Sync events
            MarketDataCommand.SYNC_SCHEDULED: self._handle_sync_scheduled,
            MarketDataCommand.SYNC_COMPLETED: self._handle_sync_complete,
            MarketDataCommand.SYNC_ERROR: self._handle_sync_error,

            # Resource management
            MarketDataCommand.ADJUST_BATCH_SIZE: self._handle_batch_size_command,
        }

        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _handle_collection_start(self, command: Command) -> None:
        """Handle start of historical collection with enhanced context"""
        symbol_name = command.params['symbol']

        async with self._symbol_lock:
            symbol = self._active_symbols[symbol_name]
            # Initialize collection progress atomically
            self._collection_progress[symbol_name] = CollectionProgress(
                symbol=symbol,
                start_time=datetime.fromtimestamp(
                    command.params['start_time'] / 1000,
                    timezone.utc
                )
            )

            # Log collection parameters while holding state
            context = command.params.get('context', {})
            logger.info(
                f"Started historical collection for {symbol_name} "
                f"with batch size {context.get('batch_size', 'default')}"
            )

    async def _handle_collection_progress(self, command: Command) -> None:
        """Handle collection progress updates with enhanced tracking"""
        symbol_name = command.params['symbol']

        async with self._symbol_lock:
            if progress := self._collection_progress.get(symbol_name):
                progress.update(
                    processed=command.params['processed'],
                    total=command.params.get('total')
                )

                # Get parameters for logging
                context = command.params.get('context', {})
                timeframe = context.get('timeframe')
                batch_size = context.get('batch_size')
                last_timestamp = command.params.get('last_timestamp')

                if last_timestamp:
                    last_time = datetime.fromtimestamp(last_timestamp / 1000, timezone.utc)
                    logger.debug(
                        f"Collection progress for {symbol_name}: "
                        f"{progress} at {last_time} "
                        f"[{timeframe} timeframe, batch size {batch_size}]"
                    )
                else:
                    logger.debug(f"Collection progress for {symbol_name}: {progress}")

    async def _handle_collection_complete(self, command: Command) -> None:
        """Handle successful completion of historical collection"""
        symbol_name = command.params['symbol']

        async with self._symbol_lock:
            if symbol := self._active_symbols.get(symbol_name):
                # Get collection statistics
                start_time = datetime.fromtimestamp(
                    command.params['start_time'] / 1000,
                    timezone.utc
                )
                end_time = datetime.fromtimestamp(
                    command.params['end_time'] / 1000,
                    timezone.utc
                )
                duration = (end_time - start_time).total_seconds()

                logger.info(
                    f"Completed historical collection for {symbol_name} "
                    f"({duration:.1f}s elapsed)"
                )

                # Atomic state transition
                self._collection_progress.pop(symbol_name, None)

                # Prepare sync schedule
                next_sync = self.batch_synchronizer.calculate_next_sync(
                    timeframe=self.base_timeframe
                )

                self._sync_schedules[symbol_name] = SyncSchedule(
                    symbol=symbol,
                    timeframe=self.base_timeframe,
                    next_sync=next_sync
                )

                # Cache values needed after lock release
                transition_symbol = symbol
                transition_timeframe = self.base_timeframe

        # Start synchronization after releasing lock
        if transition_symbol:
            await self.batch_synchronizer.schedule_symbol(
                transition_symbol,
                transition_timeframe
            )
            logger.info(
                f"Transitioned {symbol_name} to synchronized updates "
                f"starting at {next_sync}"
            )

    async def _handle_collection_error(self, command: Command) -> None:
        """Handle collection errors with enhanced error tracking and recovery"""
        symbol_name = command.params['symbol']
        error = command.params['error']
        error_type = command.params['error_type']
        context = command.params.get('context', {})

        # Track error with full context
        await self._error_tracker.record_error(
            Exception(error),
            symbol_name,
            **context
        )

        # Get process-specific information
        process = context.get('process', 'unknown')
        retry_count = context.get('retry_count', 0)
        timestamp = context.get('timestamp')

        if timestamp:
            error_time = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
            logger.error(
                f"Collection error in {process} for {symbol_name} at {error_time}: "
                f"{error_type} (retry {retry_count})"
            )

        # Check error frequency for adaptive handling
        frequency = await self._error_tracker.get_error_frequency(
            error_type,
            window_minutes=60
        )

        if frequency > 5:  # High error frequency
            if process == 'historical_collection':
                # Reduce collection batch size
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_BATCH_SIZE,
                    params={"size": int(self.historical_collector._batch_size * 0.75)},
                    priority=1
                ))
                logger.warning(
                    f"High error frequency ({frequency}/hour) for {symbol_name}, "
                    f"reducing collection batch size"
                )

        # If errors persist across retries, might need to pause collection
        if retry_count >= 3:
            logger.error(
                f"Multiple retry failures for {symbol_name}, "
                f"considering collection pause"
            )
            # Could implement pause mechanism here

    async def _handle_sync_scheduled(self, command: Command) -> None:
        """Handle sync schedule updates"""
        symbol_name = command.params['symbol']
        next_sync = datetime.fromtimestamp(command.params['next_sync'], timezone.utc)

        async with self._symbol_lock:
            if schedule := self._sync_schedules.get(symbol_name):
                schedule.next_sync = next_sync
                logger.debug(f"Updated sync schedule for {symbol_name}: next at {next_sync}")

    async def _handle_sync_complete(self, command: Command) -> None:
        """Handle successful sync completion with progress tracking"""
        symbol_name = command.params['symbol']
        context = command.params['context']

        async with self._symbol_lock:
            if schedule := self._sync_schedules.get(symbol_name):
                schedule.update(datetime.fromtimestamp(
                    command.params['sync_time'],
                    timezone.utc
                ))

                if next_sync := context.get('next_sync'):
                    schedule.next_sync = datetime.fromtimestamp(
                        next_sync,
                        timezone.utc
                    )

                processed = command.params.get('processed', 0)
                logger.debug(
                    f"Completed sync for {symbol_name}: processed {processed} candles, "
                    f"next sync at {schedule.next_sync}"
                )

                # Monitor resource usage
                resource_usage = context.get('resource_usage', {})
                if (resource_usage.get('concurrent_syncs', 0) >
                    resource_usage.get('max_allowed', 0) * 0.9):
                    logger.warning("High resource usage detected in sync operations")

    async def _handle_sync_error(self, command: Command) -> None:
        """Handle sync errors with enhanced context"""
        symbol_name = command.params['symbol']
        error = command.params['error']
        context = command.params['context']

        # Record error with full context
        await self._error_tracker.record_error(
            Exception(error),
            symbol_name,
            **context
        )

        # Check if this is a resource issue
        if context.get('is_resource_error'):
            # Get current resource usage
            active_syncs = context.get('active_syncs', 0)
            concurrent_limit = context.get('concurrent_limit', 0)
            usage_ratio = active_syncs / concurrent_limit if concurrent_limit else 1

            if usage_ratio > 0.8:  # High resource usage
                # Reduce concurrent updates gradually
                new_limit = max(10, int(concurrent_limit * 0.75))
                await self.batch_synchronizer.set_max_concurrent_updates(new_limit)
                logger.warning(
                    f"High resource usage ({usage_ratio:.1%}), "
                    f"reducing concurrent syncs to {new_limit}"
                )

        # Check error frequency for this symbol
        error_frequency = context.get('error_frequency', 0)
        if error_frequency > 5:  # High error rate for this symbol
            # Check if we should temporarily pause syncing
            if context.get('retry_count', 0) >= 3:
                logger.error(
                    f"High error frequency for {symbol_name} "
                    f"({error_frequency}/hour) with multiple retries, "
                    f"pausing sync"
                )
                # Could implement pause mechanism here

    async def _handle_batch_size_command(self, command: Command) -> None:
        """Handle batch size adjustment command"""
        new_size = command.params.get('size')

        if new_size:
            await self.batch_synchronizer.set_max_concurrent_updates(new_size)

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """
        Handle critical system conditions through command system.
        """
        pass

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
                                # Success - reset retry count
                                retry_count = 0

                                # Process new symbols
                                for symbol in symbols:
                                    if symbol.name not in self._active_symbols:
                                        self._active_symbols[symbol.name] = symbol
                                        await self.historical_collector.add_symbol(symbol)

                                # Handle delisted symbols
                                current_symbols = {s.name for s in symbols}
                                delisted = set(self._active_symbols.keys()) - current_symbols

                                for symbol_name in delisted:
                                    if symbol := self._active_symbols.pop(symbol_name, None):
                                        # Remove from tracking
                                        self._collection_progress.pop(symbol_name, None)
                                        self._sync_schedules.pop(symbol_name, None)

                                        # Notify about delisting
                                        await self.coordinator.execute(Command(
                                            type=MarketDataCommand.SYMBOL_DELISTED,
                                            params={"symbol": symbol_name}
                                        ))

                            except Exception as e:
                                await self._error_tracker.record_error(e, exchange)
                                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)

                                if should_retry:
                                    retry_count += 1
                                    delay = self._retry_strategy.get_delay(retry_count)
                                    logger.warning(
                                        f"Exchange {exchange} error ({reason}), "
                                        f"retry {retry_count} after {delay}s: {e}"
                                    )
                                    await asyncio.sleep(delay)
                                    continue
                                else:
                                    logger.error(
                                        f"Exchange {exchange} failed: {reason}, {e}"
                                    )
                                    await self.coordinator.execute(Command(
                                        type=MarketDataCommand.EXCHANGE_ERROR,
                                        params={
                                            "exchange": exchange,
                                            "error": str(e),
                                            "reason": reason
                                        },
                                        priority=1
                                    ))

                    await asyncio.sleep(self._symbol_check_interval)

                except Exception as e:
                    await self._error_tracker.record_error(e)
                    frequency = await self._error_tracker.get_error_frequency(
                        e.__class__.__name__,
                        window_minutes=60
                    )

                    if frequency > 10:  # More than 10 errors per hour
                        await self.handle_critical_condition({
                            "type": "service_error",
                            "message": f"High error frequency: {frequency}/hour",
                            "severity": "error",
                            "timestamp": get_current_timestamp()
                        })

                    await asyncio.sleep(60)
        # TODO: handle HTTPSConnectionPool(host='api.bybit.com', port=443): Max retries exceeded with url: /v5/market/instruments-info?category=linear (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fd257ea2510>: Failed to establish a new connection: [Errno 111] Connection refused'))
        except asyncio.CancelledError:
            logger.info("Symbol monitoring cancelled")
            raise
        except Exception as e:
            logger.error(f"Critical error in symbol monitoring: {e}")
            self._status = ServiceStatus.ERROR
            self._last_error = e
            await self._error_tracker.record_error(e)
            await self.coordinator.execute(Command(
                type=MarketDataCommand.HANDLE_ERROR,
                params={
                    "error": str(e),
                    "frequency": self._error_tracker.get_error_frequency(
                        e.__class__.__name__
                    )
                },
                priority=1
            ))

    async def handle_error(self, error: Optional[Exception]) -> None:
        """Handle service errors with tracking and recovery"""
        if error is None:
            return

        try:
            logger.error(f"Handling service error: {error}")
            await self._error_tracker.record_error(error)

            frequency = await self._error_tracker.get_error_frequency(
                error.__class__.__name__,
                window_minutes=60
            )

            # Adjust recovery strategy based on error frequency
            if frequency > 10:  # High error rate
                logger.warning(f"High error frequency ({frequency}/hour), extending retry interval")
                retry_interval = self.config.retry_interval * 2
            else:
                retry_interval = self.config.retry_interval

            # Stop components
            self._monitor_running.clear()
            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
                self._monitor_task = None

            await self.historical_collector.stop()
            await self.batch_synchronizer.stop()

            # Wait before recovery
            await asyncio.sleep(retry_interval)

            # Attempt restart
            await self.exchange_registry.initialize_all()
            await self.historical_collector.start()
            await self.batch_synchronizer.start()

            # Resume monitoring
            self._monitor_running.set()
            self._monitor_task = asyncio.create_task(self._monitor_symbols())

            self._status = ServiceStatus.RUNNING
            self._last_error = None

            logger.info("Service recovered successfully")

        except Exception as recovery_error:
            await self._error_tracker.record_error(recovery_error, context="recovery")
            self._status = ServiceStatus.ERROR
            self._last_error = recovery_error
            logger.error(f"Service recovery failed: {recovery_error}")
            raise ServiceError(f"Failed to recover from error: {str(recovery_error)}")

    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status = [
            "Market Data Service Status:",
            f"Service State: {self._status.value}",
            f"Active Symbols: {len(self._active_symbols)}",
            "",
            "Collection Status:",
            self.historical_collector.get_collection_status(),
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

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy"""
        return (
            self._status == ServiceStatus.RUNNING and
            not self._last_error and
            self.historical_collector._active and
            self.batch_synchronizer._active
        )