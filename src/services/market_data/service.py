# src/services/market_data/service.py

import asyncio
from datetime import datetime, timezone
from typing import Optional

from .symbol_state import SymbolState, SymbolStateManager
from .collector import HistoricalCollector
from .synchronizer import BatchSynchronizer

from ...adapters.registry import ExchangeAdapterRegistry
from ...config import MarketDataConfig
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ...core.exceptions import ServiceError
from ...repositories.kline import KlineRepository
from ...repositories.symbol import SymbolRepository
from ...services.base import ServiceBase
from ...utils.logger import LoggerSetup
from ...utils.domain_types import CriticalCondition, ServiceStatus, Timeframe, Timestamp
from ...utils.time import get_current_timestamp


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

        # Core components
        self.state_manager = SymbolStateManager()
        self.historical_collector = HistoricalCollector(
            exchange_registry,
            symbol_repository,
            kline_repository,
            self.state_manager,
            self.coordinator
        )
        self.batch_synchronizer = BatchSynchronizer(
            self.state_manager,
            exchange_registry,
            kline_repository,
            self.coordinator
        )

        # Service state
        self._status = ServiceStatus.STOPPED
        self._last_error: Optional[Exception] = None
        self._symbol_check_interval = 3600  # 1 hour
        self._symbol_lock = asyncio.Lock()              # Protect symbol state changes
        # self._processing_symbols = set()                # Track symbols being processed

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
            self._status = ServiceStatus.RUNNING
            logger.info("Market data service started successfully")
            await self._monitor_symbols()

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

            # Stop batch synchronizer first to prevent new updates
            logger.info("Stopping batch synchronizer...")
            await self.batch_synchronizer.stop()

            # Stop historical collector next to prevent new symbol processing
            logger.info("Stopping historical collector...")
            await self.historical_collector.stop()

            # Stop all exchange connections last
            logger.info("Closing exchange connections...")
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
            # Service Control
            MarketDataCommand.CLEANUP_OLD_DATA: self._handle_cleanup_command,
            MarketDataCommand.ADJUST_BATCH_SIZE: self._handle_batch_size_command,
            MarketDataCommand.UPDATE_SYMBOL_STATE: self._handle_symbol_state_command,
            MarketDataCommand.HANDLE_ERROR: self._handle_error_command,

            # Sync management
            MarketDataCommand.SYNC_ERROR: self._handle_sync_error,
            MarketDataCommand.ADJUST_SYNC_SCHEDULE: self._handle_adjust_schedule,

            # Collection events
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.COLLECTION_ERROR: self._handle_collection_error,

            # Resource management
            MarketDataCommand.REDUCE_RESOURCE_USAGE: self._handle_resource_reduction,
            MarketDataCommand.OPTIMIZE_STORAGE: self._handle_storage_optimization
        }

        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")


    async def _handle_collection_complete(self, command: Command) -> None:
        """Handle completion of historical data collection"""
        try:
            symbol_name = command.params.get('symbol')
            if not symbol_name:
                return

            # Find symbol in historical state
            historical_symbols = await self.state_manager.get_symbols_by_state(SymbolState.HISTORICAL)
            for symbol in historical_symbols:
                if symbol.name == symbol_name:
                    # Schedule for synchronized updates
                    await self.batch_synchronizer.schedule_symbol(
                        symbol,
                        Timeframe.MINUTE_5
                    )

                    # Update state
                    await self.state_manager.update_state(
                        symbol,
                        SymbolState.READY
                    )

                    logger.info(f"Symbol {symbol_name} ready for synchronized updates")
                    break

        except Exception as e:
            logger.error(f"Error handling collection complete for {symbol_name}: {e}")
            raise

    async def _handle_collection_error(self, command: Command) -> None:
        """Handle historical collection errors"""
        symbol_name = command.params.get('symbol')
        error = command.params.get('error')
        if symbol_name and error:
            historical_symbols = await self.state_manager.get_symbols_by_state(SymbolState.HISTORICAL)
            for symbol in historical_symbols:
                if symbol.name == symbol_name:
                    await self.state_manager.update_state(
                        symbol,
                        SymbolState.ERROR,
                        error
                    )
                    break

    async def _handle_resource_reduction(self, command: Command) -> None:
        """Handle resource usage reduction requests"""
        reduction_type = command.params.get('type')
        if reduction_type == 'memory':
            # Reduce batch sizes
            new_size = int(self.batch_synchronizer.max_concurrent_updates * 0.75)
            await self.batch_synchronizer.set_max_concurrent_updates(new_size)
        elif reduction_type == 'storage':
            # Trigger aggressive cleanup
            await self._cleanup_old_data(days=30)

    async def _handle_storage_optimization(self, command: Command) -> None:
        """Handle storage optimization requests"""
        strategy = command.params.get('strategy', 'aggressive')
        if strategy == 'aggressive':
            # Cleanup old data and reduce batch sizes
            await self._cleanup_old_data(days=30)
            new_size = int(self.batch_synchronizer.max_concurrent_updates * 0.5)
            await self.batch_synchronizer.set_max_concurrent_updates(new_size)
        else:
            # Normal cleanup
            await self._cleanup_old_data(days=90)


    async def _handle_sync_error(self, command: Command) -> None:
        """Handle synchronization errors"""
        symbol_name = command.params.get('symbol')
        error = command.params.get('error')
        error_count = command.params.get('error_count', 0)

        if error_count >= 3:  # Threshold for state change
            # Find the symbol in our tracked symbols
            ready_symbols = await self.state_manager.get_symbols_by_state(SymbolState.READY)
            for symbol in ready_symbols:
                if symbol.name == symbol_name:
                    await self.state_manager.update_state(
                        symbol,
                        SymbolState.ERROR,
                        error
                    )
                    logger.error(f"Symbol {symbol_name} moved to ERROR state after {error_count} sync failures")
                    break

    async def _handle_adjust_schedule(self, command: Command) -> None:
        """Handle schedule adjustment requests"""
        symbol_name = command.params.get('symbol')
        new_timeframe = command.params.get('timeframe')

        if symbol_name and new_timeframe:
            ready_symbols = await self.state_manager.get_symbols_by_state(SymbolState.READY)
            for symbol in ready_symbols:
                if symbol.name == symbol_name:
                    await self.batch_synchronizer.schedule_symbol(
                        symbol,
                        Timeframe(new_timeframe)
                    )
                    break

    async def _handle_cleanup_command(self, command: Command) -> None:
        """Handle cleanup command"""
        days = command.params.get('days', 90)
        await self._cleanup_old_data(days)

    async def _handle_batch_size_command(self, command: Command) -> None:
        """Handle batch size adjustment command"""
        new_size = command.params.get('size')
        if new_size:
            await self.batch_synchronizer.set_max_concurrent_updates(new_size)

    async def _handle_symbol_state_command(self, command: Command) -> None:
        """Handle symbol state update command"""
        symbol = command.params.get('symbol')
        new_state = command.params.get('state')
        if symbol and new_state:
            await self.state_manager.update_state(symbol, new_state)

    async def _handle_error_command(self, command: Command) -> None:
        """Handle error recovery command"""
        error = command.params.get('error')
        if error:
            await self.handle_error(error)

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """
        Handle critical system conditions through command system.

        Implements different strategies based on condition type:
        - connection_overflow: Reduces concurrent operations
        - connection_timeout: Implements backoff strategy
        - storage_full: Triggers storage optimization
        - memory_high: Reduces resource usage
        """
        logger.warning(f"Critical condition detected: {condition}")

        try:
            if condition["type"] == "connection_overflow":
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.REDUCE_RESOURCE_USAGE,
                    params={"type": "connections"},
                    priority=1
                ))

            elif condition["type"] == "connection_timeout":
                # Implement backoff strategy
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.REDUCE_RESOURCE_USAGE,
                    params={
                        "type": "memory",
                        "strategy": "aggressive"
                    },
                    priority=1
                ))

            elif condition["type"] == "storage_full":
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.OPTIMIZE_STORAGE,
                    params={"strategy": "aggressive"},
                    priority=1
                ))

            elif condition["type"] == "memory_high":
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.REDUCE_RESOURCE_USAGE,
                    params={"type": "memory"},
                    priority=1
                ))

        except Exception as e:
            logger.error(f"Error handling critical condition: {e}")
            raise

    async def _monitor_symbols(self) -> None:
        """
        Monitor available trading symbols and manage their lifecycle.

        - Discovers new symbols and initiates historical data collection
        - Handles delisted symbols
        - Maintains symbol states
        """
        while self._status == ServiceStatus.RUNNING:
            try:
                async with self._symbol_lock:
                    for exchange in self.exchange_registry.get_registered():
                        adapter = self.exchange_registry.get_adapter(exchange)
                        symbols = await adapter.get_symbols()

                        # Process new symbols
                        for symbol in symbols:
                            progress = await self.state_manager.get_progress(symbol)
                            if not progress:
                                await self.historical_collector.add_symbol(symbol)

                        # Handle delisted symbols
                        tracked_symbols = set(
                            await self.state_manager.get_symbols_by_state(SymbolState.READY)
                        )
                        current_symbols = set(symbols)

                        for delisted in tracked_symbols - current_symbols:
                            await self.state_manager.update_state(
                                delisted,
                                SymbolState.DELISTED
                            )

                            # Clean up schedules
                            if hasattr(self.batch_synchronizer, '_schedules'):
                                self.batch_synchronizer._schedules.pop(delisted.name, None)
                                logger.info(
                                    f"Removed delisted symbol {delisted.name} "
                                    f"from sync schedule"
                                )

                await asyncio.sleep(self._symbol_check_interval)

            # TODO: handle HTTPSConnectionPool(host='api.bybit.com', port=443): Max retries exceeded with url: /v5/market/instruments-info?category=linear (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fd257ea2510>: Failed to establish a new connection: [Errno 111] Connection refused'))
            except Exception as e:
                logger.error(f"Error monitoring symbols: {e}")
                await asyncio.sleep(60)

    @property
    def sync_status(self) -> str:
        """Get synchronization status summary"""
        if not hasattr(self.batch_synchronizer, '_schedules'):
            return "No sync schedules available"

        status_lines = ["Synchronization Status:"]

        schedules = self.batch_synchronizer._schedules
        active_syncs = len(self.batch_synchronizer._processing)

        status_lines.append(f"Active Syncs: {active_syncs}")
        status_lines.append(f"Total Scheduled Symbols: {len(schedules)}")

        if schedules:
            status_lines.append("\nNext Sync Times:")
            current_time = datetime.now(timezone.utc)

            for symbol_name, schedule in sorted(
                schedules.items(),
                key=lambda x: x[1].next_sync
            )[:5]:  # Show next 5 syncs
                time_until = (schedule.next_sync - current_time).total_seconds()
                status_lines.append(
                    f"  {symbol_name}: {schedule.next_sync.strftime('%H:%M:%S')} "
                    f"({time_until:.1f}s)"
                )

        return "\n".join(status_lines)

    async def handle_error(self, error: Optional[Exception]) -> None:
        """
        Handle service errors and attempt recovery.
        Implements ServiceBase.handle_error.
        """
        if error is None:
            return

        try:
            logger.error(f"Handling service error: {error}")

            # Stop components
            logger.info("Stopping components for recovery...")
            await self.historical_collector.stop()
            await self.batch_synchronizer.stop()

            # Wait a bit before recovery attempt
            await asyncio.sleep(self.config.retry_interval)

            # Attempt to restart components
            logger.info("Attempting service recovery...")

            # Verify exchange connections
            await self.exchange_registry.initialize_all()

            # Restart components
            await self.historical_collector.start()
            await self.batch_synchronizer.start()

            # Update service status
            self._status = ServiceStatus.RUNNING
            self._last_error = None

            # Resume symbol monitoring
            asyncio.create_task(self._monitor_symbols())

            logger.info("Service recovered successfully")

        except Exception as recovery_error:
            self._status = ServiceStatus.ERROR
            self._last_error = recovery_error
            logger.error(f"Service recovery failed: {recovery_error}")
            raise ServiceError(
                f"Failed to recover from error: {str(recovery_error)}"
            )

    async def _cleanup_old_data(self, days: int = 90) -> None:
        """Clean up old kline data to free storage"""
        try:
            cutoff_time = get_current_timestamp() - (days * 24 * 60 * 60 * 1000)  # 90 days
            ready_symbols = await self.state_manager.get_symbols_by_state(SymbolState.READY)

            for symbol in ready_symbols:
                deleted = await self.kline_repository.delete_old_data(
                    symbol,
                    Timeframe.MINUTE_5,
                    Timestamp(cutoff_time)
                )
                if deleted:
                    logger.info(f"Cleaned up {deleted} old records for {symbol}")

        except Exception as e:
            logger.error(f"Error during data cleanup: {e}")

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy"""
        return (
            self._status == ServiceStatus.RUNNING and
            not self._last_error and
            self.historical_collector._active and
            self.batch_synchronizer._active
        )