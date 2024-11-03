# src/services/market_data.py

import asyncio
from typing import Optional

from src.adapters.registry import ExchangeAdapterRegistry
from src.config import MarketDataConfig
from src.core.symbol_state import SymbolState, SymbolStateManager
from src.repositories.kline import KlineRepository
from src.repositories.symbol import SymbolRepository
from src.services.base import ServiceBase
from src.services.data_sync import BatchSynchronizer, HistoricalCollector
from src.utils.time import get_current_timestamp
from ..utils.domain_types import CriticalCondition, ServiceStatus, Timeframe, Timestamp
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MarketDataService(ServiceBase):
    """Core market data service with historical and batch synchronization"""

    def __init__(self,
                 symbol_repository: SymbolRepository,
                 kline_repository: KlineRepository,
                 exchange_registry: ExchangeAdapterRegistry,
                 config: MarketDataConfig):
        super().__init__(config)

        self.symbol_repository = symbol_repository
        self.kline_repository = kline_repository
        self.exchange_registry = exchange_registry
        self.config = config

        self.state_manager = SymbolStateManager()
        self.historical_collector = HistoricalCollector(
            exchange_registry,
            symbol_repository,
            kline_repository,
            self.state_manager
        )
        self.batch_synchronizer = BatchSynchronizer(
            self.state_manager,
            exchange_registry,
            kline_repository,
            sync_interval=config.sync_interval
        )

        # Service state
        self._status = ServiceStatus.STOPPED
        self._last_error: Optional[Exception] = None
        self._symbol_check_interval = 3600  # 1 hour

        # Add synchronization primitives
        self._symbol_lock = asyncio.Lock()              # Protect symbol state changes
        self._processing_symbols = set()                # Track symbols being processed

    async def start(self) -> None:
        """Start market data service"""
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting market data service")

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

    async def _monitor_symbols(self) -> None:
        """Monitor for new and delisted symbols"""
        while self._status == ServiceStatus.RUNNING:
            try:
                for exchange in self.exchange_registry.get_registered():
                    adapter = self.exchange_registry.get_adapter(exchange)
                    symbols = await adapter.get_symbols()

                    # Process potential new symbols
                    for symbol in symbols:
                        progress = await self.state_manager.get_progress(symbol)
                        if not progress:
                            await self.historical_collector.add_symbol(symbol)

                    # Handle delisted symbols
                    tracked_symbols = set(await self.state_manager.get_symbols_by_state(SymbolState.READY))
                    current_symbols = set(symbols)

                    for delisted in tracked_symbols - current_symbols:
                        await self.state_manager.update_state(
                            delisted,
                            SymbolState.DELISTED
                        )

                await asyncio.sleep(self._symbol_check_interval)

            # TODO: handle HTTPSConnectionPool(host='api.bybit.com', port=443): Max retries exceeded with url: /v5/market/instruments-info?category=linear (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7fd257ea2510>: Failed to establish a new connection: [Errno 111] Connection refused'))
            except Exception as e:
                logger.error(f"Error monitoring symbols: {e}")
                await asyncio.sleep(60)

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

    # TODO: make this more useful
    # what is 'connection_overflow' and 'connection_timeout'?
    # where do we check if the storage is full?
    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical system conditions"""
        logger.warning(f"Critical condition detected: {condition}")

        if condition["type"] == "connection_overflow":
            new_limit = max(
                10,  # minimum concurrent updates
                int(self.batch_synchronizer.max_concurrent_updates * 0.90)  # reduce by 10%
            )
            await self.batch_synchronizer.set_max_concurrent_updates(new_limit)
            logger.info(f"Reduced concurrent updates limit to {new_limit}")

        elif condition["type"] == "connection_timeout":
            # Increase delays between operations
            self.batch_synchronizer.sync_interval = min(
                900,  # max 15 minutes
                int(self.batch_synchronizer.sync_interval * 1.5)
            )
            logger.info(f"Increased sync interval to {self.batch_synchronizer.sync_interval}")

        elif condition["type"] == "storage_full":
            # Trigger cleanup of old data
            await self._cleanup_old_data()

        await super().handle_critical_condition(condition)

    async def _cleanup_old_data(self) -> None:
        """Clean up old kline data to free storage"""
        try:
            cutoff_time = get_current_timestamp() - (90 * 24 * 60 * 60 * 1000)  # 90 days
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