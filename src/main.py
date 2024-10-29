# src/main.py

import asyncio
from typing import Optional
from src.actions.symbol_actions import FetchSymbolsAction
from src.actions.data_actions import FetchHistoricalDataAction, SyncRecentDataAction
from src.actions.resource_actions import ResourceActionFactory
from src.actions.data_sync_actions import DataSyncActionFactory
from src.actions.database_actions import DatabaseActionFactory
from src.core.actions import ActionPriority
from src.core.system import System
from src.managers.database_manager import DatabaseManager
from src.api.bybit_adapter import BybitAdapter
from src.observers.resource_observer import ResourceObserver
from src.observers.data_sync_observer import DataSyncObserver
from src.observers.database_observer import DatabaseObserver
from src.config import DATABASE_URL
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class CoinwatchApp:
    """
    Main application class coordinating all components.
    Manages lifecycle and integration of all services, observers, and actions.
    """

    def __init__(self):
        # Core components
        self.system = System()
        self.db_manager = DatabaseManager(DATABASE_URL)
        self.session_factory = self.db_manager.SessionLocal
        self.bybit_adapter = BybitAdapter()

        # Initialize observers
        self.resource_observer: Optional[ResourceObserver] = None
        self.data_sync_observer: Optional[DataSyncObserver] = None
        self.database_observer: Optional[DatabaseObserver] = None

    async def setup(self):
        """Initialize and start all system components."""
        try:
            # Start core system
            await self.system.start()

            # Register actions
            self._register_actions()

            # Wait for database readiness
            await self.db_manager.wait_for_ready()

            # Setup observers
            await self._setup_observers()

            # Perform initial symbol fetch
            await self.system.action_manager.submit_action(
                "fetch_symbols",
                {
                    "session_factory": self.session_factory
                },
                priority=ActionPriority.HIGH
            )

            logger.info("Coinwatch application setup completed")

        except Exception as e:
            logger.error(f"Failed to setup application: {e}", exc_info=True)
            raise

    def _register_actions(self):
        """Register all available actions."""
        # Symbol actions
        self.system.register_action(
            "fetch_symbols",
            FetchSymbolsAction
        )

        # Data actions
        self.system.register_action(
            "fetch_historical_data",
            FetchHistoricalDataAction
        )
        self.system.register_action(
            "sync_recent_data",
            SyncRecentDataAction
        )

        # Register resource actions
        for action_name, action_class in ResourceActionFactory._actions.items():
            self.system.register_action(action_name, action_class)

        # Register data sync actions
        for action_name, action_class in DataSyncActionFactory._actions.items():
            self.system.register_action(action_name, action_class)

        # Register database actions
        for action_name, action_class in DatabaseActionFactory._actions.items():
            self.system.register_action(action_name, action_class)

    async def _setup_observers(self):
        """Initialize and start observers."""
        try:
            # Initialize observers
            self.resource_observer = ResourceObserver(
                self.system.state_manager,
                self.system.action_manager,
                observation_interval=30.0  # 30 seconds
            )

            self.data_sync_observer = DataSyncObserver(
                self.system.state_manager,
                self.system.action_manager,
                self.session_factory,
                observation_interval=300.0  # 5 minutes
            )

            self.database_observer = DatabaseObserver(
                self.system.state_manager,
                self.system.action_manager,
                self.db_manager.engine,
                self.session_factory,
                observation_interval=60.0  # 1 minute
            )

            # Start observers
            await self.resource_observer.start()
            await self.data_sync_observer.start()
            await self.database_observer.start()

            logger.info("All observers started successfully")

        except Exception as e:
            logger.error(f"Failed to setup observers: {e}", exc_info=True)
            raise

    async def stop(self):
        """Gracefully shutdown all components."""
        try:
            logger.info("Stopping Coinwatch application...")

            # Stop observers
            if self.resource_observer:
                await self.resource_observer.stop()
            if self.data_sync_observer:
                await self.data_sync_observer.stop()
            if self.database_observer:
                await self.database_observer.stop()

            # Stop core system
            await self.system.stop()

            logger.info("Coinwatch application stopped")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
            raise

async def main():
    app = CoinwatchApp()

    try:
        await app.setup()

        # Keep application running
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        await app.stop()
    except Exception as e:
        logger.critical(f"Fatal error in main function: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())