# src/main.py

import sys
import asyncio

from src.core.coordination import ServiceCoordinator
from src.repositories.kline import KlineRepository
from src.repositories.symbol import SymbolRepository
from src.services.monitor.service import MonitoringService

from .config import Config
from .services.database.service import DatabaseService
from .services.market_data.service import MarketDataService
from .adapters.registry import ExchangeAdapterRegistry
from .adapters.bybit import BybitAdapter
from .core.exceptions import CoinwatchError
from .utils.logger import LoggerSetup
from .utils.domain_types import ExchangeName

logger = LoggerSetup.setup(__name__)

class Application:
    """Main application class demonstrating component usage"""

    def __init__(self):
        # Setup logging
        self.logger = LoggerSetup.setup(f"{__name__}.Application")

        # Load configuration
        self.config = Config()

        # Initialize coordinator
        self.coordinator = ServiceCoordinator()

        # Initialize database service
        self.db_service = DatabaseService(
            self.coordinator,
            self.config.database
        )

        # Initialize monitoring service
        self.monitor_service = MonitoringService(
            self.coordinator,
            self.config.monitoring
        )

        # Initialize exchange registry
        self.exchange_registry = ExchangeAdapterRegistry()

    async def start(self) -> None:
        """Start the application"""
        try:
            # Start coordinator
            await self.coordinator.start()

            # Initialize database
            await self.db_service.start()

            # Seup exchange adapters
            await self.exchange_registry.register(
                ExchangeName("bybit"),
                BybitAdapter(self.config.exchanges.bybit)
                )

            # Create repositories without a transaction
            symbol_repo = SymbolRepository(self.db_service)
            kline_repo = KlineRepository(self.db_service)

            # Setup market data service
            market_service = MarketDataService(
                symbol_repo,
                kline_repo,
                self.exchange_registry,
                self.coordinator,
                self.config.market_data
            )

            # Start services
            await market_service.start()
            await self.monitor_service.start()

            # Keep application running
            while True:
                # Log service status periodically
                self.logger.info("\nService Status Report:")
                self.logger.info(self.monitor_service.get_service_status())

                await asyncio.sleep(60)

        except Exception as e:
            self.logger.error(f"Application error: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the application"""
        await self.monitor_service.stop()
        await self.db_service.stop()

async def main():
    """Application entry point"""
    app = Application()

    try:
        await app.start()
    except CoinwatchError as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        await app.stop()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await app.stop()
        sys.exit(1)


# Run the application
if __name__ == "__main__":
    asyncio.run(main())