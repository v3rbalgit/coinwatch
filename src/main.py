# src/main.py

import sys
import asyncio

from .core.coordination import ServiceCoordinator
from .repositories import KlineRepository, SymbolRepository, MetadataRepository, MarketMetricsRepository
from .services.fundamental_data.service import FundamentalDataService
from .services.monitor.service import MonitoringService

from .config import Config
from .services.database.service import DatabaseService
from .services.market_data.service import MarketDataService
from .adapters.registry import ExchangeAdapterRegistry
from .adapters.bybit import BybitAdapter
from .adapters.coingecko import CoinGeckoAdapter
from .core.exceptions import CoinwatchError
from .utils.logger import LoggerSetup
from .utils.domain_types import ExchangeName

logger = LoggerSetup.setup(__name__)

class Coinwatch:
    """Main application class demonstrating component usage"""

    def __init__(self):
        # Setup logging
        self.logger = LoggerSetup.setup(f"{__name__}.Coinwatch")

        # Load configuration
        self.config = Config()

        # Initialize coordinator
        self.coordinator = ServiceCoordinator()

        # Initialize database service
        self.db_service = DatabaseService(
            self.coordinator,
            self.config.database
        )

        # Initialize exchange registry
        self.exchange_registry = ExchangeAdapterRegistry()

        # Initialize fundamental data service
        self.fundamental_data_service = FundamentalDataService(
            self.coordinator,
            MetadataRepository(self.db_service),
            MarketMetricsRepository(self.db_service),
            self.exchange_registry,
            CoinGeckoAdapter(self.config.coingecko),
            self.config.fundamental_data
        )

        # Initialize market data service
        self.market_data_service = MarketDataService(
            self.coordinator,
            SymbolRepository(self.db_service),
            KlineRepository(self.db_service),
            self.exchange_registry,
            self.config.market_data
        )

        # Initialize monitoring service
        self.monitor_service = MonitoringService(
            self.coordinator,
            self.config.monitoring
        )

    async def start(self) -> None:
        """Start the application"""
        try:
            # Start coordinator
            await self.coordinator.start()

            # Start database
            await self.db_service.start()

            # Setup exchange adapters
            await self.exchange_registry.register(
                ExchangeName("bybit"),
                BybitAdapter(self.config.exchanges.bybit)
                )

            # Start services
            await self.fundamental_data_service.start()
            await self.market_data_service.start()
            await self.monitor_service.start()

            # Keep application running
            while True:
                await asyncio.sleep(3600)

                # Log service status periodically
                self.logger.info("\nService Status Report:")
                self.logger.info(self.monitor_service.get_service_status())

        except Exception as e:
            self.logger.error(f"Application error: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the application"""
        await self.monitor_service.stop()
        await self.market_data_service.stop()
        await self.fundamental_data_service.stop()
        await self.db_service.stop()
        await self.coordinator.stop()

async def main():
    """Application entry point"""
    app = Coinwatch()

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


if __name__ == "__main__":
    asyncio.run(main())