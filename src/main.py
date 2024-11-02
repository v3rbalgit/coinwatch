# src/main.py
import sys
import asyncio

from .config import Config
from .services.database import DatabaseService
from .services.market_data import MarketDataService
from .repositories.market_data import SymbolRepository, KlineRepository
from .adapters.registry import ExchangeAdapterRegistry
from .adapters.bybit import BybitAdapter
from .core.exceptions import CoinwatchError
from .utils.logger import LoggerSetup
from .utils.domain_types import ExchangeName, ServiceStatus

logger = LoggerSetup.setup(__name__)

class Application:
    """Main application class demonstrating component usage"""

    def __init__(self):
        # Load configuration
        self.config = Config()

        # Setup logging
        self.logger = LoggerSetup.setup(f"{__name__}.Application")

        # Initialize services
        self.db_service = DatabaseService(self.config.database)
        self.exchange_registry = ExchangeAdapterRegistry()

    async def start(self) -> None:
        """Start the application"""
        try:
            # Initialize database
            await self.db_service.start()

            # Seup exchange adapters
            await self.exchange_registry.register(
                ExchangeName("bybit"),
                BybitAdapter(self.config.exchanges.bybit)
                )

            # Create repositories without a transaction
            symbol_repo = SymbolRepository(self.db_service.session_factory)
            kline_repo = KlineRepository(self.db_service.session_factory)

            # Setup market data service
            market_service = MarketDataService(
                symbol_repo,
                kline_repo,
                self.exchange_registry,
                self.config.market_data
            )

            # Start service
            await market_service.start()

            # Keep application running
            while True:
                if not market_service._status == ServiceStatus.ERROR:
                    self.logger.error(
                        f"Service unhealthy: {market_service._last_error}"
                    )
                    await market_service.handle_error(
                        market_service._last_error
                    )

                # Log service status periodically
                self.logger.info("\nService Status Report:")
                self.logger.info(market_service.state_manager.get_status_report())

                await asyncio.sleep(60)

        except Exception as e:
            self.logger.error(f"Application error: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the application"""
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