import sys
import asyncio
from typing import List

from .config import BybitConfig, Config
from .services.database import DatabaseService
from .services.market_data import MarketDataService
from .repositories.market_data import SymbolRepository, KlineRepository
from .adapters.registry import ExchangeAdapterRegistry
from .adapters.bybit import BybitAdapter
from .core.exceptions import CoinwatchError
from .utils.logger import LoggerSetup
from .utils.domain_types import ExchangeName, SymbolName
from .core.models import SymbolInfo

logger = LoggerSetup.setup(__name__)

class TestApplication:
    """Test version of the application that only processes specific symbols"""

    def __init__(self, test_symbols: List[SymbolName]):
        # Load configuration
        self.config = Config()
        self.test_symbols = test_symbols

        # Setup logging
        self.logger = LoggerSetup.setup(f"{__name__}.TestApplication")

        # Initialize services
        self.db_service = DatabaseService(self.config.database)
        self.exchange_registry = ExchangeAdapterRegistry()

        # Override BybitAdapter's get_symbols method
        class TestBybitAdapter(BybitAdapter):
            async def get_symbols(self) -> List[SymbolInfo]:
                """Override to return only test symbols"""
                all_symbols = await super().get_symbols()
                return [s for s in all_symbols if s.name in test_symbols]

        # Register test adapter
        self.test_adapter = TestBybitAdapter(BybitConfig(
            testnet=True,
            rate_limit=600,
            rate_limit_window=300
        ))

    async def start(self) -> None:
        """Start the test application"""
        try:
            # Initialize database
            await self.db_service.start()

            # Register exchange adapter
            await self.exchange_registry.register(
                ExchangeName("bybit"),
                self.test_adapter
            )

            # Create repositories
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

            self.logger.info(f"Test application started with symbols: {self.test_symbols}")

            # Keep application running
            while True:
                if not market_service.is_healthy:
                    self.logger.error(f"Service unhealthy: {market_service._last_error}")
                    await market_service.handle_error(market_service._last_error)
                await asyncio.sleep(60)

        except Exception as e:
            self.logger.error(f"Application error: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the test application"""
        await self.db_service.stop()

async def main():
    """Test entry point"""
    # List of symbols to test with
    test_symbols = [
        SymbolName("10000WHYUSDT"),
        SymbolName("1000CATSUSDT")
    ]

    app = TestApplication(test_symbols)

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