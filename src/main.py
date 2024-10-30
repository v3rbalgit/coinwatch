# src/main.py
import sys
import asyncio

from .config import BybitConfig, Config
from .services.database import DatabaseService
from .services.market_data import MarketDataService
from .repositories.market_data import SymbolRepository, KlineRepository
from .adapters.registry import ExchangeAdapterRegistry
from .adapters.bybit import BybitAdapter
from .core.exceptions import CoinwatchError
from .utils.logger import LoggerSetup
from .domain_types import ExchangeName

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

    async def _setup_exchanges(self) -> None:
        """Setup exchange adapters"""
        bybit_config = self.config.exchanges.bybit
        await self.exchange_registry.register(
            ExchangeName("bybit"),
            BybitAdapter(BybitConfig(
                api_key=bybit_config.api_key,
                api_secret=bybit_config.api_secret,
                testnet=bybit_config.testnet,
                rate_limit=bybit_config.rate_limit,
                rate_limit_window=bybit_config.rate_limit_window)
            )
        )

    async def start(self) -> None:
        """Start the application"""
        try:
            # Initialize database
            await self.db_service.start()

            # Create repositories
            async with self.db_service.get_session() as session:
                symbol_repo = SymbolRepository(session)
                kline_repo = KlineRepository(session)

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
                    if not market_service.is_healthy:
                        self.logger.error(
                            f"Service unhealthy: {market_service._last_error}"
                        )
                        await market_service.handle_error(
                            market_service._last_error
                        )
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


# Example usage with specific operations
# async def example_operations():
#     """Example of specific operations using the components"""
#     app = Application("postgresql+asyncpg://user:pass@localhost/coinwatch")

#     try:
#         async with app.get_repository() as repository:
#             # Get or create symbol
#             symbol = await repository.get_or_create_symbol(
#                 "BTCUSDT", "bybit"
#             )

#             # Get latest timestamp
#             latest_ts = await repository.get_latest_timestamp(
#                 "BTCUSDT",
#                 Timeframe.MINUTE_5,
#                 "bybit"
#             )

#             # Get exchange adapter
#             exchange = app.exchange_registry.get_adapter("bybit")

#             # Fetch new data
#             klines = await exchange.get_klines(
#                 "BTCUSDT",
#                 Timeframe.MINUTE_5,
#                 latest_ts
#             )

#             # Insert new data
#             if klines:
#                 inserted = await repository.insert_klines(
#                     symbol.id,
#                     Timeframe.MINUTE_5,
#                     klines
#                 )
#                 logger.info(f"Inserted {inserted} new klines")

#             # Check for gaps
#             gaps = await repository.get_data_gaps(
#                 "BTCUSDT",
#                 Timeframe.MINUTE_5,
#                 "bybit",
#                 latest_ts - (86400 * 1000),  # Last 24 hours
#                 latest_ts
#             )

#             if gaps:
#                 logger.warning(f"Found {len(gaps)} gaps in data")

#     except CoinwatchError as e:
#         logger.error(f"Operation failed: {e}")
#     finally:
#         await app.stop()