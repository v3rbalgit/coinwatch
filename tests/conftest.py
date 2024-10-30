# tests/conftest.py
import pytest
import os
import logging
import asyncio
from decimal import Decimal
from datetime import datetime, timezone
from typing import AsyncGenerator, Generator, List, Optional

from src.core.protocols import ExchangeAdapter
from src.config import Config, DatabaseConfig
from src.models.base import Base
from src.services.database import DatabaseService
from src.repositories.market_data import SymbolRepository, KlineRepository
from src.adapters.registry import ExchangeAdapterRegistry
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName, Timestamp
from src.core.models import KlineData, SymbolInfo
from src.config import Config, DatabaseConfig

class TestConfigClass(Config):
    def __init__(self):
        super().__init__()
        # Override database config for testing
        self.database = DatabaseConfig(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="test_db",
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=True
        )

    def _init_log_config(self):
        config = super()._init_log_config()

        # Set up test-specific directory
        test_log_dir = os.path.join(os.path.dirname(__file__), 'test_logs')
        os.makedirs(test_log_dir, exist_ok=True)
        config.file_path = os.path.join(test_log_dir, 'test.log')

        # Configure test logging
        logging.root.handlers.clear()

        # Add console handler for tests
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logging.root.addHandler(console_handler)

        return config


# Mock Bybit Adapter for testing
class MockBybitAdapter(ExchangeAdapter):
    async def initialize(self) -> None:
        pass

    async def get_symbols(self) -> List[SymbolInfo]:
        return [
            SymbolInfo(
                name=SymbolName("BTCUSDT"),
                base_asset="BTC",
                quote_asset="USDT",
                price_precision=2,
                qty_precision=6,
                min_order_qty=Decimal("0.001")
            ),
            SymbolInfo(
                name=SymbolName("ETHUSDT"),
                base_asset="ETH",
                quote_asset="USDT",
                price_precision=2,
                qty_precision=5,
                min_order_qty=Decimal("0.01")
            )
        ]

    async def get_klines(self,
                        symbol: SymbolName,
                        timeframe: Timeframe,
                        start_time: Optional[Timestamp] = None) -> List[KlineData]:
        current_time = Timestamp(int(datetime.now(timezone.utc).timestamp() * 1000))
        interval = timeframe.to_milliseconds()

        klines = []
        for i in range(10):  # Generate 10 klines for testing
            timestamp = current_time - (i * interval)
            klines.append(KlineData(
                timestamp=timestamp,
                open_price=Decimal("50000.00"),
                high_price=Decimal("51000.00"),
                low_price=Decimal("49000.00"),
                close_price=Decimal("50500.00"),
                volume=Decimal("10.5"),
                turnover=Decimal("525000.00"),
                symbol=symbol,
                timeframe=timeframe.value
            ))
        return klines

    async def close(self) -> None:
        pass

# Fixtures
@pytest.fixture(scope="session")
async def test_config() -> Config:
    return TestConfigClass()

@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
  loop = asyncio.new_event_loop()
  yield loop
  loop.close()

@pytest.fixture
async def test_db(test_config: Config) -> AsyncGenerator[DatabaseService, None]:
    # Create test database service
    db_service = DatabaseService(test_config.database)
    await db_service.start()

    if db_service.engine is None:
        raise RuntimeError("Database engine not initialized")

    # Create tables
    async with db_service.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield db_service

    # Cleanup
    if db_service.engine is not None:
        async with db_service.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    await db_service.stop()

@pytest.fixture
async def test_registry() -> AsyncGenerator[ExchangeAdapterRegistry, None]:
    registry = ExchangeAdapterRegistry()
    mock_adapter: ExchangeAdapter = MockBybitAdapter()
    await registry.register(ExchangeName("bybit"), mock_adapter)
    await registry.initialize_all()
    yield registry

@pytest.fixture
async def symbol_repo(test_db: DatabaseService) -> AsyncGenerator[SymbolRepository, None]:
    repo = SymbolRepository(test_db.session_factory)
    yield repo

@pytest.fixture
async def kline_repo(test_db: DatabaseService) -> AsyncGenerator[KlineRepository, None]:
    repo = KlineRepository(test_db.session_factory)
    yield repo