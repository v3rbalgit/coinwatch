# tests/conftest.py
import pytest
import os
import logging
import asyncio
from decimal import Decimal
from datetime import datetime, timezone
from typing import AsyncGenerator, List, Optional, Generator

from src.core.protocols import ExchangeAdapter
from src.config import Config, DatabaseConfig
from src.models.base import Base
from src.services.database import DatabaseService
from src.repositories.market_data import SymbolRepository, KlineRepository
from src.adapters.registry import ExchangeAdapterRegistry
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName, Timestamp
from src.core.models import KlineData, SymbolInfo
from src.config import Config, DatabaseConfig

class TestConfigBase(Config):
  """Base test configuration with common settings"""
  def _init_log_config(self):
      config = super()._init_log_config()
      test_log_dir = os.path.join(os.path.dirname(__file__), 'test_logs')
      os.makedirs(test_log_dir, exist_ok=True)

      # Create a file handler
      file_handler = logging.FileHandler(
          os.path.join(test_log_dir, 'test.log'),
          mode='w'
      )
      file_handler.setLevel(logging.DEBUG)
      file_handler.setFormatter(logging.Formatter(
          '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
      ))

      # Add handler to root logger
      logging.getLogger().addHandler(file_handler)

      return config

class SQLiteTestConfig(TestConfigBase):
  """SQLite configuration for unit tests"""
  def __init__(self):
      super().__init__()
      self.database = DatabaseConfig(
          host="",
          port=0,
          user="",
          password="",
          database="test_db.sqlite",
          pool_size=5,
          max_overflow=10,
          pool_timeout=30,
          pool_recycle=1800,
          echo=True,
          dialect="sqlite",
          driver="aiosqlite"
      )

class MySQLTestConfig(TestConfigBase):
  """MySQL configuration for integration tests"""
  def __init__(self):
      super().__init__()
      self.database = DatabaseConfig(
          host="localhost",
          port=3306,
          user="test",
          password="test",
          database="test_db",
          pool_size=5,
          max_overflow=10,
          pool_timeout=30,
          pool_recycle=1800,
          echo=True,
          dialect="mysql",
          driver="aiomysql"
      )

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

def pytest_configure(config):
    """Configure pytest for async testing"""
    config.addinivalue_line(
        "markers",
        "asyncio: mark test as async"
    )

# Fixtures
@pytest.fixture(scope="function")
def event_loop():
    """Create new event loop for each test"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    # Clean up
    if loop.is_running():
        loop.stop()
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()
    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()

@pytest.fixture(autouse=True)
async def setup_test_loop(event_loop):
    """Ensure event loop is properly set for each test"""
    asyncio.set_event_loop(event_loop)
    yield
    # Cleanup any pending tasks
    pending = [t for t in asyncio.all_tasks(event_loop) if not t.done()]
    for task in pending:
        task.cancel()
        try:
            await asyncio.gather(task, return_exceptions=True)
        except asyncio.CancelledError:
            pass

@pytest.fixture(autouse=True)
async def clean_loop():
    yield
    loop = asyncio.get_event_loop()
    tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

@pytest.fixture(scope="session")
async def sqlite_config() -> Config:
  return SQLiteTestConfig()

@pytest.fixture(scope="session")
async def mysql_config() -> Config:
  return MySQLTestConfig()

@pytest.fixture
async def sqlite_db(sqlite_config: Config) -> AsyncGenerator[DatabaseService, None]:
    db_service = DatabaseService(sqlite_config.database)
    try:
        await db_service.start()

        if db_service.engine is None:
            raise RuntimeError("Database engine not initialized")

        async with db_service.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield db_service
    finally:
        if db_service.engine is not None:
            async with db_service.engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
            await db_service.stop()

@pytest.fixture
async def mysql_db(mysql_config: Config) -> AsyncGenerator[DatabaseService, None]:
  db_service = DatabaseService(mysql_config.database)
  await db_service.start()

  if db_service.engine is None:
      raise RuntimeError("Database engine not initialized")

  async with db_service.engine.begin() as conn:
      await conn.run_sync(Base.metadata.create_all)

  yield db_service

  if db_service.engine is not None:
      async with db_service.engine.begin() as conn:
          await conn.run_sync(Base.metadata.drop_all)

  for task in asyncio.all_tasks():
        if task != asyncio.current_task():
            task.cancel()

  await db_service.stop()

@pytest.fixture
async def test_registry() -> AsyncGenerator[ExchangeAdapterRegistry, None]:
  registry = ExchangeAdapterRegistry()
  mock_adapter: ExchangeAdapter = MockBybitAdapter()
  await registry.register(ExchangeName("bybit"), mock_adapter)
  await registry.initialize_all()
  yield registry

@pytest.fixture(autouse=True)
async def cleanup_registry():
    """Cleanup any registered adapters after each test"""
    yield
    ExchangeAdapterRegistry()._adapters.clear()
    ExchangeAdapterRegistry()._initialized_adapters.clear()

@pytest.fixture
def mock_current_time(monkeypatch):
    """Mock current time for predictable timestamps"""
    fixed_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(
        'src.utils.time.get_current_timestamp',
        lambda: int(fixed_time.timestamp() * 1000)
    )
    return fixed_time

@pytest.fixture
async def symbol_repo(sqlite_db: DatabaseService) -> AsyncGenerator[SymbolRepository, None]:
  repo = SymbolRepository(sqlite_db.session_factory)
  yield repo

@pytest.fixture
async def mysql_symbol_repo(mysql_db: DatabaseService) -> AsyncGenerator[SymbolRepository, None]:
  repo = SymbolRepository(mysql_db.session_factory)
  yield repo

@pytest.fixture
async def kline_repo(sqlite_db: DatabaseService) -> AsyncGenerator[KlineRepository, None]:
  repo = KlineRepository(sqlite_db.session_factory)
  yield repo

@pytest.fixture
async def mysql_kline_repo(mysql_db: DatabaseService) -> AsyncGenerator[KlineRepository, None]:
  repo = KlineRepository(mysql_db.session_factory)
  yield repo