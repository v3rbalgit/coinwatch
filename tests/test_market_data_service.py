# test_market_data_service.py

import pytest
import asyncio
from src.adapters.registry import ExchangeAdapterRegistry
from src.services.market_data import MarketDataService
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName
from typing import Union
from src.repositories.market_data import SymbolRepository, KlineRepository
from src.config import Config

# Test with MySQL
@pytest.mark.mysql
async def test_market_data_service_initialization_mysql(
  mysql_config,
  mysql_symbol_repo,
  mysql_kline_repo,
  test_registry
):
  service = MarketDataService(
      mysql_symbol_repo,
      mysql_kline_repo,
      test_registry,
      mysql_config.market_data
  )
  await service.start()
  assert service.is_healthy
  await service.stop()

# Test with SQLite
@pytest.mark.sqlite
async def test_market_data_service_initialization_sqlite(
  sqlite_config,
  symbol_repo,
  kline_repo,
  test_registry
):
  service = MarketDataService(
      symbol_repo,
      kline_repo,
      test_registry,
      sqlite_config.market_data
  )
  await service.start()
  assert service.is_healthy
  await service.stop()

# MySQL sync process test
@pytest.mark.mysql
async def test_market_data_sync_process_mysql(
  mysql_config,
  mysql_symbol_repo,
  mysql_kline_repo,
  test_registry
):
  service = MarketDataService(
      mysql_symbol_repo,
      mysql_kline_repo,
      test_registry,
      mysql_config.market_data
  )

  await service.start()
  # Wait for initial sync
  await asyncio.sleep(2)

  # Verify data was synced
  symbols = await mysql_symbol_repo.get_by_exchange(ExchangeName("bybit"))
  assert len(symbols) > 0

  # Check if klines were stored
  latest_ts = await mysql_kline_repo.get_latest_timestamp(
      SymbolName("BTCUSDT"),
      Timeframe.MINUTE_5,
      ExchangeName("bybit")
  )
  assert latest_ts is not None

  await service.stop()

# SQLite sync process test
@pytest.mark.sqlite
async def test_market_data_sync_process_sqlite(
  sqlite_config,
  symbol_repo,
  kline_repo,
  test_registry
):
  service = MarketDataService(
      symbol_repo,
      kline_repo,
      test_registry,
      sqlite_config.market_data
  )

  await service.start()
  # Wait for initial sync
  await asyncio.sleep(2)

  # Verify data was synced
  symbols = await symbol_repo.get_by_exchange(ExchangeName("bybit"))
  assert len(symbols) > 0

  # Check if klines were stored
  latest_ts = await kline_repo.get_latest_timestamp(
      SymbolName("BTCUSDT"),
      Timeframe.MINUTE_5,
      ExchangeName("bybit")
  )
  assert latest_ts is not None

  await service.stop()

@pytest.mark.parametrize(
    "config_fixture,symbol_repo_fixture,kline_repo_fixture",
    [
        pytest.param(
            "sqlite_config",
            "symbol_repo",
            "kline_repo",
            id="sqlite",
            marks=pytest.mark.sqlite
        ),
        pytest.param(
            "mysql_config",
            "mysql_symbol_repo",
            "mysql_kline_repo",
            id="mysql",
            marks=pytest.mark.mysql
        )
    ]
)
async def test_market_data_sync_process_both(
    request: pytest.FixtureRequest,
    config_fixture: str,
    symbol_repo_fixture: str,
    kline_repo_fixture: str,
    test_registry: ExchangeAdapterRegistry
):
    # Get actual fixtures
    config = request.getfixturevalue(config_fixture)
    symbol_repo = request.getfixturevalue(symbol_repo_fixture)
    kline_repo = request.getfixturevalue(kline_repo_fixture)


    service = MarketDataService(
        symbol_repo,
        kline_repo,
        test_registry,
        config.market_data
    )

    await service.start()

    try:
        # Wait for initial sync
        await asyncio.sleep(2)

        # Verify data was synced
        symbols = await symbol_repo.get_by_exchange(ExchangeName("bybit"))
        assert len(symbols) > 0

        # Check if klines were stored
        latest_ts = await kline_repo.get_latest_timestamp(
            SymbolName("BTCUSDT"),
            Timeframe.MINUTE_5,
            ExchangeName("bybit")
        )
        assert latest_ts is not None
    finally:
        await service.stop()

# Additional helper tests if needed
@pytest.mark.parametrize("config,symbol_repo,kline_repo", [
    pytest.param(
        "sqlite_config",
        "symbol_repo",
        "kline_repo",
        id="sqlite",
        marks=pytest.mark.sqlite
    ),
    pytest.param(
        "mysql_config",
        "mysql_symbol_repo",
        "mysql_kline_repo",
        id="mysql",
        marks=pytest.mark.mysql
    )
])
async def test_market_data_service_error_handling(
  request,
  config,
  db,
  symbol_repo,
  kline_repo,
  test_registry
):
  # Get the actual fixtures
  config = request.getfixturevalue(config)
  db = request.getfixturevalue(db)
  symbol_repo = request.getfixturevalue(symbol_repo)
  kline_repo = request.getfixturevalue(kline_repo)

  service = MarketDataService(
      symbol_repo,
      kline_repo,
      test_registry,
      config.market_data
  )

  await service.start()

  # Test error handling
  try:
      # Simulate an error condition
      await db.engine.dispose()

      # Wait for error handling
      await asyncio.sleep(1)

      # Service should remain healthy despite the error
      assert service.is_healthy

  finally:
      await service.stop()