# test_integration.py

import pytest
import asyncio
from src.services.market_data import MarketDataService

@pytest.mark.mysql
async def test_full_data_flow(
  mysql_config,  # Changed from test_config
  mysql_db,      # Changed from test_db
  mysql_symbol_repo,  # Changed from symbol_repo
  mysql_kline_repo,   # Changed from kline_repo
  test_registry
):
  # Start services
  service = MarketDataService(
      mysql_symbol_repo,
      mysql_kline_repo,
      test_registry,
      mysql_config.market_data
  )
  await service.start()

  # Wait for initial sync
  await asyncio.sleep(2)

  # Verify database state
  async with mysql_db.transaction() as session:
      # Check symbols
      symbols = await session.execute(
          "SELECT COUNT(*) FROM symbols WHERE exchange = 'bybit'"
      )
      symbol_count = symbols.scalar()
      assert symbol_count > 0

      # Check klines
      klines = await session.execute(
          """
          SELECT COUNT(*) FROM kline_data k
          JOIN symbols s ON k.symbol_id = s.id
          WHERE s.exchange = 'bybit'
          """
      )
      kline_count = klines.scalar()
      assert kline_count > 0

  await service.stop()

# If you want to test with SQLite as well, you can add another test
@pytest.mark.sqlite
async def test_full_data_flow_sqlite(
  sqlite_config,
  sqlite_db,
  symbol_repo,
  kline_repo,
  test_registry
):
  # Start services
  service = MarketDataService(
      symbol_repo,
      kline_repo,
      test_registry,
      sqlite_config.market_data
  )
  await service.start()

  # Wait for initial sync
  await asyncio.sleep(2)

  # Verify database state
  async with sqlite_db.transaction() as session:
      # Check symbols
      symbols = await session.execute(
          "SELECT COUNT(*) FROM symbols WHERE exchange = 'bybit'"
      )
      symbol_count = symbols.scalar()
      assert symbol_count > 0

      # Check klines
      klines = await session.execute(
          """
          SELECT COUNT(*) FROM kline_data k
          JOIN symbols s ON k.symbol_id = s.id
          WHERE s.exchange = 'bybit'
          """
      )
      kline_count = klines.scalar()
      assert kline_count > 0

  await service.stop()