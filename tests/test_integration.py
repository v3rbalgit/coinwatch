# test_integration.py

import pytest
import asyncio
from src.services.market_data import MarketDataService

async def test_full_data_flow(
    test_config,
    test_db,
    symbol_repo,
    kline_repo,
    test_registry
):
    # Start services
    service = MarketDataService(
        symbol_repo,
        kline_repo,
        test_registry,
        test_config.market_data
    )
    await service.start()

    # Wait for initial sync
    await asyncio.sleep(2)

    # Verify database state
    async with test_db.transaction() as session:
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