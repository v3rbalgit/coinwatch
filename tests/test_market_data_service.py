# test_market_data_service.py

import asyncio
from src.services.market_data import MarketDataService
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName


async def test_market_data_service_initialization(
    test_config,
    symbol_repo,
    kline_repo,
    test_registry
):
    service = MarketDataService(
        symbol_repo,
        kline_repo,
        test_registry,
        test_config.market_data
    )
    await service.start()
    assert service.is_healthy
    await service.stop()

async def test_market_data_sync_process(
    test_config,
    symbol_repo,
    kline_repo,
    test_registry
):
    service = MarketDataService(
        symbol_repo,
        kline_repo,
        test_registry,
        test_config.market_data
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