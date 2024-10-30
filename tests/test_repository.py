# test_repository.py

import pytest
from datetime import datetime, timezone
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName

async def test_symbol_repository_operations(symbol_repo):
    # Test symbol creation
    symbol = await symbol_repo.get_or_create(
        SymbolName("BTCUSDT"),
        ExchangeName("bybit")
    )
    assert symbol.name == "BTCUSDT"
    assert symbol.exchange == "bybit"

    # Test symbol retrieval
    symbols = await symbol_repo.get_by_exchange(ExchangeName("bybit"))
    assert len(symbols) > 0
    assert any(s.name == "BTCUSDT" for s in symbols)

async def test_kline_repository_operations(kline_repo, symbol_repo):
    # Create test symbol
    symbol = await symbol_repo.get_or_create(
        SymbolName("BTCUSDT"),
        ExchangeName("bybit")
    )

    # Test kline insertion
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    test_klines = [
        (timestamp, 50000.0, 51000.0, 49000.0, 50500.0, 10.5, 525000.0),
        (timestamp + 300000, 50500.0, 52000.0, 50000.0, 51500.0, 12.3, 615000.0)
    ]

    inserted = await kline_repo.insert_batch(
        symbol.id,
        Timeframe.MINUTE_5,
        test_klines
    )
    assert inserted == len(test_klines)

    # Test gap detection
    gaps = await kline_repo.get_data_gaps(
        SymbolName("BTCUSDT"),
        Timeframe.MINUTE_5,
        ExchangeName("bybit"),
        timestamp - 3600000,  # 1 hour before
        timestamp + 3600000   # 1 hour after
    )
    assert isinstance(gaps, list)