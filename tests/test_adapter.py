# test_adapter.py

import pytest
from src.core.models import KlineData
from src.utils.domain_types import SymbolName, Timeframe, ExchangeName

async def test_bybit_adapter_initialization(test_registry):
    adapter = test_registry.get_adapter(ExchangeName("bybit"))
    assert adapter is not None
    symbols = await adapter.get_symbols()
    assert len(symbols) > 0
    assert any(s.name == "BTCUSDT" for s in symbols)

async def test_bybit_adapter_kline_fetching(test_registry):
    adapter = test_registry.get_adapter(ExchangeName("bybit"))
    klines = await adapter.get_klines(
        SymbolName("BTCUSDT"),
        Timeframe.MINUTE_5
    )
    assert len(klines) > 0
    assert all(isinstance(k, KlineData) for k in klines)
    assert all(k.symbol == "BTCUSDT" for k in klines)