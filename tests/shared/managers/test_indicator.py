from typing import Any
import pytest
import polars as pl
from decimal import Decimal
from datetime import datetime, timezone
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from shared.managers.indicator import IndicatorManager
from shared.core.models import KlineModel, RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
from shared.core.enums import Interval
from shared.core.exceptions import ValidationError
from shared.utils.time import to_timestamp
from shared.utils.cache import RedisCache
from pydantic import BaseModel

class MockRedisCache(RedisCache):
    """Mock Redis cache for testing"""
    def __init__(self, *args, **kwargs):
        self._cache = {}
        self.namespace = "test"

    async def get(self, key: str):
        return self._cache.get(self._make_key(key))

    async def set(self, key: str, value: Any, ttl: int = 300):
        self._cache[self._make_key(key)] = value
        return True

    async def close(self):
        self._cache.clear()

@pytest.fixture
def sample_klines():
    """Generate sample kline data for testing"""
    base_time = to_timestamp(datetime(2023, 1, 1, tzinfo=timezone.utc))
    return [
        KlineModel(
            timestamp=base_time + (i * 60000),  # 1 minute intervals
            open_price=Decimal(str(100 + i)),
            high_price=Decimal(str(105 + i)),
            low_price=Decimal(str(95 + i)),
            close_price=Decimal(str(102 + i)),
            volume=Decimal("1000"),
            turnover=Decimal("100000"),
            interval=Interval.MINUTE_1
        )
        for i in range(50)  # Generate 50 klines
    ]

@pytest.fixture
async def indicator_manager():
    """Create IndicatorManager instance with mocked Redis"""
    with patch('shared.utils.cache.RedisCache', MockRedisCache):
        manager = IndicatorManager("redis://fake")
        yield manager
        await manager.cleanup()

# Input validation tests
async def test_validate_klines_empty(indicator_manager):
    """Test validation with empty klines"""
    with pytest.raises(ValidationError, match="No kline data provided"):
        indicator_manager._validate_klines([])

async def test_validate_klines_insufficient(indicator_manager):
    """Test validation with insufficient klines"""
    klines = [KlineModel(
        timestamp=to_timestamp(datetime(2023, 1, 1, tzinfo=timezone.utc)),
        open_price=Decimal("100"),
        high_price=Decimal("105"),
        low_price=Decimal("95"),
        close_price=Decimal("102"),
        volume=Decimal("1000"),
        turnover=Decimal("100000"),
        interval=Interval.MINUTE_1
    ) for _ in range(20)]

    with pytest.raises(ValidationError, match="Insufficient kline data"):
        indicator_manager._validate_klines(klines)

# RSI tests
async def test_rsi_calculation(indicator_manager, sample_klines):
    """Test RSI calculation with sample data"""
    results = await indicator_manager.calculate_rsi(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, RSIResult)
        assert 0 <= float(result.value) <= 100
        assert result.length == 14  # Default length

async def test_rsi_custom_length(indicator_manager, sample_klines):
    """Test RSI calculation with custom length"""
    results = await indicator_manager.calculate_rsi(sample_klines, length=21)

    assert len(results) > 0
    for result in results:
        assert result.length == 21

# Bollinger Bands tests
async def test_bollinger_bands_calculation(indicator_manager, sample_klines):
    """Test Bollinger Bands calculation"""
    results = await indicator_manager.calculate_bollinger_bands(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, BollingerBandsResult)
        assert float(result.lower) <= float(result.middle) <= float(result.upper)
        assert float(result.bandwidth) >= 0

async def test_bollinger_bands_custom_params(indicator_manager, sample_klines):
    """Test Bollinger Bands with custom parameters"""
    results = await indicator_manager.calculate_bollinger_bands(
        sample_klines, length=10, std_dev=3.0
    )

    assert len(results) > 0
    for result in results:
        assert float(result.upper) - float(result.lower) > 0

# MACD tests
async def test_macd_calculation(indicator_manager, sample_klines):
    """Test MACD calculation"""
    results = await indicator_manager.calculate_macd(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, MACDResult)
        # Verify histogram matches macd - signal
        assert abs(float(result.histogram) -
                  (float(result.macd) - float(result.signal))) < 0.0001

async def test_macd_custom_params(indicator_manager, sample_klines):
    """Test MACD with custom parameters"""
    results = await indicator_manager.calculate_macd(
        sample_klines, fast=8, slow=21, signal=5
    )

    assert len(results) > 0

# Moving Average tests
async def test_sma_calculation(indicator_manager, sample_klines):
    """Test Simple Moving Average calculation"""
    results = await indicator_manager.calculate_sma(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, MAResult)

async def test_ema_calculation(indicator_manager, sample_klines):
    """Test Exponential Moving Average calculation"""
    results = await indicator_manager.calculate_ema(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, MAResult)

# OBV tests
async def test_obv_calculation(indicator_manager, sample_klines):
    """Test On Balance Volume calculation"""
    results = await indicator_manager.calculate_obv(sample_klines)

    assert len(results) > 0
    for result in results:
        assert isinstance(result, OBVResult)

# Cache tests
async def test_cache_usage(indicator_manager, sample_klines):
    """Test that results are cached"""
    # First call should calculate and cache
    results1 = await indicator_manager.calculate_rsi(sample_klines)
    assert len(results1) > 0

    # Second call should return cached results
    results2 = await indicator_manager.calculate_rsi(sample_klines)
    assert results1 == results2
