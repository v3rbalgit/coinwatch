# src/managers/indicator.py

import asyncio
import os
import pandas as pd
import pandas_ta as ta
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from ..core.coordination import ServiceCoordinator
from ..core.exceptions import ServiceError, ValidationError
from ..core.models import (
    KlineData,
    RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
    )
from ..utils.domain_types import Timestamp
from ..utils.logger import LoggerSetup
from ..utils.retry import RetryConfig, RetryStrategy
from ..utils.cache import AsyncTTLCache, async_ttl_cache

logger = LoggerSetup.setup(__name__)

@dataclass
class CacheConfig:
    """Configuration for indicator caching"""
    ttl: int
    max_size: int

    def __post_init__(self) -> None:
        if self.ttl < 60:  # Minimum 1 minute
            self.ttl = 60
        if self.max_size < 100:  # Minimum 100 entries
            self.max_size = 100

@dataclass
class IndicatorCacheConfig:
    """Fixed configuration for all indicator caches"""
    default: CacheConfig
    fast_indicators: CacheConfig

    @classmethod
    def from_memory_limit(cls, memory_limit_mb: int) -> 'IndicatorCacheConfig':
        """
        Create cache configuration based on container memory limit.
        Args:
            memory_limit_mb: Container memory limit in MB
        """
        # Allocate 30% of container memory for caches
        cache_memory = int(memory_limit_mb * 0.3)

        # Calculate cache sizes
        # Fast indicators (more sensitive to price changes) get 40% of cache memory
        fast_size = max(1000, int((cache_memory * 0.4) / 0.001))  # Assume ~1KB per cache entry
        # Default cache gets 60% of cache memory
        default_size = max(1000, int((cache_memory * 0.6) / 0.001))

        return cls(
            default=CacheConfig(
                ttl=300,  # 5 minutes
                max_size=default_size
            ),
            fast_indicators=CacheConfig(
                ttl=60,  # 1 minute
                max_size=fast_size
            )
        )

    @classmethod
    def get_container_memory_limit(cls) -> int:
        """
        Get container memory limit from environment or cgroups.
        Returns memory limit in MB.
        """
        # Try to get from environment first
        if memory_limit := os.getenv('MEMORY_LIMIT'):
            try:
                # Convert from format like "512Mi" to MB
                if memory_limit.endswith('Mi'):
                    return int(memory_limit[:-2])
                if memory_limit.endswith('Gi'):
                    return int(memory_limit[:-2]) * 1024
                return int(memory_limit)
            except ValueError:
                logger.warning(f"Invalid MEMORY_LIMIT format: {memory_limit}")

        # Fallback to cgroups
        try:
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                memory_bytes = int(f.read().strip())
                return memory_bytes // (1024 * 1024)  # Convert to MB
        except (FileNotFoundError, ValueError, IOError):
            logger.warning("Could not determine container memory limit")

        # Default to 512MB if we can't determine limit
        return 512

class IndicatorManager:
    """
    Manages technical indicator calculations using pandas-ta.

    This manager interfaces with TimeframeManager to get price data
    and calculates various technical indicators with proper caching
    and validation.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 config: Optional[IndicatorCacheConfig] = None):
        self.coordinator = coordinator

        # Initialize cache configuration
        if config is None:
            memory_limit = IndicatorCacheConfig.get_container_memory_limit()
            self.config = IndicatorCacheConfig.from_memory_limit(memory_limit)
            logger.info(
                f"Initialized cache with memory limit {memory_limit}MB "
                f"(default: {self.config.default.max_size} entries, "
                f"fast: {self.config.fast_indicators.max_size} entries)"
            )
        else:
            self.config = config

        self._calculation_lock = asyncio.Lock()

        # Initialize retry strategy
        retry_config = RetryConfig(
            base_delay=1.0,    # Start with 1 second delay
            max_delay=30.0,    # Maximum 30 seconds delay
            max_retries=3,     # Try 3 times
            jitter_factor=0.1  # 10% jitter
        )
        self._retry_strategy = RetryStrategy(retry_config)
        self._configure_retry_strategy()

        # Initialize caches with config
        self._setup_indicator_caches()

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior for indicator calculations"""
        from pandas.errors import EmptyDataError

        # Basic retryable errors
        self._retry_strategy.add_retryable_error(
            ConnectionError,      # Network/connection issues
            TimeoutError,        # Timeout issues
            EmptyDataError       # Pandas empty data issues
        )

        self._retry_strategy.add_non_retryable_error(
            ValidationError,     # Data validation errors
            ValueError,         # Invalid values
            ServiceError       # Application errors
        )

        # Configure specific delays for different error types
        self._retry_strategy.configure_error_delays({
            EmptyDataError: RetryConfig(
                base_delay=1.0,    # Quick retry for empty data
                max_delay=10.0,
                max_retries=2,
                jitter_factor=0.1
            ),
            ConnectionError: RetryConfig(
                base_delay=2.0,    # Longer delay for connection issues
                max_delay=30.0,
                max_retries=3,
                jitter_factor=0.2
            ),
            TimeoutError: RetryConfig(
                base_delay=3.0,    # Even longer for timeouts
                max_delay=45.0,
                max_retries=3,
                jitter_factor=0.2
            )
        })

    async def _calculate_with_retry(self,
                                  calc_name: str,
                                  calc_func: Callable,
                                  *args: Any,
                                  **kwargs: Any) -> Any:
        """Execute calculation with retry logic"""
        retry_count = 0

        while True:
            try:
                return await calc_func(*args, **kwargs)

            except Exception as e:
                should_retry, reason = self._retry_strategy.should_retry(retry_count, e)

                if should_retry:
                    retry_count += 1
                    delay = self._retry_strategy.get_delay(retry_count)
                    logger.warning(
                        f"Retrying {calc_func.__name__} to calculate {calc_name} "
                        f"after {delay:.2f}s (attempt {retry_count}): {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                raise  # Non-retryable error or max retries exceeded

    def _setup_indicator_caches(self) -> None:
        """Apply fixed cache configuration to all indicators"""
        # Fast indicators (more sensitive to price changes)
        self._setup_cache(self.calculate_rsi, self.config.fast_indicators)
        self._setup_cache(self.calculate_obv, self.config.fast_indicators)

        # Standard indicators with default configuration
        for method in [
            self.calculate_macd,
            self.calculate_bollinger_bands,
            self.calculate_sma,
            self.calculate_ema
        ]:
            self._setup_cache(method, self.config.default)

    def _setup_cache(self, method: Any, config: CacheConfig) -> None:
        """Setup cache for individual indicator method"""
        if hasattr(method, '__wrapped__'):
            cache_instance = self._get_cache_instance(method)
            if cache_instance:
                cache_instance.reconfigure(
                    maxsize=config.max_size,
                    ttl=config.ttl
                )

    def _get_cache_instance(self, method: Any) -> Optional[AsyncTTLCache]:
        """Safely get cache instance from method"""
        try:
            if hasattr(method, '__wrapped__'):
                cache_instance = method.__wrapped__.__closure__[0].cell_contents
                if isinstance(cache_instance, AsyncTTLCache):
                    return cache_instance
        except Exception as e:
            logger.error(f"Error accessing cache instance: {e}")
        return None

    async def cleanup(self) -> None:
        """Cleanup resources"""

        # Clear all caches
        for method in [
            self.calculate_rsi,
            self.calculate_macd,
            self.calculate_bollinger_bands,
            self.calculate_sma,
            self.calculate_ema,
            self.calculate_obv
        ]:
            if hasattr(method, 'cache_clear'):
                method.cache_clear()

    def get_cache_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all indicator caches"""
        stats = {}

        for name, method in [
            ('rsi', self.calculate_rsi),
            ('macd', self.calculate_macd),
            ('bollinger', self.calculate_bollinger_bands),
            ('sma', self.calculate_sma),
            ('ema', self.calculate_ema),
            ('obv', self.calculate_obv)
        ]:
            if hasattr(method, 'cache_info'):
                cache_stats = method.cache_info()
                stats[name] = {
                    'size': cache_stats.size,
                    'maxsize': cache_stats.maxsize,
                    'ttl': cache_stats.ttl,
                    'hits': cache_stats.hits,
                    'misses': cache_stats.misses,
                    'hit_ratio': cache_stats.hit_ratio
                }

        return stats

    def _validate_klines(self, klines: List[KlineData]) -> None:
        """Validate kline data for indicator calculation"""
        if not klines:
            raise ValidationError("No kline data provided")
        if len(klines) < 30:
            raise ValidationError(
                f"Insufficient kline data. Need at least 30 candles, got {len(klines)}"
            )

    def _prepare_dataframe(self, klines: List[KlineData]) -> pd.DataFrame:
        """Convert KlineData to pandas DataFrame"""
        df = pd.DataFrame([{
            'timestamp': k.timestamp,
            'open': float(k.open_price),
            'high': float(k.high_price),
            'low': float(k.low_price),
            'close': float(k.close_price),
            'volume': float(k.volume)
        } for k in klines])

        df.set_index('timestamp', inplace=True)
        return df

    @async_ttl_cache()
    async def calculate_rsi(self,
                            klines: List[KlineData],
                            length: int = 14,
                            ) -> List[RSIResult]:
        """Calculate Relative Strength Index"""
        async def _calc() -> List[RSIResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            rsi = df.ta.rsi(length=length)
            return [
                RSIResult.from_series(
                    Timestamp(int(idx)),
                    value,
                    length=length
                )
                for idx, value in rsi.items()
                if not pd.isna(value)
            ]

        return await self._calculate_with_retry('RSI', _calc)

    @async_ttl_cache()
    async def calculate_bollinger_bands(self,
                                        klines: List[KlineData],
                                        length: int = 20,
                                        std_dev: float = 2.0,
                                        ) -> List[BollingerBandsResult]:
        """Calculate Bollinger Bands"""
        async def _calc() -> List[BollingerBandsResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            bb = df.ta.bbands(length=length, std=std_dev)
            return [
                BollingerBandsResult.from_series(Timestamp(int(idx)), row.to_dict())
                for idx, row in bb.iterrows()
                if not row.isna().any()
            ]

        return await self._calculate_with_retry('BB', _calc)

    @async_ttl_cache()
    async def calculate_macd(self,
                             klines: List[KlineData],
                             fast: int = 12,
                             slow: int = 26,
                             signal: int = 9,
                             ) -> List[MACDResult]:
        """Calculate MACD"""
        async def _calc() -> List[MACDResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            macd = df.ta.macd(fast=fast, slow=slow, signal=signal)
            return [
                MACDResult.from_series(Timestamp(int(idx)), row.to_dict())
                for idx, row in macd.iterrows()
                if not row.isna().any()
            ]

        return await self._calculate_with_retry('MACD', _calc)

    @async_ttl_cache()
    async def calculate_sma(self,
                            klines: List[KlineData],
                            period: int = 20
                            ) -> List[MAResult]:
        """Calculate Simple Moving Average"""
        async def _calc() -> List[MAResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            sma = df.ta.sma(length=period)
            return [
                MAResult.from_series(Timestamp(int(idx)), value)
                for idx, value in sma.items()
                if not pd.isna(value)
            ]

        return await self._calculate_with_retry('SMA', _calc)

    @async_ttl_cache()
    async def calculate_ema(self,
                            klines: List[KlineData],
                            period: int = 20) -> List[MAResult]:
        """Calculate Exponential Moving Average"""
        async def _calc() -> List[MAResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            ema = df.ta.ema(length=period)
            return [
                MAResult.from_series(Timestamp(int(idx)), value)
                for idx, value in ema.items()
                if not pd.isna(value)
            ]

        return await self._calculate_with_retry('EMA', _calc)

    @async_ttl_cache()
    async def calculate_obv(self, klines: List[KlineData]) -> List[OBVResult]:
        """Calculate On Balance Volume"""
        async def _calc() -> List[OBVResult]:
            self._validate_klines(klines)
            df = self._prepare_dataframe(klines)

            obv = df.ta.obv()
            return [
                OBVResult.from_series(Timestamp(int(idx)), value)
                for idx, value in obv.items()
                if not pd.isna(value)
            ]
        return await self._calculate_with_retry('OBV', _calc)