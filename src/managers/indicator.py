# src/managers/indicator.py

import asyncio
from enum import Enum
from cachetools import TTLCache
import pandas as pd
import pandas_ta as ta
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, cast

from ..core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ..core.exceptions import ServiceError, ValidationError
from ..core.models import (
    KlineData,
    RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
    )
from ..utils.domain_types import Timestamp
from ..utils.logger import LoggerSetup
from ..utils.time import TimeUtils
from ..utils.retry import RetryConfig, RetryStrategy
from ..utils.cache import AsyncTTLCache, CacheStats, async_ttl_cache

logger = LoggerSetup.setup(__name__)

class CacheAdjustment(Enum):
    """Type of cache adjustment made"""
    TTL = "ttl"
    SIZE = "size"
    BOTH = "both"

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
    """Configuration for all indicator caches"""
    default: CacheConfig = CacheConfig(ttl=300, max_size=1000)  # 5 minutes, 1000 entries
    critical: CacheConfig = CacheConfig(ttl=600, max_size=100)  # 10 minutes, 100 entries
    reduced: CacheConfig = CacheConfig(ttl=450, max_size=500)   # 7.5 minutes, 500 entries

    # Special configurations for sensitive indicators
    fast_indicators: CacheConfig = CacheConfig(ttl=60, max_size=1000)  # 1 minute TTL

    recovery_delay: int = 300  # 5 minutes before attempting recovery
    min_reduction_interval: int = 60  # 1 minute between reductions

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
        self.config = config or IndicatorCacheConfig()

        self._calculation_lock = asyncio.Lock()
        self._last_reduction_ts = Timestamp(0)
        self._last_adjustment: Optional[CacheAdjustment] = None
        self._recovery_task: Optional[asyncio.Task] = None

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

        # Register command handler
        asyncio.create_task(self._register_command_handlers())

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior"""
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            TimeoutError,
            pd.errors.EmptyDataError
        )
        self._retry_strategy.add_non_retryable_error(
            ValidationError
        )
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
        """Apply initial cache configuration to all indicators"""
        # Fast indicators (more sensitive to price changes)
        self._setup_cache(self.calculate_rsi, self.config.fast_indicators)
        self._setup_cache(self.calculate_obv, self.config.fast_indicators)

        # Standard indicators
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
                cache_instance.stats.maxsize = config.max_size
                cache_instance.stats.ttl = config.ttl
                # Create new cache with config
                cache_instance.cache = TTLCache(
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

    async def _register_command_handlers(self) -> None:
        """Register resource management command handler"""
        await self.coordinator.register_handler(
            MarketDataCommand.ADJUST_CACHE,
            self._handle_resource_pressure
        )

    def _should_handle_reduction(self) -> bool:
        """Check if enough time has passed since last reduction"""
        current_ts = TimeUtils.get_current_timestamp()
        elapsed_ms = current_ts - self._last_reduction_ts
        return elapsed_ms >= (self.config.min_reduction_interval * 1000)

    async def _adjust_cache_config(self,
                                 new_config: CacheConfig,
                                 adjustment_type: CacheAdjustment) -> None:
        """
        Adjust cache configuration for all indicators.
        Creates new cache instances with updated parameters while preserving
        valid cached entries.
        """
        async with self._calculation_lock:
            logger.info(
                f"Adjusting cache config: {adjustment_type.value} "
                f"(TTL: {new_config.ttl}s, Size: {new_config.max_size})"
            )
            for method in [
                self.calculate_rsi,
                self.calculate_macd,
                self.calculate_bollinger_bands,
                self.calculate_sma,
                self.calculate_ema,
                self.calculate_obv
            ]:
                cache_instance = self._get_cache_instance(method)
                if cache_instance:
                    old_cache = cache_instance.cache

                    # Create new cache with updated parameters
                    cache_instance.cache = TTLCache(
                        maxsize=new_config.max_size if adjustment_type in (CacheAdjustment.SIZE, CacheAdjustment.BOTH) else old_cache.maxsize,
                        ttl=new_config.ttl if adjustment_type in (CacheAdjustment.TTL, CacheAdjustment.BOTH) else old_cache.ttl
                    )

                    # Transfer still-valid entries
                    for key, value in old_cache.items():
                        cache_instance.cache[key] = value

            # Store adjustment type for recovery
            self._last_adjustment = adjustment_type

            # Start recovery monitoring if not already running
            if not self._recovery_task or self._recovery_task.done():
                self._recovery_task = asyncio.create_task(
                    self._monitor_for_recovery(self._last_reduction_ts)
                )

    async def _monitor_for_recovery(self, reduction_ts: Timestamp) -> None:
        """
        Monitor for conditions that would allow cache recovery.
        """
        try:
            if self._recovery_task and self._recovery_task.cancelled():
                return

            await asyncio.sleep(self.config.recovery_delay)

            # Only recover if no new reductions have occurred
            if reduction_ts == self._last_reduction_ts:
                # First try partial recovery
                await self._adjust_cache_config(
                    self.config.reduced,
                    self._last_adjustment or CacheAdjustment.BOTH
                )

                # Wait another recovery period
                await asyncio.sleep(self.config.recovery_delay)

                # If still stable, restore default configuration
                if reduction_ts == self._last_reduction_ts:
                    await self._adjust_cache_config(
                        self.config.default,
                        self._last_adjustment or CacheAdjustment.BOTH
                    )
                    logger.info("Cache configuration restored to default")
                    self._last_adjustment = None

        except Exception as e:
            logger.error(f"Error during cache recovery: {e}")
        finally:
            self._recovery_task = None

    async def cleanup(self) -> None:
        """Cleanup resources"""
        # Cancel recovery task
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass

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
        """Get statistics for all indicator caches with pressure metrics"""
        stats: Dict[str, Dict[str, Any]] = {}

        for name, method in [
            ('rsi', self.calculate_rsi),
            ('macd', self.calculate_macd),
            ('bollinger', self.calculate_bollinger_bands),
            ('sma', self.calculate_sma),
            ('ema', self.calculate_ema),
            ('obv', self.calculate_obv)
        ]:
            if hasattr(method, 'cache_info'):
                cache_stats = cast(CacheStats, method.cache_info())
                stats[name] = {
                    'stats': cache_stats,
                    'utilization': cache_stats.size / cache_stats.maxsize * 100,
                    'hit_ratio': cache_stats.hit_ratio
                }

        return stats

    # Resource pressure handlers
    async def _handle_resource_pressure(self, command: Command) -> None:
        """Handle resource pressure notifications."""
        error_type = command.params.get('error_type')
        severity = command.params.get('severity', 'warning')
        context = command.params.get('context', {})

        try:
            # Handle recovery notification
            if error_type == 'resource_recovered':
                logger.info("Received resource recovery notification")
                # Let the regular recovery monitoring handle the recovery
                # This avoids potential conflicts with ongoing recovery processes
                return

            # Only process reductions if enough time has passed
            if not self._should_handle_reduction():
                return

            async with self._calculation_lock:
                logger.info(f"Handling resource pressure: {error_type} ({severity})")

                if error_type == 'memory_pressure':
                    await self._handle_memory_pressure(severity, context)
                elif error_type == 'cpu_pressure':
                    await self._handle_cpu_pressure(severity, context)
                elif error_type == 'disk_pressure':
                    await self._handle_disk_pressure(severity, context)

                self._last_reduction_ts = TimeUtils.get_current_timestamp()

        except Exception as e:
            logger.error(f"Error handling resource pressure: {e}")
            raise ServiceError(f"Resource handling failed: {str(e)}")

    async def _handle_memory_pressure(self, severity: str, context: Dict[str, Any]) -> None:
        """Handle memory pressure by reducing cache size"""
        if severity == 'critical':
            await self._adjust_cache_config(
                self.config.critical,
                CacheAdjustment.SIZE
            )
        else:
            await self._adjust_cache_config(
                self.config.reduced,
                CacheAdjustment.SIZE
            )

    async def _handle_cpu_pressure(self, severity: str, context: Dict[str, Any]) -> None:
        """Handle CPU pressure by adjusting TTL"""
        if severity == 'critical':
            await self._adjust_cache_config(
                self.config.critical,
                CacheAdjustment.TTL
            )
        else:
            await self._adjust_cache_config(
                self.config.reduced,
                CacheAdjustment.TTL
            )

    async def _handle_disk_pressure(self, severity: str, context: Dict[str, Any]) -> None:
        """Handle disk pressure by adjusting both TTL and size"""
        if severity == 'critical':
            await self._adjust_cache_config(
                self.config.critical,
                CacheAdjustment.BOTH
            )
        else:
            await self._adjust_cache_config(
                self.config.reduced,
                CacheAdjustment.BOTH
            )

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