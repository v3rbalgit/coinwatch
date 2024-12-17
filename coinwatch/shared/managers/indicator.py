import polars as pl
import polars_talib as plta
from typing import List
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from shared.core.exceptions import ValidationError
from shared.core.models import (
    KlineData,
    RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
)
from shared.utils.logger import LoggerSetup
from shared.utils.cache import RedisCache, redis_cached
logger = LoggerSetup.setup(__name__)


class IndicatorManager:
    """
    Manages technical indicator calculations using polars-talib.

    This manager interfaces with TimeframeManager to get price data
    and calculates various technical indicators with proper caching
    and validation.
    """

    def __init__(self, redis_url: str):
        """
        Initialize indicator manager with Redis caching.

        Args:
            redis_url: Redis connection URL
        """
        self.cache = RedisCache(redis_url, namespace="indicators")

    def _validate_klines(self, klines: List[KlineData]) -> None:
        """Validate kline data for indicator calculation"""
        if not klines:
            raise ValidationError("No kline data provided")
        if len(klines) < 30:
            raise ValidationError(
                f"Insufficient kline data. Need at least 30 candles, got {len(klines)}"
            )

    def _prepare_dataframe(self, klines: List[KlineData]) -> pl.DataFrame:
        """Convert KlineData to Polars DataFrame"""
        return pl.DataFrame({
            'timestamp': [k.timestamp for k in klines],
            'open': [float(k.open_price) for k in klines],
            'high': [float(k.high_price) for k in klines],
            'low': [float(k.low_price) for k in klines],
            'close': [float(k.close_price) for k in klines],
            'volume': [float(k.volume) for k in klines]
        }).with_columns(pl.col('timestamp').cast(pl.Int64))

    @redis_cached[List[RSIResult]](ttl=60)  # Fast indicator, shorter TTL
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)  # INFO level
    )
    async def calculate_rsi(self,
                          klines: List[KlineData],
                          length: int = 14) -> List[RSIResult]:
        """Calculate Relative Strength Index"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_rsi = df.with_columns(plta.rsi().alias("rsi"))

        return [
            RSIResult.from_series(
                int(row['timestamp']),
                float(row['rsi']),
                length=length
            )
            for row in df_with_rsi.filter(pl.col('rsi').is_not_null()).iter_rows(named=True)
        ]

    @redis_cached[List[BollingerBandsResult]](ttl=300)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)
    )
    async def calculate_bollinger_bands(self,
                                     klines: List[KlineData],
                                     length: int = 20,
                                     std_dev: float = 2.0) -> List[BollingerBandsResult]:
        """Calculate Bollinger Bands"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        bbands = df.with_columns([
            plta.bbands(timeperiod=length, nbdevup=std_dev, nbdevdn=std_dev).struct.field("upperband").alias("upper"),
            plta.bbands(timeperiod=length, nbdevup=std_dev, nbdevdn=std_dev).struct.field("middleband").alias("middle"),
            plta.bbands(timeperiod=length, nbdevup=std_dev, nbdevdn=std_dev).struct.field("lowerband").alias("lower")
        ])

        return [
            BollingerBandsResult.from_series(
                int(row['timestamp']),
                {
                    'BBL': float(row['bbands']['lower']),
                    'BBM': float(row['bbands']['middle']),
                    'BBU': float(row['bbands']['upper']),
                    'BBB': float((row['bbands']['upper'] - row['bbands']['lower']) / row['bbands']['middle'])
                }
            )
            for row in bbands.filter(pl.col('bbands').is_not_null()).iter_rows(named=True)
        ]

    @redis_cached[List[MACDResult]](ttl=300)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)
    )
    async def calculate_macd(self,
                           klines: List[KlineData],
                           fast: int = 12,
                           slow: int = 26,
                           signal: int = 9) -> List[MACDResult]:
        """Calculate MACD"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_macd = df.with_columns([
            plta.macd(
                fastperiod=fast,
                slowperiod=slow,
                signalperiod=signal
            ).struct.field("macd").alias("macd"),
            plta.macd(
                fastperiod=fast,
                slowperiod=slow,
                signalperiod=signal
            ).struct.field("macdsignal").alias("macdsignal"),
            plta.macd(
                fastperiod=fast,
                slowperiod=slow,
                signalperiod=signal
            ).struct.field("macdhist").alias("macdhist")
        ])

        return [
            MACDResult.from_series(
                int(row['timestamp']),
                {
                    f'MACD_{fast}_{slow}_{signal}': float(row['macd']['macd']),
                    f'MACDs_{fast}_{slow}_{signal}': float(row['macd']['macdsignal']),
                    f'MACDh_{fast}_{slow}_{signal}': float(row['macd']['macdhist'])
                }
            )
            for row in df_with_macd.filter(pl.col('macd').is_not_null()).iter_rows(named=True)
        ]

    @redis_cached[List[MAResult]](ttl=300)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)
    )
    async def calculate_sma(self,
                          klines: List[KlineData],
                          period: int = 20) -> List[MAResult]:
        """Calculate Simple Moving Average"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_sma = df.with_columns(plta.sma(timeperiod=period).alias("sma"))

        return [
            MAResult.from_series(
                int(row['timestamp']),
                float(row['sma'])
            )
            for row in df_with_sma.filter(pl.col('sma').is_not_null()).iter_rows(named=True)
        ]

    @redis_cached[List[MAResult]](ttl=300)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)
    )
    async def calculate_ema(self,
                          klines: List[KlineData],
                          period: int = 20) -> List[MAResult]:
        """Calculate Exponential Moving Average"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_ema = df.with_columns(plta.ema(timeperiod=period).alias("ema"))

        return [
            MAResult.from_series(
                int(row['timestamp']),
                float(row['ema'])
            )
            for row in df_with_ema.filter(pl.col('ema').is_not_null()).iter_rows(named=True)
        ]

    @redis_cached[List[OBVResult]](ttl=60)  # Fast indicator, shorter TTL
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, pl.exceptions.ComputeError)),
        before_sleep=before_sleep_log(logger, log_level=20)
    )
    async def calculate_obv(self, klines: List[KlineData]) -> List[OBVResult]:
        """Calculate On Balance Volume"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_obv = df.with_columns(plta.obv(pl.col("close"), pl.col("volume")).alias("obv"))

        return [
            OBVResult.from_series(
                int(row['timestamp']),
                float(row['obv'])
            )
            for row in df_with_obv.filter(pl.col('obv').is_not_null()).iter_rows(named=True)
        ]

    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.cache.close()
