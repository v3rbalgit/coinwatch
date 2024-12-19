import polars as pl
import polars_talib as plta

from shared.core.exceptions import ValidationError
from shared.core.models import (
    KlineModel,
    RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
)
from shared.utils.cache import RedisCache, redis_cached


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

    def _validate_klines(self, klines: list[KlineModel]) -> None:
        """Validate kline data for indicator calculation"""
        if not klines:
            raise ValidationError("No kline data provided")
        if len(klines) < 30:
            raise ValidationError(
                f"Insufficient kline data. Need at least 30 candles, got {len(klines)}"
            )

    def _prepare_dataframe(self, klines: list[KlineModel]) -> pl.DataFrame:
        """Convert KlineModel to Polars DataFrame"""
        return pl.DataFrame({
            'timestamp': [k.timestamp for k in klines],
            'open': [float(k.open_price) for k in klines],
            'high': [float(k.high_price) for k in klines],
            'low': [float(k.low_price) for k in klines],
            'close': [float(k.close_price) for k in klines],
            'volume': [float(k.volume) for k in klines]
        }).with_columns(pl.col('timestamp').cast(pl.Int64))

    @redis_cached[list[RSIResult]](ttl=60)  # Fast indicator, shorter TTL
    async def calculate_rsi(self,
                          klines: list[KlineModel],
                          length: int = 14) -> list[RSIResult]:
        """Calculate Relative Strength Index"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        # Calculate RSI and filter out NaN values
        df_with_rsi = df.with_columns(
            plta.rsi(pl.col("close"), timeperiod=length).alias(f"RSI_{length}")
        ).filter(
            pl.col(f'RSI_{length}').is_not_null() & pl.col(f'RSI_{length}').is_finite()
        )

        return [
            RSIResult.from_series(
                int(row['timestamp']),
                float(row[f'RSI_{length}']),
                length=length
            )
            for row in df_with_rsi.iter_rows(named=True)
        ]

    @redis_cached[list[BollingerBandsResult]](ttl=300)
    async def calculate_bollinger_bands(self,
                                     klines: list[KlineModel],
                                     length: int = 20,
                                     std_dev: float = 2.0) -> list[BollingerBandsResult]:
        """Calculate Bollinger Bands"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        suffix = f"_{length}_{std_dev}.0"
        bbands = plta.bbands(pl.col("close"), timeperiod=length, nbdevup=std_dev, nbdevdn=std_dev)
        df_with_bb = df.with_columns([
            bbands.struct.field("upperband").alias(f"BBU{suffix}"),
            bbands.struct.field("middleband").alias(f"BBM{suffix}"),
            bbands.struct.field("lowerband").alias(f"BBL{suffix}")
        ])

        # Calculate bandwidth and filter out NaN values
        df_with_bb = df_with_bb.with_columns([
            ((pl.col(f"BBU{suffix}") - pl.col(f"BBL{suffix}")) / pl.col(f"BBM{suffix}")).alias(f"BBB{suffix}")
        ]).filter(
            pl.col(f'BBU{suffix}').is_not_null() &
            pl.col(f'BBM{suffix}').is_not_null() &
            pl.col(f'BBL{suffix}').is_not_null() &
            pl.col(f'BBU{suffix}').is_finite() &
            pl.col(f'BBM{suffix}').is_finite() &
            pl.col(f'BBL{suffix}').is_finite()
        )

        return [
            BollingerBandsResult.from_series(
                int(row['timestamp']),
                {
                    f'BBU{suffix}': float(row[f'BBU{suffix}']),
                    f'BBM{suffix}': float(row[f'BBM{suffix}']),
                    f'BBL{suffix}': float(row[f'BBL{suffix}']),
                    f'BBB{suffix}': float(row[f'BBB{suffix}'])
                },
                length=length,
                std=std_dev
            )
            for row in df_with_bb.iter_rows(named=True)
        ]

    @redis_cached[list[MACDResult]](ttl=300)
    async def calculate_macd(self,
                           klines: list[KlineModel],
                           fast: int = 12,
                           slow: int = 26,
                           signal: int = 9) -> list[MACDResult]:
        """Calculate MACD"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        suffix = f"_{fast}_{slow}_{signal}"
        macd = plta.macd(
            pl.col("close"),
            fastperiod=fast,
            slowperiod=slow,
            signalperiod=signal
        )
        df_with_macd = df.with_columns([
            macd.struct.field("macd").alias(f"MACD{suffix}"),
            macd.struct.field("macdsignal").alias(f"MACDs{suffix}"),
            macd.struct.field("macdhist").alias(f"MACDh{suffix}")
        ]).filter(
            pl.col(f'MACD{suffix}').is_not_null() &
            pl.col(f'MACDs{suffix}').is_not_null() &
            pl.col(f'MACDh{suffix}').is_not_null() &
            pl.col(f'MACD{suffix}').is_finite() &
            pl.col(f'MACDs{suffix}').is_finite() &
            pl.col(f'MACDh{suffix}').is_finite()
        )

        return [
            MACDResult.from_series(
                int(row['timestamp']),
                {
                    f'MACD{suffix}': float(row[f'MACD{suffix}']),
                    f'MACDs{suffix}': float(row[f'MACDs{suffix}']),
                    f'MACDh{suffix}': float(row[f'MACDh{suffix}'])
                },
                fast=fast,
                slow=slow,
                signal=signal
            )
            for row in df_with_macd.iter_rows(named=True)
        ]

    @redis_cached[list[MAResult]](ttl=300)
    async def calculate_sma(self,
                          klines: list[KlineModel],
                          period: int = 20) -> list[MAResult]:
        """Calculate Simple Moving Average"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_sma = df.with_columns(
            plta.sma(pl.col("close"), timeperiod=period).alias(f"SMA_{period}")
        ).filter(
            pl.col(f'SMA_{period}').is_not_null() & pl.col(f'SMA_{period}').is_finite()
        )

        return [
            MAResult.from_series(
                int(row['timestamp']),
                float(row[f'SMA_{period}']),
                length=period
            )
            for row in df_with_sma.iter_rows(named=True)
        ]

    @redis_cached[list[MAResult]](ttl=300)
    async def calculate_ema(self,
                          klines: list[KlineModel],
                          period: int = 20) -> list[MAResult]:
        """Calculate Exponential Moving Average"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_ema = df.with_columns(
            plta.ema(pl.col("close"), timeperiod=period).alias(f"EMA_{period}")
        ).filter(
            pl.col(f'EMA_{period}').is_not_null() & pl.col(f'EMA_{period}').is_finite()
        )

        return [
            MAResult.from_series(
                int(row['timestamp']),
                float(row[f'EMA_{period}']),
                length=period
            )
            for row in df_with_ema.iter_rows(named=True)
        ]

    @redis_cached[list[OBVResult]](ttl=60)  # Fast indicator, shorter TTL
    async def calculate_obv(self, klines: list[KlineModel]) -> list[OBVResult]:
        """Calculate On Balance Volume"""
        self._validate_klines(klines)
        df = self._prepare_dataframe(klines)

        df_with_obv = df.with_columns(
            plta.obv(pl.col("close"), pl.col("volume")).alias("OBV")
        ).filter(
            pl.col('OBV').is_not_null() & pl.col('OBV').is_finite()
        )

        return [
            OBVResult.from_series(
                int(row['timestamp']),
                float(row['OBV'])
            )
            for row in df_with_obv.iter_rows(named=True)
        ]

    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.cache.close()
