from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, ClassVar
from pydantic import (
    BaseModel,
    Field,
    ValidationInfo,
    model_validator,
    field_validator,
    computed_field,
    ValidationError,
    ConfigDict
)

import shared.utils.time as TimeUtils
from .enums import DataSource, Interval

class SymbolInfo(BaseModel):
    """
    Domain model for cryptocurrency trading symbol information.

    Represents a trading symbol on a cryptocurrency exchange with its
    associated metadata and trading parameters.
    """

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        validate_assignment=True,
        json_schema_extra={
            "example": {
                "name": "BTCUSDT",
                "exchange": "bybit",
                "launch_time": 1573776000000,
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "price_scale": 2,
                "tick_size": "0.01",
                "qty_step": "0.001",
                "max_qty": "100",
                "min_notional": "5",
                "max_leverage": "100",
                "funding_interval": 480
            }
        }
    )

    # Constants
    MIN_LAUNCH_TIME: ClassVar[int] = TimeUtils.to_timestamp(
        datetime(2009, 1, 3)  # Bitcoin genesis block date
    )

    # Required fields with validation and metadata
    name: str = Field(
        ...,
        description="Trading symbol name (e.g., 'BTCUSDT')",
        min_length=5,
        max_length=50,
        pattern="^[A-Z0-9]+$",
        examples=["BTCUSDT", "ETHUSDT"]
    )

    exchange: str = Field(
        ...,
        description="Exchange name",
        min_length=1,
        max_length=20,
        pattern="^[a-z0-9_]+$",
        examples=["bybit", "binance"]
    )

    launch_time: int = Field(
        description="Symbol launch timestamp in milliseconds",
        examples=[1573776000000]
    )

    base_asset: str = Field(
        ...,
        description="Base asset symbol (e.g., 'BTC')",
        min_length=1,
        max_length=20,
        pattern="^[A-Z0-9]+$",
        examples=["BTC", "ETH"]
    )

    quote_asset: str = Field(
        ...,
        description="Quote asset symbol (e.g., 'USDT')",
        min_length=1,
        max_length=10,
        pattern="^[A-Z0-9]+$",
        examples=["USDT", "USD"]
    )

    price_scale: int = Field(
        ...,
        description="Number of decimal places for price",
        ge=0,
        le=18,
        examples=[2]
    )

    tick_size: str = Field(
        ...,
        description="Minimum price movement",
        examples=["0.01"]
    )

    qty_step: str = Field(
        ...,
        description="Minimum quantity movement",
        examples=["0.001"]
    )

    max_qty: str = Field(
        ...,
        description="Maximum order quantity",
        examples=["100"]
    )

    min_notional: str = Field(
        ...,
        description="Minimum order value in quote currency",
        examples=["5"]
    )

    max_leverage: str | None = Field(
        None,
        description="Maximum allowed leverage",
        examples=["100"]
    )

    funding_interval: int | None = Field(
        None,
        description="Funding interval in minutes",
        examples=[480]
    )

    @field_validator('name')
    def validate_name(value: Any, info: ValidationInfo) -> str:
        """Validate symbol name matches base/quote convention if possible"""
        data = info.data
        if 'base_asset' in data and 'quote_asset' in data:
            expected = f"{data['base_asset']}{data['quote_asset']}"
            if value != expected:
                raise ValidationError(f'Expected {expected}, got {value}')
        return value

    @field_validator('launch_time')
    def validate_launch_time(value: Any) -> int:
        """Validate launch time is within acceptable range"""
        if value < SymbolInfo.MIN_LAUNCH_TIME:
            raise ValidationError(f"Launch time cannot be before {SymbolInfo.MIN_LAUNCH_TIME}")

        max_time = TimeUtils.get_current_timestamp() + (24 * 60 * 60 * 1000)
        if value > max_time:
            raise ValidationError(f"Launch time cannot be more than 1 day in future")

        return value

    @computed_field
    @property
    def token_name(self) -> str:
        """Lowercase base asset name"""
        return self.base_asset.lower()

    @computed_field
    @property
    def trading_pair(self) -> str:
        """Trading pair in format BASE/QUOTE"""
        return f"{self.base_asset}/{self.quote_asset}"

    def check_retention_time(self, retention_days: int) -> 'SymbolInfo':
        """
        Create new instance with adjusted launch time based on retention period.

        Args:
            retention_days: Number of days to retain data

        Returns:
            New SymbolInfo instance with adjusted launch time
        """
        if retention_days <= 0:
            return self

        retention_start = TimeUtils.get_current_timestamp() - (retention_days * 24 * 60 * 60 * 1000)
        new_launch_time = max(self.launch_time, retention_start)

        return self.model_copy(update={'launch_time': new_launch_time})

    def __hash__(self) -> int:
        """Hash based on name and exchange which uniquely identify a symbol"""
        return hash((self.name, self.exchange))

    def __eq__(self, other: object) -> bool:
        """Equality based on name and exchange"""
        if not isinstance(other, SymbolInfo):
            return NotImplemented
        return (self.name == other.name and
                self.exchange == other.exchange)

    def __lt__(self, other: 'SymbolInfo') -> bool:
        """Enable sorting of symbols by name and exchange"""
        if not isinstance(other, SymbolInfo):
            return NotImplemented
        return (self.name, self.exchange) < (other.name, other.exchange)

    def __str__(self) -> str:
        """Human-readable string representation"""
        return f"{self.name} ({self.exchange})"

    def __format__(self, format_spec: str) -> str:
        """
        Support string formatting

        Format specs:
            'short': Symbol name and exchange
            'pair': Trading pair format (BASE/QUOTE)
            default: Detailed representation
        """
        if format_spec == 'short':
            return self.__str__()
        if format_spec == 'pair':
            return self.trading_pair
        return self.__repr__()

    def __bool__(self) -> bool:
        """Consider symbol valid if it has a name and exchange"""
        return bool(self.name and self.exchange)


class KlineData(BaseModel):
    """
    Domain model for kline data with built-in validation

    Attributes:
        timestamp: Unix timestamp in milliseconds
        open_price: Opening price of the period
        high_price: Highest price during the period
        low_price: Lowest price during the period
        close_price: Closing price of the period
        volume: Trading volume during the period
        turnover: Trading turnover during the period
        interval: Trading interval
    """

    # Model configuration
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        json_schema_extra={
            "example": {
                "timestamp": 1635724800000,
                "open_price": "60000.0",
                "high_price": "61000.0",
                "low_price": "59000.0",
                "close_price": "60500.0",
                "volume": "1000000.0",
                "turnover": "1000000.0",
                "interval": "60"
            }
        }
    )

    # Class variables (constants)
    BYBIT_LAUNCH_DATE: ClassVar[int] = TimeUtils.to_timestamp(
        datetime(2019, 11, 1, tzinfo=timezone.utc)
    )
    MAX_FUTURE_TOLERANCE: ClassVar[int] = 60  # seconds

    # Fields with validation and metadata
    timestamp: int = Field(
        ...,  # ... means required
        description="Unix timestamp in milliseconds",
        examples=[1635724800000]
    )
    open_price: Decimal = Field(
        ...,
        description="Opening price of the period",
        examples=["60000.0"]
    )
    high_price: Decimal = Field(
        ...,
        description="Highest price during the period",
        examples=["61000.0"]
    )
    low_price: Decimal = Field(
        ...,
        description="Lowest price during the period",
        examples=["59000.0"]
    )
    close_price: Decimal = Field(
        ...,
        description="Closing price of the period",
        examples=["60500.0"]
    )
    volume: Decimal = Field(
        ...,
        description="Trading volume during the period",
        ge=0,
        examples=["1000000.0"]
    )
    turnover: Decimal = Field(
        ...,
        description="Trading turnover during the period",
        ge=0,
        examples=["1000000.0"]
    )
    interval: Interval = Field(
        ...,
        description="Trading interval"
    )

    @classmethod
    def quantize_decimal(cls, value: str | float, precision: int) -> Decimal:
        """
        Quantize a decimal to the specified precision

        Args:
            value: Value to quantize
            precision: Number of decimal places

        Returns:
            Quantized Decimal value
        """
        decimal_value = Decimal(str(value))
        return decimal_value.quantize(
            Decimal(f'0.{"0" * precision}'),
            rounding=ROUND_HALF_UP
        )

    @classmethod
    def from_raw_data(cls,
                     timestamp: int,
                     open_price: str | float,
                     high_price: str | float,
                     low_price: str | float,
                     close_price: str | float,
                     volume: str | float,
                     turnover: str | float,
                     interval: Interval,
                     symbol: SymbolInfo) -> 'KlineData':
        """
        Create a new KlineData instance from raw data with proper precision

        Args:
            timestamp: Unix timestamp in milliseconds
            open_price: Opening price
            high_price: Highest price
            low_price: Lowest price
            close_price: Closing price
            volume: Trading volume
            turnover: Trading turnover
            interval: Trading interval
            symbol: Symbol info for precision settings

        Returns:
            New KlineData instance with quantized values
        """
        return cls(
            timestamp=timestamp,
            open_price=cls.quantize_decimal(open_price, symbol.price_scale),
            high_price=cls.quantize_decimal(high_price, symbol.price_scale),
            low_price=cls.quantize_decimal(low_price, symbol.price_scale),
            close_price=cls.quantize_decimal(close_price, symbol.price_scale),
            volume=cls.quantize_decimal(volume, symbol.price_scale),
            turnover=cls.quantize_decimal(turnover, symbol.price_scale),
            interval=interval
        )

    @field_validator('timestamp')
    def validate_timestamp(value: Any) -> int:
        """Validate timestamp is within acceptable range"""
        if len(str(abs(value))) != 13:
            raise ValidationError(
                'Invalid timestamp length',
                {'timestamp': f'Must be 13 digits, got {len(str(abs(value)))}'}
            )

        if value < KlineData.BYBIT_LAUNCH_DATE:
            launch_date = TimeUtils.from_timestamp(KlineData.BYBIT_LAUNCH_DATE)
            raise ValidationError(
                'Invalid timestamp range',
                {'timestamp': f'Cannot be before exchange launch ({launch_date})'}
            )

        current_time = TimeUtils.get_current_timestamp()
        dt = TimeUtils.from_timestamp(value)
        max_allowed = TimeUtils.from_timestamp(current_time) + timedelta(
            seconds=KlineData.MAX_FUTURE_TOLERANCE
        )

        if dt > max_allowed:
            raise ValidationError(
                'Future timestamp',
                {'timestamp': f'Cannot be in future: {dt} > {max_allowed}'}
            )

        return value

    @field_validator('open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover')
    def validate_numeric(value: Any, info: Any) -> Decimal:
        """Validate and convert numeric values to Decimal"""
        try:
            decimal_value = Decimal(str(value))
            if decimal_value < 0:
                raise ValidationError(
                    'Negative value',
                    {info.field_name: f'Must be non-negative, got {value}'}
                )
            return decimal_value
        except (ValueError, TypeError, InvalidOperation):
            raise ValidationError(
                'Invalid decimal',
                {info.field_name: f'Cannot convert {value} to Decimal'}
            )

    @model_validator(mode='after')
    def validate_price_relationships(self) -> 'KlineData':
        """Validate OHLC price relationships follow market logic"""
        if not (self.low_price <= min(self.open_price, self.close_price) <=
                max(self.open_price, self.close_price) <= self.high_price):
            raise ValidationError(
                'Invalid OHLC relationships',
                {
                    'prices': {
                        'open': self.open_price,
                        'high': self.high_price,
                        'low': self.low_price,
                        'close': self.close_price,
                        'message': 'Price relationships must follow: low ≤ open/close ≤ high'
                    }
                }
            )
        return self

# Technical Indicator Models
class RSIResult(BaseModel):
    """Relative Strength Index result"""
    timestamp: int
    value: Decimal
    length: int = 14

    @classmethod
    def from_series(cls, timestamp: int, value: float, length: int = 14) -> 'RSIResult':
        # RSI column name format: 'RSI_14'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value)),
            length=length
        )

class MACDResult(BaseModel):
    """MACD result"""
    timestamp: int
    macd: Decimal
    signal: Decimal
    histogram: Decimal

    @classmethod
    def from_series(cls, timestamp: int, macd_dict: dict[str, float],
                   fast: int = 12, slow: int = 26, signal: int = 9) -> 'MACDResult':
        # MACD column names format: 'MACD_12_26_9', 'MACDs_12_26_9', 'MACDh_12_26_9'
        suffix = f"_{fast}_{slow}_{signal}"
        return cls(
            timestamp=timestamp,
            macd=Decimal(str(macd_dict[f'MACD{suffix}'])),
            signal=Decimal(str(macd_dict[f'MACDs{suffix}'])),
            histogram=Decimal(str(macd_dict[f'MACDh{suffix}']))
        )

class BollingerBandsResult(BaseModel):
    """Bollinger Bands result"""
    timestamp: int
    upper: Decimal
    middle: Decimal
    lower: Decimal
    bandwidth: Decimal

    @classmethod
    def from_series(cls, timestamp: int, bb_dict: dict[str, float],
                   length: int = 20, std: float = 2.0) -> 'BollingerBandsResult':
        # BB column names format: 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'BBB_20_2.0'
        suffix = f"_{length}_{std}.0"
        return cls(
            timestamp=timestamp,
            upper=Decimal(str(bb_dict[f'BBU{suffix}'])),
            middle=Decimal(str(bb_dict[f'BBM{suffix}'])),
            lower=Decimal(str(bb_dict[f'BBL{suffix}'])),
            bandwidth=Decimal(str(bb_dict[f'BBB{suffix}']))
        )

class MAResult(BaseModel):
    """Moving Average result"""
    timestamp: int
    value: Decimal
    length: int = 20

    @classmethod
    def from_series(cls, timestamp: int, value: float, length: int = 20) -> 'MAResult':
        # MA column names format: 'SMA_20' or 'EMA_20'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value)),
            length=length
        )

class OBVResult(BaseModel):
    """On Balance Volume result"""
    timestamp: int
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: int, value: float) -> 'OBVResult':
        # OBV doesn't have parameters, column name is just 'OBV'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value))
        )

# Platform Model
@dataclass
class Platform:
    """Platform information for a token"""
    platform_id: str
    contract_address: str

# Metadata Model
@dataclass
class Metadata:
    """Domain model for token metadata"""
    id: str
    symbol: SymbolInfo
    name: str
    description: str
    market_cap_rank: int
    categories: list[str]
    updated_at: int
    launch_time: int | None
    hashing_algorithm: str | None
    website: str | None
    whitepaper: str | None
    reddit: str | None
    twitter: str | None
    telegram: str | None
    github: str | None
    images: dict[str, str]
    data_source: DataSource
    platforms: list[Platform] = field(default_factory=list)

    def to_dict(self) -> dict[str, str | list[str] | int | datetime | None]:
        """Convert metadata to database-friendly dictionary"""
        return {
            "id": self.id,
            "symbol": self.symbol.name,
            "name": self.name,
            "description": self.description,
            "market_cap_rank": int(self.market_cap_rank) if self.market_cap_rank else None,
            "categories": self.categories,
            "launch_time": TimeUtils.from_timestamp(self.launch_time) if self.launch_time else None,
            "hashing_algorithm": self.hashing_algorithm,
            "website": self.website,
            "whitepaper": self.whitepaper,
            "reddit": self.reddit,
            "twitter": self.twitter,
            "telegram": self.telegram,
            "github": self.github,
            "image_thumb": self.images.get('thumb'),
            "image_small": self.images.get('small'),
            "image_large": self.images.get('large'),
            "updated_at": TimeUtils.from_timestamp(self.updated_at),
            "data_source": self.data_source.value
        }

# Market Metrics Model
@dataclass
class MarketMetrics:
    """Market metrics for a token"""
    id: str  # CoinGecko ID
    symbol: SymbolInfo
    current_price: Decimal
    market_cap: Decimal | None
    market_cap_rank: int | None
    fully_diluted_valuation: Decimal | None
    total_volume: Decimal
    high_24h: Decimal | None
    low_24h: Decimal | None
    price_change_24h: Decimal | None
    price_change_percentage_24h: Decimal | None
    market_cap_change_24h: Decimal | None
    market_cap_change_percentage_24h: Decimal | None
    circulating_supply: Decimal | None
    total_supply: Decimal | None
    max_supply: Decimal | None
    ath: Decimal | None
    ath_change_percentage: Decimal | None
    ath_date: str | None
    atl: Decimal | None
    atl_change_percentage: Decimal | None
    atl_date: str | None
    updated_at: int
    data_source: DataSource

    def to_dict(self) -> dict[str, str | int | float | datetime | None]:
        """Convert to dictionary for database storage"""
        return {
            'id': self.id,
            'symbol': self.symbol.name,
            'current_price': float(self.current_price),
            'market_cap': float(self.market_cap) if self.market_cap else None,
            'market_cap_rank': self.market_cap_rank,
            'fully_diluted_valuation': float(self.fully_diluted_valuation) if self.fully_diluted_valuation else None,
            'total_volume': float(self.total_volume),
            'high_24h': float(self.high_24h) if self.high_24h else None,
            'low_24h': float(self.low_24h) if self.low_24h else None,
            'price_change_24h': float(self.price_change_24h) if self.price_change_24h else None,
            'price_change_percentage_24h': float(self.price_change_percentage_24h) if self.price_change_percentage_24h else None,
            'market_cap_change_24h': float(self.market_cap_change_24h) if self.market_cap_change_24h else None,
            'market_cap_change_percentage_24h': float(self.market_cap_change_percentage_24h) if self.market_cap_change_percentage_24h else None,
            'circulating_supply': float(self.circulating_supply) if self.circulating_supply else None,
            'total_supply': float(self.total_supply) if self.total_supply else None,
            'max_supply': float(self.max_supply) if self.max_supply else None,
            'ath': float(self.ath) if self.ath else None,
            'ath_change_percentage': float(self.ath_change_percentage) if self.ath_change_percentage else None,
            'ath_date': self.ath_date,
            'atl': float(self.atl) if self.atl else None,
            'atl_change_percentage': float(self.atl_change_percentage) if self.atl_change_percentage else None,
            'atl_date': self.atl_date,
            'updated_at': TimeUtils.from_timestamp(self.updated_at),
            'data_source': self.data_source.value
        }

# Sentiment Metrics Model
@dataclass
class SentimentMetrics:
    """Social media sentiment metrics for a token"""
    id: str  # CoinGecko ID
    symbol: SymbolInfo

    # Twitter metrics
    twitter_followers: int | None
    twitter_following: int | None
    twitter_posts_24h: int | None
    twitter_engagement_rate: Decimal | None
    twitter_sentiment_score: Decimal | None

    # Reddit metrics
    reddit_subscribers: int | None
    reddit_active_users: int | None
    reddit_posts_24h: int | None
    reddit_comments_24h: int | None
    reddit_sentiment_score: Decimal | None

    # Telegram metrics
    telegram_members: int | None
    telegram_online_members: int | None
    telegram_messages_24h: int | None
    telegram_sentiment_score: Decimal | None

    # Aggregated metrics
    overall_sentiment_score: Decimal  # Weighted average of platform sentiment scores
    social_score: Decimal  # Weighted engagement score

    updated_at: int
    data_source: DataSource

    def to_dict(self) -> dict[str, str | int | float | datetime | None]:
        """Convert to dictionary for database storage"""
        return {
            'id': self.id,
            'symbol': self.symbol.name,
            'twitter_followers': self.twitter_followers,
            'twitter_following': self.twitter_following,
            'twitter_posts_24h': self.twitter_posts_24h,
            'twitter_engagement_rate': float(self.twitter_engagement_rate) if self.twitter_engagement_rate else None,
            'twitter_sentiment_score': float(self.twitter_sentiment_score) if self.twitter_sentiment_score else None,
            'reddit_subscribers': self.reddit_subscribers,
            'reddit_active_users': self.reddit_active_users,
            'reddit_posts_24h': self.reddit_posts_24h,
            'reddit_comments_24h': self.reddit_comments_24h,
            'reddit_sentiment_score': float(self.reddit_sentiment_score) if self.reddit_sentiment_score else None,
            'telegram_members': self.telegram_members,
            'telegram_online_members': self.telegram_online_members,
            'telegram_messages_24h': self.telegram_messages_24h,
            'telegram_sentiment_score': float(self.telegram_sentiment_score) if self.telegram_sentiment_score else None,
            'overall_sentiment_score': float(self.overall_sentiment_score),
            'social_score': float(self.social_score),
            'updated_at': TimeUtils.from_timestamp(self.updated_at),
            'data_source': self.data_source.value
        }
