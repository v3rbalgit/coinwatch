from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, ClassVar
from pydantic import (
    BaseModel,
    Field,
    ValidationInfo,
    model_validator,
    field_validator,
    ConfigDict
)
from .enums import DataSource, Interval
from shared.utils.time import to_timestamp, from_timestamp, get_current_timestamp, get_current_datetime


# Market data models
class SymbolModel(BaseModel):
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
    MIN_LAUNCH_TIME: ClassVar[int] = to_timestamp(
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
                raise ValueError(f'Expected {expected}, got {value}')
        return value

    @field_validator('launch_time')
    def validate_launch_time(value: Any) -> int:
        """Validate launch time is within acceptable range"""
        if value < SymbolModel.MIN_LAUNCH_TIME:
            raise ValueError(f"Launch time cannot be before {SymbolModel.MIN_LAUNCH_TIME}")

        max_time = get_current_timestamp() + (24 * 60 * 60 * 1000)
        if value > max_time:
            raise ValueError(f"Launch time cannot be more than 1 day in future")

        return value

    @property
    def token_name(self) -> str:
        """Lowercase base asset name"""
        return self.base_asset.lower()

    @property
    def trading_pair(self) -> str:
        """Trading pair in format BASE/QUOTE"""
        return f"{self.base_asset}/{self.quote_asset}"

    def adjust_launch_time(self, retention_days: int) -> 'SymbolModel':
        """
        Create new instance with adjusted launch time based on retention period.

        Args:
            retention_days: Number of days to retain data

        Returns:
            New SymbolModel instance with adjusted launch time
        """
        if retention_days <= 0:
            return self

        retention_start = get_current_timestamp() - (retention_days * 24 * 60 * 60 * 1000)
        new_launch_time = max(self.launch_time, retention_start)

        return self.model_copy(update={'launch_time': new_launch_time})

    def __hash__(self) -> int:
        """Hash based on name and exchange which uniquely identify a symbol"""
        return hash((self.name, self.exchange))

    def __eq__(self, other: object) -> bool:
        """Equality based on name and exchange"""
        if not isinstance(other, SymbolModel):
            return NotImplemented
        return (self.name == other.name and
                self.exchange == other.exchange)

    def __lt__(self, other: 'SymbolModel') -> bool:
        """Enable sorting of symbols by name and exchange"""
        if not isinstance(other, SymbolModel):
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


class KlineModel(BaseModel):
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
    BYBIT_LAUNCH_DATE: ClassVar[int] = to_timestamp(
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

    @property
    def start_time(self) -> str:
        """Timestamp formatted as a datetime string"""
        return from_timestamp(self.timestamp).strftime('%d-%m-%Y %H:%M')

    @property
    def end_time(self) -> str:
        """Calculated end time based on an interval as a datetime string"""
        return from_timestamp(self.timestamp + self.interval.to_milliseconds()).strftime('%d-%m-%Y %H:%M')

    @property
    def time_range(self) -> str:
        """Time range covered by the kline"""
        return f"{self.start_time} - {self.end_time}"

    @classmethod
    def _to_decimal(cls, value: Any) -> Decimal:
        """Convert any numeric input to a normalized Decimal"""
        if value is None:
            raise ValueError("Value cannot be None")
        try:
            if isinstance(value, float):
                # Convert float to string without scientific notation
                value = f"{value:.8f}"
            return Decimal(str(value)).normalize()
        except (ValueError, InvalidOperation) as e:
            raise ValueError(f"Cannot convert {value} to Decimal: {str(e)}")

    @classmethod
    def from_raw_data(cls, data: list[Any], interval: Interval) -> 'KlineModel':
        """
        Create a new KlineModel instance from raw data with proper precision

        Args:
            data (list[Any]): List of price data
            interval (Interval): Trading interval of the price data

        Returns:
            New KlineModel instance with normalized decimal values
        """
        return cls(
            timestamp=int(data[0]),
            open_price=data[1],
            high_price=data[2],
            low_price=data[3],
            close_price=data[4],
            volume=data[5],
            turnover=data[6],
            interval=interval
        )

    @field_validator('open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover', mode='before')
    def validate_and_convert_decimal(cls, value: Any, info: ValidationInfo) -> Decimal:
        """Convert and validate numeric values to Decimal"""
        try:
            decimal_value = cls._to_decimal(value)
            if decimal_value < 0:
                raise ValueError(f'Must be non-negative, got {value}')
            return decimal_value
        except (ValueError, TypeError, InvalidOperation) as e:
            raise ValueError(f'Cannot convert {value} to Decimal: {str(e)}')

    @field_validator('timestamp', mode='before')
    def validate_timestamp(cls, value: Any) -> int:
        """Convert and validate timestamp"""
        if isinstance(value, Decimal):
            value = int(value)
        elif isinstance(value, datetime):
            value = int(value.timestamp() * 1000)

        if len(str(abs(value))) != 13:
            raise ValueError(f'Must be 13 digits, got {len(str(abs(value)))}')

        if value < KlineModel.BYBIT_LAUNCH_DATE:
            launch_date = from_timestamp(KlineModel.BYBIT_LAUNCH_DATE)
            raise ValueError(f'Cannot be before exchange launch ({launch_date})')

        current_time = get_current_timestamp()
        dt = from_timestamp(value)
        max_allowed = from_timestamp(current_time) + timedelta(
            seconds=KlineModel.MAX_FUTURE_TOLERANCE
        )

        if dt > max_allowed:
            raise ValueError(f'Cannot be in future: {dt} > {max_allowed}')

        return value

    @field_validator('interval', mode='before')
    def validate_interval(cls, value: Any) -> Interval:
        """Convert string to Interval enum"""
        if isinstance(value, str):
            try:
                return Interval(value)
            except ValueError:
                raise ValueError(f"Invalid interval value: {value}")
        return value

    @model_validator(mode='after')
    def validate_price_relationships(self) -> 'KlineModel':
        """Validate OHLC price relationships follow market logic"""
        if not (self.low_price <= min(self.open_price, self.close_price) <=
                max(self.open_price, self.close_price) <= self.high_price):
            raise ValueError(
                f'Invalid OHLC relationships: Price relationships must follow: '
                f'low ({self.low_price}) ≤ open ({self.open_price})/close ({self.close_price}) '
                f'≤ high ({self.high_price})'
            )
        return self


# Technical Indicator Models
class IndicatorBase(BaseModel):
    """Base class for all indicator models with common decimal handling"""
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        json_encoders={
            # Convert Decimal to string without scientific notation
            Decimal: lambda v: f"{v:f}"
        }
    )

    @field_validator('*')
    def validate_decimal_fields(cls, value: Any, info: ValidationInfo) -> Any:
        """Convert any numeric field to normalized Decimal"""
        if isinstance(value, (Decimal, int)):
            return value
        if isinstance(value, str) and info.field_name != 'timestamp':
            try:
                # Handle scientific notation by converting through float
                return Decimal(str(float(value))).normalize()
            except (ValueError, InvalidOperation):
                raise ValueError(f'Invalid decimal value: {value}')
        return value

class RSIResult(IndicatorBase):
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

class MACDResult(IndicatorBase):
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

class BollingerBandsResult(IndicatorBase):
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

class MAResult(IndicatorBase):
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

class OBVResult(IndicatorBase):
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


# Fundamental data models
class PlatformModel(BaseModel):
    """Platform information for a token"""
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        json_schema_extra={
            "example": {
                "token_id": "ethereum",
                "platform_id": "ethereum",
                "contract_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
            }
        }
    )

    token_id: str = Field(
        ...,
        description="Token id (lowercase base asset)",
        min_length=1,
        examples=['dexpad','fedoracoin']
    )
    platform_id: str = Field(
        ...,
        description="Platform identifier (e.g., 'ethereum', 'binance-smart-chain')",
        min_length=1,
        examples=["ethereum", "polygon-pos"]
    )
    contract_address: str = Field(
        ...,
        description="Token contract address on the platform",
        min_length=1,
        examples=["0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"]
    )


class MetadataModel(BaseModel):
    """Domain model for token metadata"""
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "description": "Bitcoin is the first successful internet money...",
                "market_cap_rank": 1,
                "categories": ["Cryptocurrency", "Layer 1 (L1)"],
                "launch_time": "2009-01-03T00:00:00Z",
                "hashing_algorithm": "SHA-256",
                "website": "http://www.bitcoin.org",
                "whitepaper": "https://bitcoin.org/bitcoin.pdf",
                "reddit": "https://www.reddit.com/r/Bitcoin/",
                "twitter": "bitcoin",
                "telegram": "",
                "github": "https://github.com/bitcoin/bitcoin",
                "images": {
                    "thumb": "https://example.com/thumb.png",
                    "small": "https://example.com/small.png",
                    "large": "https://example.com/large.png"
                },
                "platforms": [],
                "updated_at": "2024-01-10T00:00:00Z",
                "data_source": "coingecko"
            }
        }
    )

    id: str = Field(..., description="Unique identifier", min_length=1)
    symbol: str = Field(..., description="Token symbol", min_length=1)
    name: str = Field(..., description="Token name", min_length=1)
    description: str = Field(..., description="Token description")
    market_cap_rank: int = Field(..., description="Market cap rank")
    categories: list[str] = Field(default_factory=list, description="Token categories")
    launch_time: datetime | None = Field(None, description="Token launch time")
    hashing_algorithm: str | None = Field(None, description="Mining algorithm")
    website: str | None = Field(None, description="Official website URL")
    whitepaper: str | None = Field(None, description="Whitepaper URL")
    reddit: str | None = Field(None, description="Reddit URL")
    twitter: str | None = Field(None, description="Twitter handle")
    telegram: str | None = Field(None, description="Telegram channel")
    github: str | None = Field(None, description="GitHub repository URL")
    images: dict[str, str] = Field(
        ...,
        description="Token images in different sizes",
        json_schema_extra={
            "example": {
                "thumb": "https://example.com/thumb.png",
                "small": "https://example.com/small.png",
                "large": "https://example.com/large.png"
            }
        },
        exclude=True  # Exclude from serialization for database operations
    )
    platforms: list[PlatformModel] = Field(
        default_factory=list,
        description="List of platforms where token is available",
        exclude=True  # Exclude from serialization for database
    )
    updated_at: datetime = Field(..., description="Last update timestamp")
    data_source: DataSource = Field(..., description="Data source identifier")

    # Database-specific fields
    image_thumb: str = Field(
        default="",
        description="Thumbnail image URL",
        validation_alias="images.thumb"
    )
    image_small: str = Field(
        default="",
        description="Small image URL",
        validation_alias="images.small"
    )
    image_large: str = Field(
        default="",
        description="Large image URL",
        validation_alias="images.large"
    )

    @field_validator('symbol', mode='before')
    def convert_symbol_to_lowercase(cls, v: str) -> str:
        """Convert symbol to lowercase"""
        return v.lower() if isinstance(v, str) else v

    @field_validator('market_cap_rank', mode='before')
    def handle_missing_market_cap_rank(cls, v: Any) -> int:
        """Convert symbol to lowercase"""
        return int(v) if v is not None else -1

    @field_validator('data_source', mode='before')
    def convert_data_source(cls, v: Any) -> DataSource:
        """Convert string to DataSource enum"""
        if isinstance(v, DataSource):
            return v
        return DataSource(v)


class MarketMetricsModel(BaseModel):
    """Market metrics for a token"""
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        json_encoders={
            Decimal: lambda v: f"{v:f}"  # Format decimals without scientific notation
        },
        json_schema_extra={
            "example": {
                "id": "bitcoin",
                "symbol": "btc",
                "current_price": "45000.00",
                "market_cap": "850000000000.00",
                "market_cap_rank": 1,
                "fully_diluted_valuation": "945000000000.00",
                "total_volume": "25000000000.00",
                "high_24h": "46000.00",
                "low_24h": "44000.00",
                "price_change_24h": "1000.00",
                "price_change_percentage_24h": "2.25",
                "market_cap_change_24h": "20000000000.00",
                "market_cap_change_percentage_24h": "2.35",
                "circulating_supply": "19000000.00",
                "total_supply": "21000000.00",
                "max_supply": "21000000.00",
                "ath": "69000.00",
                "ath_change_percentage": "-34.78",
                "ath_date": "2021-11-10T14:24:11.849Z",
                "atl": "67.81",
                "atl_change_percentage": "66256.00",
                "atl_date": "2013-07-06T00:00:00.000Z",
                "updated_at": "2024-01-10T00:00:00Z",
                "data_source": "coingecko"
            }
        }
    )

    # Identifiers (required fields)
    id: str = Field(..., description="Unique identifier", min_length=1)
    symbol: str = Field(..., description="Token symbol", min_length=1)

    # Price fields (all nullable)
    current_price: Decimal | None = Field(default=None, description="Current token price", max_digits=20, decimal_places=10)
    high_24h: Decimal | None = Field(default=None, description="24h high", max_digits=20, decimal_places=10)
    low_24h: Decimal | None = Field(default=None, description="24h low", max_digits=20, decimal_places=10)
    price_change_24h: Decimal | None = Field(default=None, description="24h price change", max_digits=20, decimal_places=10)
    ath: Decimal | None = Field(default=None, description="All-time high price", max_digits=20, decimal_places=10)
    atl: Decimal | None = Field(default=None, description="All-time low price", max_digits=20, decimal_places=10)

    # Percentage fields (all nullable)
    price_change_percentage_24h: Decimal | None = Field(default=None, description="24h price change percentage", max_digits=16, decimal_places=4)
    market_cap_change_percentage_24h: Decimal | None = Field(default=None, description="24h market cap change percentage", max_digits=16, decimal_places=4)
    ath_change_percentage: Decimal | None = Field(default=None, description="Change from ATH percentage", max_digits=16, decimal_places=4)
    atl_change_percentage: Decimal | None = Field(default=None, description="Change from ATL percentage", max_digits=16, decimal_places=4)

    # Large number fields (all nullable)
    market_cap: Decimal | None = Field(default=None, description="Market capitalization", max_digits=30, decimal_places=2)
    fully_diluted_valuation: Decimal | None = Field(default=None, description="Fully diluted valuation", max_digits=30, decimal_places=2)
    total_volume: Decimal | None = Field(default=None, description="24h trading volume", max_digits=30, decimal_places=2)
    market_cap_change_24h: Decimal | None = Field(default=None, description="24h market cap change", max_digits=30, decimal_places=2)

    # Supply fields (all nullable)
    circulating_supply: Decimal | None = Field(default=None, description="Circulating supply", max_digits=30, decimal_places=8)
    total_supply: Decimal | None = Field(default=None, description="Total supply", max_digits=30, decimal_places=8)
    max_supply: Decimal | None = Field(default=None, description="Maximum supply", max_digits=30, decimal_places=8)

    # Other fields (all nullable except required management fields)
    market_cap_rank: int | None = Field(default=None, description="Market cap rank")
    ath_date: datetime | None = Field(default=None, description="ATH date")
    atl_date: datetime | None = Field(default=None, description="ATL date")

    # Management fields (required)
    updated_at: datetime = Field(..., description="Last update timestamp")
    data_source: DataSource = Field(..., description="Data source identifier")

    @classmethod
    def _to_decimal(cls, value: Any) -> Decimal:
        """Convert any numeric value to Decimal safely"""
        if value is None:
            raise ValueError("Value cannot be None")
        try:
            if isinstance(value, float):
                # Convert float to string without scientific notation
                value = f"{value:.10f}"
            return Decimal(str(value)).normalize()
        except (ValueError, InvalidOperation) as e:
            raise ValueError(f"Cannot convert {value} to Decimal: {str(e)}")

    @classmethod
    def _parse_date(cls, date_str: str) -> datetime:
        """Parse ISO datetime string to datetime object"""
        try:
            # Handle various ISO formats
            if 'Z' in date_str:
                date_str = date_str.replace('Z', '+00:00')
            elif '+' not in date_str and '-' not in date_str[10:]:
                date_str = f"{date_str}+00:00"
            return datetime.fromisoformat(date_str)
        except (ValueError, AttributeError):
            return get_current_datetime()

    @classmethod
    def from_raw_data(cls, data: dict[str, Any]) -> 'MarketMetricsModel':
        """Create instance from raw CoinGecko API response"""
        def normalize_decimal(value: Any, precision: int = 10, max_digits: int = 20, default: str = '0') -> Decimal:
            """Convert and normalize any numeric value to Decimal within constraints"""
            if value is None:
                return Decimal(default)
            try:
                # Convert to float first to handle scientific notation
                if isinstance(value, str):
                    value = value.replace(',', '').strip()
                    if value.endswith('%'):
                        value = value[:-1]
                    if not value:
                        return Decimal(default)

                try:
                    if isinstance(value, (int, Decimal)):
                        float_val = float(value)
                    else:
                        float_val = float(value)
                except (ValueError, TypeError):
                    return Decimal(default)

                # Handle invalid values
                if (float_val == float('inf') or float_val == float('-inf')
                    or float_val != float_val):  # Check for inf/-inf/nan
                    return Decimal(default)

                # Convert to decimal and normalize
                decimal_val = Decimal(str(float_val)).normalize()

                # Handle very small numbers
                if abs(decimal_val) < Decimal('1e-10'):
                    return Decimal('0')

                # Handle large numbers by rounding to millions if needed
                if abs(decimal_val) > Decimal('1e12'):
                    decimal_val = (decimal_val / Decimal('1000000')).quantize(Decimal('1')) * Decimal('1000000')

                # Apply precision
                scale = Decimal('0.' + '0' * (precision - 1) + '1')
                return decimal_val.quantize(scale, rounding=ROUND_HALF_UP)

            except (ValueError, InvalidOperation, TypeError, OverflowError) as e:
                print(f"Warning: Could not convert value '{value}' ({type(value)}) to Decimal: {str(e)}")
                return Decimal(default)

        def round_pct(value: Any, default: str = '0') -> Decimal:
            """Round percentage values to 4 decimal places"""
            return normalize_decimal(value, precision=4, max_digits=16, default=default)

        # Process price fields with appropriate constraints
        def process_price(value: Any) -> Decimal:
            result = normalize_decimal(value, precision=10, max_digits=20, default='0.0000000001')
            return max(result, Decimal('0.0000000001'))  # Ensure minimum price

        # Process supply fields with appropriate constraints
        def process_supply(value: Any) -> Decimal:
            return normalize_decimal(value, precision=8, max_digits=20, default='0')

        # Process market cap and volume fields
        def process_market_value(value: Any) -> Decimal:
            return normalize_decimal(value, precision=2, max_digits=30, default='0')

        return cls(
            id=data.get('id', ''),
            symbol=data.get('symbol', ''),
            current_price=None if data.get('current_price') is None else process_price(data['current_price']),
            market_cap=None if data.get('market_cap') is None else process_market_value(data['market_cap']),
            market_cap_rank=data.get('market_cap_rank'),
            fully_diluted_valuation=None if data.get('fully_diluted_valuation') is None else process_market_value(data['fully_diluted_valuation']),
            total_volume=None if data.get('total_volume') is None else process_market_value(data['total_volume']),
            high_24h=None if data.get('high_24h') is None else process_price(data['high_24h']),
            low_24h=None if data.get('low_24h') is None else process_price(data['low_24h']),
            price_change_24h=None if data.get('price_change_24h') is None else process_price(data['price_change_24h']),
            price_change_percentage_24h=None if data.get('price_change_percentage_24h') is None else round_pct(data['price_change_percentage_24h']),
            market_cap_change_24h=None if data.get('market_cap_change_24h') is None else process_market_value(data['market_cap_change_24h']),
            market_cap_change_percentage_24h=None if data.get('market_cap_change_percentage_24h') is None else round_pct(data['market_cap_change_percentage_24h']),
            circulating_supply=None if data.get('circulating_supply') is None else process_supply(data['circulating_supply']),
            total_supply=None if data.get('total_supply') is None else process_supply(data['total_supply']),
            max_supply=None if data.get('max_supply') is None else process_supply(data['max_supply']),
            ath=None if data.get('ath') is None else process_price(data['ath']),
            ath_change_percentage=None if data.get('ath_change_percentage') is None else round_pct(data['ath_change_percentage']),
            ath_date=None if data.get('ath_date') is None else cls._parse_date(data['ath_date']),
            atl=None if data.get('atl') is None else process_price(data.get('atl')),
            atl_change_percentage=None if data.get('atl_change_percentage') is None else round_pct(data['atl_change_percentage']),
            atl_date=None if data.get('atl_date') is None else cls._parse_date(data['atl_date']),
            updated_at=get_current_datetime(),
            data_source=DataSource.COINGECKO
        )

    @field_validator('symbol', mode='before')
    def convert_symbol_to_lowercase(cls, v: str) -> str:
        """Convert symbol to lowercase"""
        return v.lower() if isinstance(v, str) else v

    @field_validator(
        'current_price', 'high_24h', 'low_24h', 'price_change_24h',
        'ath', 'atl', 'price_change_percentage_24h', 'market_cap_change_percentage_24h',
        'ath_change_percentage', 'atl_change_percentage', 'market_cap',
        'fully_diluted_valuation', 'total_volume', 'market_cap_change_24h',
        'circulating_supply', 'total_supply',
        mode='before'
    )
    def convert_to_decimal(cls, v: Any) -> Decimal | None:
        """Convert numeric values to Decimal safely, allowing None values"""
        if v is None:
            return None
        try:
            return cls._to_decimal(v)
        except ValueError:
            return None

    @field_validator('market_cap_rank', mode='before')
    def handle_missing_market_cap_rank(cls, v: Any) -> int:
        """Convert symbol to lowercase"""
        return int(v) if v is not None else -1

    @field_validator('data_source', mode='before')
    def convert_data_source(cls, v: Any) -> DataSource:
        """Convert string to DataSource enum"""
        if isinstance(v, DataSource):
            return v
        return DataSource(v)


class SentimentMetricsModel(BaseModel):
    """Social media sentiment metrics for a token"""
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        strict=True,
        json_encoders={
            Decimal: lambda v: f"{v:f}"  # Format decimals without scientific notation
        }
    )

    id: str = Field(..., description="CoinGecko ID")
    symbol: str = Field(..., description="Token symbol")

    # Twitter metrics
    twitter_followers: int | None = Field(None, description="Number of Twitter followers")
    twitter_following: int | None = Field(None, description="Number of Twitter accounts following")
    twitter_posts_24h: int | None = Field(None, description="Number of Twitter posts in the last 24 hours")
    twitter_engagement_rate: Decimal | None = Field(None, description="Twitter engagement rate")
    twitter_sentiment_score: Decimal | None = Field(None, description="Twitter sentiment score")

    # Reddit metrics
    reddit_subscribers: int | None = Field(None, description="Number of Reddit subscribers")
    reddit_active_users: int | None = Field(None, description="Number of active Reddit users")
    reddit_posts_24h: int | None = Field(None, description="Number of Reddit posts in the last 24 hours")
    reddit_comments_24h: int | None = Field(None, description="Number of Reddit comments in the last 24 hours")
    reddit_sentiment_score: Decimal | None = Field(None, description="Reddit sentiment score")

    # Telegram metrics
    telegram_members: int | None = Field(None, description="Number of Telegram members")
    telegram_online_members: int | None = Field(None, description="Number of online Telegram members")
    telegram_messages_24h: int | None = Field(None, description="Number of Telegram messages in the last 24 hours")
    telegram_sentiment_score: Decimal | None = Field(None, description="Telegram sentiment score")

    # Aggregated metrics
    overall_sentiment_score: Decimal = Field(..., description="Weighted average of platform sentiment scores")
    social_score: Decimal = Field(..., description="Weighted engagement score")

    updated_at: datetime = Field(..., description="Last update timestamp")
    data_source: DataSource = Field(..., description="Data source identifier")

    @field_validator('data_source', mode='before')
    def convert_data_source(cls, v: Any) -> DataSource:
        """Convert string to DataSource enum"""
        if isinstance(v, DataSource):
            return v
        return DataSource(v)
