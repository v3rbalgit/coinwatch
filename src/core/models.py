# src/core/models.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from ..utils.time import TimeUtils
from ..utils.domain_types import DataSource, SymbolName, ExchangeName, Timeframe, Timestamp

@dataclass(frozen=True)
class SymbolInfo:
    """Domain model for symbol information"""
    name: SymbolName
    base_asset: str
    quote_asset: str
    price_precision: int
    qty_precision: int
    min_order_qty: Decimal
    launch_time: Timestamp
    exchange: ExchangeName = ExchangeName("bybit")

    @property
    def symbol_name(self):
        return self.name.lower().replace('usdt', '')

    def check_retention_time(self, retention_days: int) -> 'SymbolInfo':
        """Create new instance with adjusted launch time based on retention period"""
        if retention_days > 0:
            retention_start = TimeUtils.get_current_timestamp() - (retention_days * 24 * 60 * 60 * 1000)
            new_launch_time = Timestamp(max(self.launch_time, retention_start))

            return SymbolInfo(
                name=self.name,
                base_asset=self.base_asset,
                quote_asset=self.quote_asset,
                price_precision=self.price_precision,
                qty_precision=self.qty_precision,
                min_order_qty=self.min_order_qty,
                launch_time=new_launch_time,
                exchange=self.exchange
            )
        return self

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

    def __repr__(self) -> str:
        """Detailed string representation for debugging"""
        return (f"SymbolInfo(name='{self.name}', exchange='{self.exchange}', "
                f"launch_time={self.launch_time}, "
                f"base='{self.base_asset}', quote='{self.quote_asset}', "
                f"price_prec={self.price_precision}, qty_prec={self.qty_precision}, "
                f"min_qty={self.min_order_qty})")

    def __format__(self, format_spec: str) -> str:
        """Support string formatting"""
        if format_spec == 'short':
            return self.__str__()
        if format_spec == 'pair':
            return self.trading_pair
        return self.__repr__()

    def __bool__(self) -> bool:
        """Consider symbol valid if it has a name and exchange"""
        return bool(self.name and self.exchange)

    @property
    def trading_pair(self) -> str:
        """Get base/quote pair"""
        return f"{self.base_asset}/{self.quote_asset}"


@dataclass
class KlineData:
    """Domain model for kline data (business logic)"""
    timestamp: Timestamp
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    turnover: Decimal
    symbol: SymbolInfo
    timeframe: Timeframe

    def to_tuple(self) -> Tuple[Timestamp, float, float, float, float, float, float]:
        """Convert to tuple format for database storage"""
        return (
            self.timestamp,
            float(self.open_price),
            float(self.high_price),
            float(self.low_price),
            float(self.close_price),
            float(self.volume),
            float(self.turnover)
        )

# Technical Indicator Models
@dataclass
class RSIResult:
    """Relative Strength Index result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float, length: int = 14) -> 'RSIResult':
        # RSI column name format: 'RSI_14'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value))
        )

@dataclass
class MACDResult:
    """MACD result"""
    timestamp: Timestamp
    macd: Decimal
    signal: Decimal
    histogram: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, macd_dict: Dict[str, float],
                   fast: int = 12, slow: int = 26, signal: int = 9) -> 'MACDResult':
        # MACD column names format: 'MACD_12_26_9', 'MACDs_12_26_9', 'MACDh_12_26_9'
        suffix = f"_{fast}_{slow}_{signal}"
        return cls(
            timestamp=timestamp,
            macd=Decimal(str(macd_dict[f'MACD{suffix}'])),
            signal=Decimal(str(macd_dict[f'MACDs{suffix}'])),
            histogram=Decimal(str(macd_dict[f'MACDh{suffix}']))
        )

@dataclass
class BollingerBandsResult:
    """Bollinger Bands result"""
    timestamp: Timestamp
    upper: Decimal
    middle: Decimal
    lower: Decimal
    bandwidth: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, bb_dict: Dict[str, float],
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

@dataclass
class MAResult:
    """Moving Average result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float, length: int = 20) -> 'MAResult':
        # MA column names format: 'SMA_20' or 'EMA_20'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value))
        )

@dataclass
class OBVResult:
    """On Balance Volume result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float) -> 'OBVResult':
        # OBV doesn't have parameters, column name is just 'OBV'
        return cls(
            timestamp=timestamp,
            value=Decimal(str(value))
        )

# Metadata Model
@dataclass
class Metadata:
    """Domain model for token metadata"""
    id: str
    symbol: SymbolInfo
    name: str
    description: str
    market_cap_rank: int
    category: str
    updated_at: Timestamp
    launch_time: Optional[Timestamp]
    platform: Optional[str]
    contract_address: Optional[str]
    hashing_algorithm: Optional[str]
    website: Optional[str]
    whitepaper: Optional[str]
    reddit: Optional[str]
    twitter: Optional[str]
    telegram: Optional[str]
    github: Optional[str]
    images: Dict[str, str]
    data_source: DataSource

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to database-friendly dictionary"""
        return {
            "coingecko_id": self.id,
            "symbol": self.symbol.name,
            "name": self.name,
            "description": self.description,
            "market_cap_rank": int(self.market_cap_rank) if self.market_cap_rank else None,
            "category": self.category,
            "platform": self.platform,
            "contract_address": self.contract_address,
            "launch_time": TimeUtils.from_timestamp(self.launch_time) if self.launch_time else None,
            "hashing_algorithm": self.hashing_algorithm,
            "website": self.website,
            "whitepaper": self.whitepaper,
            "reddit": self.reddit,
            "twitter": self.twitter,
            "telegram": self.telegram,
            "github": self.github,
            "images": self.images,
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
    market_cap: Optional[Decimal]
    market_cap_rank: Optional[int]
    fully_diluted_valuation: Optional[Decimal]
    total_volume: Decimal
    high_24h: Optional[Decimal]
    low_24h: Optional[Decimal]
    price_change_24h: Optional[Decimal]
    price_change_percentage_24h: Optional[Decimal]
    market_cap_change_24h: Optional[Decimal]
    market_cap_change_percentage_24h: Optional[Decimal]
    circulating_supply: Optional[Decimal]
    total_supply: Optional[Decimal]
    max_supply: Optional[Decimal]
    ath: Optional[Decimal]
    ath_change_percentage: Optional[Decimal]
    ath_date: Optional[str]
    atl: Optional[Decimal]
    atl_change_percentage: Optional[Decimal]
    atl_date: Optional[str]
    updated_at: Timestamp
    data_source: DataSource

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'id': self.id,
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