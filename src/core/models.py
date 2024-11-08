# src/core/models.py

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Tuple
from ..utils.domain_types import SymbolName, ExchangeName, Timestamp

@dataclass
class KlineData:
    """Domain model for kline data (business logic)"""
    timestamp: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    turnover: Decimal
    symbol: str
    timeframe: str

    @property
    def datetime(self) -> datetime:
        """Convert timestamp to datetime"""
        return datetime.fromtimestamp(self.timestamp / 1000)

    @property
    def price_range(self) -> Decimal:
        """Calculate price range"""
        return self.high_price - self.low_price

    def to_tuple(self) -> Tuple[Timestamp, float, float, float, float, float, float]:
        """Convert to tuple format for database storage"""
        return (
            Timestamp(self.timestamp),
            float(self.open_price),
            float(self.high_price),
            float(self.low_price),
            float(self.close_price),
            float(self.volume),
            float(self.turnover)
        )

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

# Technical Indicator Models
@dataclass
class RSIResult:
    """Relative Strength Index result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float) -> 'RSIResult':
        return cls(timestamp=timestamp, value=Decimal(str(value)))

@dataclass
class BollingerBandsResult:
    """Bollinger Bands result"""
    timestamp: Timestamp
    upper: Decimal
    middle: Decimal
    lower: Decimal
    bandwidth: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, bb_dict: Dict[str, float]) -> 'BollingerBandsResult':
        return cls(
            timestamp=timestamp,
            upper=Decimal(str(bb_dict['BBU_20_2.0'])),
            middle=Decimal(str(bb_dict['BBM_20_2.0'])),
            lower=Decimal(str(bb_dict['BBL_20_2.0'])),
            bandwidth=Decimal(str(bb_dict['BBB_20_2.0']))
        )

@dataclass
class MACDResult:
    """MACD result"""
    timestamp: Timestamp
    macd: Decimal
    signal: Decimal
    histogram: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, macd_dict: Dict[str, float]) -> 'MACDResult':
        return cls(
            timestamp=timestamp,
            macd=Decimal(str(macd_dict['MACD_12_26_9'])),
            signal=Decimal(str(macd_dict['MACDs_12_26_9'])),
            histogram=Decimal(str(macd_dict['MACDh_12_26_9']))
        )

@dataclass
class MAResult:
    """Moving Average result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float) -> 'MAResult':
        return cls(timestamp=timestamp, value=Decimal(str(value)))

@dataclass
class OBVResult:
    """On Balance Volume result"""
    timestamp: Timestamp
    value: Decimal

    @classmethod
    def from_series(cls, timestamp: Timestamp, value: float) -> 'OBVResult':
        return cls(timestamp=timestamp, value=Decimal(str(value)))