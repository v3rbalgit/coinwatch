# src/core/models.py

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Tuple
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

@dataclass
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
    def trading_pair(self) -> str:
        """Get base/quote pair"""
        return f"{self.base_asset}/{self.quote_asset}"