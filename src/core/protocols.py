# src/core/protocols.py

from typing import Protocol, List, Optional
from ..domain_types import SymbolName, Timeframe, Price, Timestamp
from ..core.models import KlineData, SymbolInfo

class ExchangeAdapter(Protocol):
    """Exchange adapter protocol"""

    async def initialize(self) -> None:
        """Initialize the adapter"""
        ...

    async def get_symbols(self) -> List[SymbolInfo]:
        """Get available trading pairs"""
        ...

    async def get_klines(self,
                        symbol: SymbolName,
                        timeframe: Timeframe,
                        start_time: Optional[Timestamp] = None) -> List[KlineData]:
        """Get kline data"""
        ...

    async def close(self) -> None:
        """Close adapter connection"""
        ...


class MarketDataProvider(Protocol):
    """Market data provider protocol"""
    async def get_latest_price(self, symbol: SymbolName) -> Price: ...
    async def get_price_history(self,
                              symbol: SymbolName,
                              timeframe: Timeframe,
                              start_time: Timestamp) -> List['Kline']: ...