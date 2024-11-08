# src/core/protocols.py

from typing import Protocol, List, Optional

from ..utils.domain_types import Timeframe, Timestamp
from ..core.models import KlineData, Observation, SymbolInfo

class ExchangeAdapter(Protocol):
    """Exchange adapter protocol"""

    @property
    def kline_limit(self) -> int:
        """Get maximum number of klines that can be fetched in one request"""
        ...

    async def initialize(self) -> None:
        """Initialize the adapter"""
        ...

    async def get_symbols(self) -> List[SymbolInfo]:
        """Get available trading pairs"""
        ...

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[Timestamp] = None,
                        end_time: Optional[Timestamp] = None,
                        limit: Optional[int] = None) -> List[KlineData]:
        """Get kline data"""
        ...

    async def close(self) -> None:
        """Close adapter connection"""
        ...

class ObserverProtocol(Protocol):
    """Protocol for system observers"""
    async def on_observation(self, observation: Observation) -> None:
        """Handle new observation"""
        ...