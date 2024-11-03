# src/core/symbol_state.py

import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, List

from ..utils.time import get_current_timestamp
from ..utils.logger import LoggerSetup
from ..core.models import SymbolInfo
from ..utils.domain_types import Timeframe, Timestamp

logger = LoggerSetup.setup(__name__)

class SymbolState(Enum):
    INITIALIZING = "initializing"    # New symbol, getting metadata
    HISTORICAL = "historical"        # Collecting historical data
    READY = "ready"                  # Ready for batch sync
    DELISTED = "delisted"            # No longer trading
    ERROR = "error"                  # Error state

@dataclass
class SymbolProgress:
    """Tracks progress for a symbol's data collection"""
    symbol: SymbolInfo
    state: SymbolState
    launch_time: Optional[Timestamp] = None
    last_updated: Optional[Timestamp] = None
    last_sync_time: Optional[Timestamp] = None
    error_count: int = 0
    last_error: Optional[str] = None
    historical_progress: Optional[float] = None
    total_periods: Optional[int] = None
    processed_periods: Optional[int] = None

    def update_progress(self,
                        current_time: Timestamp,
                        processed_time: Optional[Timestamp] = None,
                        timeframe: Timeframe = Timeframe.MINUTE_5) -> None:
        """Update historical data collection progress"""
        if self.launch_time and current_time > self.launch_time:
            total_time = current_time - self.launch_time
            interval_ms = timeframe.to_milliseconds()

            if processed_time:
                collected_time = processed_time - self.launch_time + interval_ms # last candle is incomplete
                self.processed_periods = int(collected_time / interval_ms)  # for 5-minute candles
                self.total_periods = int(total_time / interval_ms)
                self.historical_progress = min(100.0, (collected_time / total_time) * 100)

    def __str__(self) -> str:
        """Enhanced string representation with detailed progress"""
        status = f"Symbol: {self.symbol.name} @ {self.symbol.exchange} | State: {self.state.value}"

        if self.historical_progress is not None:
            status += f" | Progress: {self.historical_progress:.2f}%"
            if self.processed_periods and self.total_periods:
                status += f" ({self.processed_periods}/{self.total_periods} periods)"

        if self.last_sync_time:
            from ..utils.time import from_timestamp
            status += f" | Last Sync: {from_timestamp(self.last_sync_time)}"

        if self.last_error:
            status += f" | Error: {self.last_error}"

        return status

class SymbolStateManager:
    """Manages symbol states and progress tracking"""

    def __init__(self):
        self._symbols: Dict[SymbolInfo, SymbolProgress] = {}
        self._lock = asyncio.Lock()

    async def add_symbol(self, symbol: SymbolInfo) -> None:
        """Add new symbol to tracking"""
        if symbol not in self._symbols:
            self._symbols[symbol] = SymbolProgress(
                symbol=symbol,
                state=SymbolState.INITIALIZING,
                launch_time=symbol.launch_time
            )

    async def update_state(self,
                         symbol: SymbolInfo,
                         state: SymbolState,
                         error: Optional[str] = None) -> None:
        """Update symbol state"""
        async with self._lock:
            if symbol in self._symbols:
                progress = self._symbols[symbol]
                progress.state = state
                progress.last_updated = Timestamp(get_current_timestamp())

                if error:
                    progress.error_count += 1
                    progress.last_error = error
                    logger.error(f"Symbol {symbol} error: {error}")
                else:
                    progress.last_error = None

    async def update_sync_time(self,
                             symbol: SymbolInfo,
                             sync_time: Timestamp) -> None:
        """Update last sync time and progress"""
        async with self._lock:
            if symbol in self._symbols:
                progress = self._symbols[symbol]
                progress.last_sync_time = sync_time
                progress.update_progress(Timestamp(get_current_timestamp()))

    async def get_symbols_by_state(self, state: SymbolState) -> List[SymbolInfo]:
        """Get all symbols in a specific state"""
        return [ symbol for symbol, progress in self._symbols.items()
                if progress.state == state ]

    async def get_progress(self, symbol: SymbolInfo) -> Optional[SymbolProgress]:
        """Get progress for a specific symbol"""
        return self._symbols.get(symbol)

    async def remove_symbol(self, symbol: SymbolInfo) -> None:
        """Remove symbol from tracking"""
        if symbol in self._symbols:
            del self._symbols[symbol]
            logger.info(f"Removed symbol from tracking: {symbol}")

    def get_status_report(self) -> str:
        """Generate detailed status report for all symbols"""
        status_groups = {state: [] for state in SymbolState}

        for progress in self._symbols.values():
            status_groups[progress.state].append(str(progress))

        report = ["Symbol Status Report:"]

        for state in SymbolState:
            symbols = status_groups[state]
            if symbols:
                report.append(f"\n{state.value} Symbols ({len(symbols)}):")
                report.extend(f"  {s}" for s in symbols)

        return "\n".join(report)