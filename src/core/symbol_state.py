# src/core/symbol_state.py

import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, List

from ..utils.time import get_current_timestamp
from ..utils.logger import LoggerSetup
from ..core.models import SymbolInfo
from ..utils.domain_types import Timestamp

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
    historical_progress: Optional[float] = None  # Percentage complete

    def update_progress(self, current_time: Timestamp) -> None:
        """Update historical data collection progress"""
        if self.launch_time and current_time > self.launch_time:
            total_time = get_current_timestamp() - self.launch_time
            if self.last_sync_time:
                collected_time = self.last_sync_time - self.launch_time
                self.historical_progress = min(100.0, (collected_time / total_time) * 100)

    def __str__(self) -> str:
        status = f"Symbol: {self.symbol.name} | State: {self.state.value}"
        if self.historical_progress is not None:
            status += f" | Progress: {self.historical_progress:.2f}%"
        if self.last_error:
            status += f" | Last Error: {self.last_error}"
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
            logger.info(f"Added new symbol for tracking: {symbol}")

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

                logger.info(f"Symbol {symbol} state updated: {progress}")

    async def update_sync_time(self,
                             symbol: SymbolInfo,
                             sync_time: Timestamp) -> None:
        """Update last sync time and progress"""
        async with self._lock:
            if symbol in self._symbols:
                progress = self._symbols[symbol]
                progress.last_sync_time = sync_time
                progress.update_progress(Timestamp(get_current_timestamp()))
                logger.debug(f"Updated sync time for {symbol}: {progress}")

    async def get_symbols_by_state(self, state: SymbolState) -> List[SymbolInfo]:
        """Get all symbols in a specific state"""
        return [
            symbol for symbol, progress in self._symbols.items()
            if progress.state == state
        ]

    async def get_progress(self, symbol: SymbolInfo) -> Optional[SymbolProgress]:
        """Get progress for a specific symbol"""
        return self._symbols.get(symbol)

    async def remove_symbol(self, symbol: SymbolInfo) -> None:
        """Remove symbol from tracking"""
        if symbol in self._symbols:
            del self._symbols[symbol]
            logger.info(f"Removed symbol from tracking: {symbol}")

    def get_status_report(self) -> str:
        """Generate status report for all symbols"""
        report = ["Symbol Status Report:"]
        for progress in self._symbols.values():
            report.append(str(progress))
        return "\n".join(report)