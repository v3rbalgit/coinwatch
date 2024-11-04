# src/services/market_data/symbol_state.py

import asyncio
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, List, Set

from ...utils.time import get_current_timestamp
from ...utils.logger import LoggerSetup
from ...utils.domain_types import Timeframe, Timestamp
from ...core.models import SymbolInfo

logger = LoggerSetup.setup(__name__)

class SymbolState(Enum):
    INITIALIZING = "initializing"    # New symbol, getting metadata
    HISTORICAL = "historical"        # Collecting historical data
    READY = "ready"                  # Ready for batch sync
    DELISTED = "delisted"            # No longer trading
    ERROR = "error"                  # Error state

@dataclass
class CollectionProgress:
    """Detailed progress tracking for historical collection"""
    start_time: datetime
    processed_candles: int = 0
    total_candles: Optional[int] = None
    last_processed_time: Optional[datetime] = None
    error_count: int = 0
    last_error: Optional[str] = None

@dataclass
class SyncProgress:
    """Progress tracking for batch synchronization"""
    next_sync: Optional[datetime] = None
    last_sync: Optional[datetime] = None
    error_count: int = 0
    last_error: Optional[str] = None

@dataclass
class SymbolProgress:
    """Enhanced symbol progress tracking"""
    symbol: SymbolInfo
    state: SymbolState
    launch_time: Optional[Timestamp] = None
    last_updated: Optional[Timestamp] = None
    last_sync_time: Optional[Timestamp] = None

    # Detailed progress tracking
    collection_progress: Optional[CollectionProgress] = None
    sync_progress: Optional[SyncProgress] = None

    # Protected symbols (won't be auto-removed when delisted)
    protected: bool = False

    # Error tracking at symbol level
    error_count: int = 0
    last_error: Optional[str] = None

    def get_progress_percentage(self) -> Optional[float]:
        """Get overall progress percentage"""
        if (self.collection_progress and
            self.collection_progress.total_candles and
            self.collection_progress.processed_candles):
            return min(100.0, (self.collection_progress.processed_candles /
                             self.collection_progress.total_candles) * 100)
        return None

    def update_progress(self,
                       current_time: Timestamp,
                       processed_time: Optional[Timestamp] = None,
                       timeframe: Timeframe = Timeframe.MINUTE_5) -> None:
        """Update progress based on processed time"""
        if self.launch_time and current_time > self.launch_time:
            total_time = current_time - self.launch_time
            interval_ms = timeframe.to_milliseconds()

            if processed_time:
                collected_time = processed_time - self.launch_time + interval_ms
                if not self.collection_progress:
                    self.collection_progress = CollectionProgress(
                        start_time=datetime.fromtimestamp(
                            self.launch_time / 1000,
                            timezone.utc
                        )
                    )

                self.collection_progress.processed_candles = int(collected_time / interval_ms)
                self.collection_progress.total_candles = int(total_time / interval_ms)
                self.last_sync_time = processed_time

    def __str__(self) -> str:
        """Enhanced string representation with detailed progress"""
        status = [f"Symbol: {self.symbol.name} @ {self.symbol.exchange}"]
        status.append(f"State: {self.state.value}")

        if self.state == SymbolState.HISTORICAL:
            if progress := self.get_progress_percentage():
                status.append(f"Collection Progress: {progress:.1f}%")
                if self.collection_progress:
                    status.append(
                        f"({self.collection_progress.processed_candles}/"
                        f"{self.collection_progress.total_candles} candles)"
                    )

        elif self.state == SymbolState.READY:
            if self.sync_progress and self.sync_progress.next_sync:
                time_until = (self.sync_progress.next_sync -
                            datetime.now(timezone.utc)).total_seconds()
                status.append(
                    f"Next Sync: {self.sync_progress.next_sync.strftime('%H:%M:%S')} "
                    f"({time_until:.1f}s)"
                )

        if self.last_error:
            status.append(f"Error: {self.last_error}")

        return " | ".join(status)

class SymbolStateManager:
    """Enhanced symbol state manager with consolidated progress tracking"""

    def __init__(self):
        self._symbols: Dict[SymbolInfo, SymbolProgress] = {}
        self._lock = asyncio.Lock()
        self._protected_symbols: Set[str] = set()

    async def add_symbol(self,
                        symbol: SymbolInfo,
                        protected: bool = False) -> None:
        """Add new symbol to tracking with optional protection"""
        async with self._lock:
            if symbol not in self._symbols:
                self._symbols[symbol] = SymbolProgress(
                    symbol=symbol,
                    state=SymbolState.INITIALIZING,
                    launch_time=symbol.launch_time,
                    protected=protected
                )
                if protected:
                    self._protected_symbols.add(symbol.name)

    async def update_collection_progress(self,
                                       symbol: SymbolInfo,
                                       processed: int,
                                       total: Optional[int] = None,
                                       error: Optional[str] = None) -> None:
        """Update historical collection progress"""
        async with self._lock:
            if progress := self._symbols.get(symbol):
                if not progress.collection_progress:
                    progress.collection_progress = CollectionProgress(
                        start_time=datetime.fromtimestamp(
                            symbol.launch_time / 1000,
                            timezone.utc
                        )
                    )

                progress.collection_progress.processed_candles = processed
                if total is not None:
                    progress.collection_progress.total_candles = total

                progress.collection_progress.last_processed_time = datetime.now(timezone.utc)

                if error:
                    progress.collection_progress.error_count += 1
                    progress.collection_progress.last_error = error

    async def update_sync_progress(self,
                                 symbol: SymbolInfo,
                                 next_sync: Optional[datetime] = None,
                                 error: Optional[str] = None) -> None:
        """Update batch sync progress"""
        async with self._lock:
            if progress := self._symbols.get(symbol):
                if not progress.sync_progress:
                    progress.sync_progress = SyncProgress()

                if next_sync:
                    progress.sync_progress.next_sync = next_sync
                    progress.sync_progress.last_sync = datetime.now(timezone.utc)

                if error:
                    progress.sync_progress.error_count += 1
                    progress.sync_progress.last_error = error

    async def get_sync_schedule(self) -> Dict[str, datetime]:
        """Get next sync times for all ready symbols"""
        schedule = {}
        async with self._lock:
            for symbol, progress in self._symbols.items():
                if (progress.state == SymbolState.READY and
                    progress.sync_progress and
                    progress.sync_progress.next_sync):
                    schedule[symbol.name] = progress.sync_progress.next_sync
        return schedule

    async def get_collection_status(self) -> Dict[str, Dict]:
        """Get detailed collection status for all symbols"""
        status = {}
        async with self._lock:
            for symbol, progress in self._symbols.items():
                if progress.state == SymbolState.HISTORICAL:
                    if cp := progress.collection_progress:
                        status[symbol.name] = {
                            "processed": cp.processed_candles,
                            "total": cp.total_candles,
                            "last_update": cp.last_processed_time,
                            "error_count": cp.error_count,
                            "last_error": cp.last_error
                        }
        return status

    async def protect_symbol(self, symbol_name: str) -> None:
        """Protect a symbol from auto-removal"""
        self._protected_symbols.add(symbol_name)
        for symbol, progress in self._symbols.items():
            if symbol.name == symbol_name:
                progress.protected = True
                break

    async def unprotect_symbol(self, symbol_name: str) -> None:
        """Remove symbol protection"""
        self._protected_symbols.discard(symbol_name)
        for symbol, progress in self._symbols.items():
            if symbol.name == symbol_name:
                progress.protected = False
                break

    def get_status_report(self) -> str:
        """Generate enhanced status report"""
        status_groups = {state: [] for state in SymbolState}
        protected_count = len(self._protected_symbols)

        for progress in self._symbols.values():
            status_groups[progress.state].append(str(progress))

        report = [
            "Symbol Status Report:",
            f"Protected Symbols: {protected_count}"
        ]

        for state in SymbolState:
            symbols = status_groups[state]
            if symbols:
                report.append(f"\n{state.value} Symbols ({len(symbols)}):")
                report.extend(f"  {s}" for s in symbols)

        return "\n".join(report)

    async def update_state(self,
                          symbol: SymbolInfo,
                          state: SymbolState,
                          error: Optional[str] = None) -> None:
        """Update symbol state with error tracking"""
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
                progress.update_progress(
                    Timestamp(get_current_timestamp()),
                    sync_time
                )

                # Update sync progress
                if not progress.sync_progress:
                    progress.sync_progress = SyncProgress()
                progress.sync_progress.last_sync = datetime.fromtimestamp(
                    sync_time / 1000,
                    timezone.utc
                )

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