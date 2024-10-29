# src/observers/data_sync_observer.py

from typing import Dict, Any, Optional, Set, List, Tuple
from dataclasses import dataclass
import time
from sqlalchemy import text
from src.core.observers.base import Observer, ObservationContext
from src.core.events import Event, EventType
from src.core.actions import ActionPriority
from src.utils.timestamp import get_current_timestamp, from_timestamp
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class DataSyncMetrics:
    """Container for data synchronization metrics."""
    total_symbols: int
    outdated_symbols: List[str]
    symbols_with_gaps: Dict[str, List[Dict[str, int]]]
    latest_timestamps: Dict[str, int]
    sync_lag: Dict[str, float]  # in seconds
    data_coverage: Dict[str, float]  # percentage
    oldest_data: Optional[int] = None
    newest_data: Optional[int] = None

class DataSyncObserver(Observer):
    """
    Observes data synchronization state and coordinates data maintenance.

    Responsibilities:
    - Monitor data freshness for all symbols
    - Detect and report data gaps
    - Track sync progress and health
    - Coordinate data repair actions
    """

    def __init__(self,
                 state_manager,
                 action_manager,
                 session_factory,
                 observation_interval: float = 30.0,
                 sync_threshold: int = 300,  # 5 minutes in seconds
                 max_sync_lag: int = 3600):  # 1 hour in seconds
        super().__init__(
            name="DataSyncObserver",
            state_manager=state_manager,
            action_manager=action_manager,
            observation_interval=observation_interval
        )
        self.session_factory = session_factory
        self.sync_threshold = sync_threshold
        self.max_sync_lag = max_sync_lag
        self._active_syncs: Set[str] = set()
        self._last_gap_check = 0
        self._gap_check_interval = 3600  # 1 hour in seconds

    async def observe(self) -> ObservationContext:
        """Collect current data sync state."""
        try:
            with self.session_factory() as session:
                current_time = get_current_timestamp()

                # Collect comprehensive sync metrics
                metrics = await self._collect_sync_metrics(session, current_time)

                # Determine if significant changes occurred
                changes_detected = self._detect_significant_changes(metrics)

                return ObservationContext(
                    timestamp=current_time,
                    current_state={'metrics': metrics},
                    changes_detected=changes_detected
                )

        except Exception as e:
            logger.error(f"Error during sync observation: {e}", exc_info=True)
            return ObservationContext(
                timestamp=get_current_timestamp(),
                error=e
            )

    async def analyze(self, context: ObservationContext) -> None:
        """Analyze sync state and trigger necessary actions."""
        if not context.current_state or 'metrics' not in context.current_state:
            return

        metrics: DataSyncMetrics = context.current_state['metrics']
        try:
            # Handle outdated symbols
            if metrics.outdated_symbols:
                await self._handle_outdated_symbols(metrics.outdated_symbols)

            # Check for significant gaps
            current_time = time.time()
            if current_time - self._last_gap_check >= self._gap_check_interval:
                if metrics.symbols_with_gaps:
                    await self._handle_data_gaps(metrics.symbols_with_gaps)
                self._last_gap_check = current_time

            # Handle excessive sync lag
            excessive_lag = {
                symbol: lag for symbol, lag in metrics.sync_lag.items()
                if lag > self.max_sync_lag
            }
            if excessive_lag:
                await self._handle_excessive_lag(excessive_lag)

            # Update metrics
            self._update_observer_metrics(metrics)

        except Exception as e:
            logger.error(f"Error analyzing sync state: {e}", exc_info=True)
            await self.handle_error(e)

    async def handle_error(self, error: Exception) -> None:
        """Handle observation errors."""
        logger.error(f"Data sync observation error: {error}", exc_info=True)

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_HEALTH_CHANGED,
            {
                'service_name': self.name,
                'status': 'error',
                'error': str(error),
                'timestamp': time.time()
            }
        ))

    async def _collect_sync_metrics(self,
                                  session,
                                  current_time: int) -> DataSyncMetrics:
        """Collect comprehensive sync metrics."""
        # Get all symbols
        symbols = await self._get_all_symbols(session)

        # Find outdated symbols
        outdated = await self._find_outdated_symbols(session, current_time)

        # Check for gaps
        gaps = await self._find_data_gaps(session)

        # Get latest timestamps
        latest_timestamps = await self._get_latest_timestamps(session)

        # Calculate sync lag
        sync_lag = {
            symbol: (current_time - latest_timestamps.get(symbol, 0)) / 1000
            for symbol in symbols
        }

        # Calculate data coverage
        coverage = await self._calculate_data_coverage(session)

        # Get data range
        oldest, newest = await self._get_data_range(session)

        return DataSyncMetrics(
            total_symbols=len(symbols),
            outdated_symbols=outdated,
            symbols_with_gaps=gaps,
            latest_timestamps=latest_timestamps,
            sync_lag=sync_lag,
            data_coverage=coverage,
            oldest_data=oldest,
            newest_data=newest
        )

    async def _find_outdated_symbols(self,
                                   session,
                                   current_time: int) -> List[str]:
        """Find symbols that need data update."""
        threshold_time = current_time - (self.sync_threshold * 1000)

        result = session.execute(text("""
            SELECT s.name
            FROM symbols s
            LEFT JOIN (
                SELECT symbol_id, MAX(start_time) as last_time
                FROM kline_data
                GROUP BY symbol_id
            ) k ON s.id = k.symbol_id
            WHERE k.last_time < :threshold OR k.last_time IS NULL
        """), {"threshold": threshold_time})

        return [row[0] for row in result]

    async def _find_data_gaps(self,
                            session) -> Dict[str, List[Dict[str, int]]]:
        """Find gaps in historical data."""
        result = session.execute(text("""
            WITH gaps AS (
                SELECT
                    s.name,
                    k.start_time,
                    LEAD(k.start_time) OVER (
                        PARTITION BY s.id
                        ORDER BY k.start_time
                    ) as next_time
                FROM symbols s
                JOIN kline_data k ON s.id = k.symbol_id
                WHERE k.start_time >= :start_time
            )
            SELECT name, start_time, next_time
            FROM gaps
            WHERE next_time - start_time > :gap_threshold
        """), {
            "start_time": get_current_timestamp() - (24 * 3600 * 1000),  # Last 24h
            "gap_threshold": 5 * 60 * 1000  # 5 minutes in milliseconds
        })

        gaps: Dict[str, List[Dict[str, int]]] = {}
        for row in result:
            if row[0] not in gaps:
                gaps[row[0]] = []
            gaps[row[0]].append({
                'start': row[1] + (5 * 60 * 1000),  # Add 5 minutes to start
                'end': row[2]
            })

        return gaps

    async def _handle_outdated_symbols(self, symbols: List[str]) -> None:
        """Handle outdated symbols by triggering sync actions."""
        # Filter out symbols already being synced
        symbols_to_sync = [s for s in symbols if s not in self._active_syncs]

        if not symbols_to_sync:
            return

        await self.state_manager.emit_event(Event.create(
            EventType.DATA_SYNC_NEEDED,
            {
                'symbols': symbols_to_sync,
                'reason': 'outdated_data',
                'priority': 'HIGH'
            }
        ))

        # Track active syncs
        self._active_syncs.update(symbols_to_sync)

    async def _handle_data_gaps(self,
                              gaps: Dict[str, List[Dict[str, int]]]) -> None:
        """Handle detected data gaps."""
        for symbol, symbol_gaps in gaps.items():
            if symbol not in self._active_syncs:
                await self.state_manager.emit_event(Event.create(
                    EventType.DATA_GAP_DETECTED,
                    {
                        'symbol': symbol,
                        'gaps': symbol_gaps,
                        'priority': 'MEDIUM'
                    }
                ))

    async def _handle_excessive_lag(self, lag_info: Dict[str, float]) -> None:
        """Handle symbols with excessive sync lag."""
        symbols = list(lag_info.keys())

        await self.state_manager.emit_event(Event.create(
            EventType.HISTORICAL_DATA_NEEDED,
            {
                'symbols': symbols,
                'reason': 'excessive_lag',
                'lag_info': lag_info
            }
        ))

    def _update_observer_metrics(self, metrics: DataSyncMetrics) -> None:
        """Update internal metrics state."""
        self._metrics.update({
            'total_symbols': metrics.total_symbols,
            'outdated_symbols': len(metrics.outdated_symbols),
            'symbols_with_gaps': len(metrics.symbols_with_gaps),
            'average_sync_lag': sum(metrics.sync_lag.values()) / len(metrics.sync_lag) if metrics.sync_lag else 0,
            'average_coverage': sum(metrics.data_coverage.values()) / len(metrics.data_coverage) if metrics.data_coverage else 0,
            'active_syncs': len(self._active_syncs),
            'oldest_data': from_timestamp(metrics.oldest_data) if metrics.oldest_data else None,
            'newest_data': from_timestamp(metrics.newest_data) if metrics.newest_data else None
        })

    async def _get_all_symbols(self, session) -> List[str]:
        """Get all symbols from database."""
        result = session.execute(text("SELECT name FROM symbols"))
        return [row[0] for row in result]

    async def _get_latest_timestamps(self, session) -> Dict[str, int]:
        """Get latest timestamp for each symbol."""
        result = session.execute(text("""
            SELECT s.name, MAX(k.start_time) as latest
            FROM symbols s
            LEFT JOIN kline_data k ON s.id = k.symbol_id
            GROUP BY s.name
        """))
        return {row[0]: row[1] for row in result}

    async def _calculate_data_coverage(self, session) -> Dict[str, float]:
        """Calculate data coverage percentage for each symbol."""
        result = session.execute(text("""
            SELECT
                s.name,
                COUNT(k.id) * 300 as covered_seconds,
                MAX(k.start_time) - MIN(k.start_time) as total_seconds
            FROM symbols s
            LEFT JOIN kline_data k ON s.id = k.symbol_id
            GROUP BY s.name
        """))

        coverage = {}
        for row in result:
            if row[2]:  # total_seconds exists
                coverage[row[0]] = (row[1] / row[2]) * 100
            else:
                coverage[row[0]] = 0.0

        return coverage

    async def _get_data_range(self, session) -> Tuple[Optional[int], Optional[int]]:
        """Get oldest and newest data points."""
        result = session.execute(text("""
            SELECT MIN(start_time), MAX(start_time)
            FROM kline_data
        """)).first()
        return result[0], result[1]

    def _detect_significant_changes(self, metrics: DataSyncMetrics) -> bool:
        """Detect if significant changes occurred in sync state."""
        # Define significance thresholds
        OUTDATED_THRESHOLD = 5  # More than 5 outdated symbols
        GAP_THRESHOLD = 3       # More than 3 symbols with gaps
        LAG_THRESHOLD = 1800    # 30 minutes average lag

        average_lag = sum(metrics.sync_lag.values()) / len(metrics.sync_lag) if metrics.sync_lag else 0

        return (
            len(metrics.outdated_symbols) > OUTDATED_THRESHOLD or
            len(metrics.symbols_with_gaps) > GAP_THRESHOLD or
            average_lag > LAG_THRESHOLD
        )