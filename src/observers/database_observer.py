# src/observers/database_observer.py

from typing import Dict, Any, Optional, List, NamedTuple
from dataclasses import dataclass
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from src.core.observers.base import Observer, ObservationContext
from src.core.events import Event, EventType
from src.core.actions import ActionPriority
from src.managers.integrity_manager import DataIntegrityManager
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class DatabaseMetrics:
    """Container for database performance metrics."""
    pool_size: int
    pool_checked_out: int
    pool_overflow: int
    pool_available: int
    connection_count: int
    slow_queries_count: int
    failed_queries_count: int
    avg_query_time: float
    max_query_time: float
    transaction_count: int
    deadlock_count: int
    table_sizes: Dict[str, int]
    index_sizes: Dict[str, int]
    data_integrity_score: float

class DatabaseHealthStatus(NamedTuple):
    """Database health status indicators."""
    connection_health: str  # 'healthy', 'warning', 'critical'
    performance_health: str
    storage_health: str
    integrity_health: str
    overall_health: str

class DatabaseObserver(Observer):
    """
    Observes database health, performance, and integrity.

    Monitors:
    - Connection pool health
    - Query performance
    - Storage utilization
    - Data integrity
    - Transaction health
    """

    def __init__(self,
                 state_manager,
                 action_manager,
                 engine: Engine,
                 session_factory,
                 observation_interval: float = 30.0,
                 slow_query_threshold: float = 1.0):  # 1 second
        super().__init__(
            name="DatabaseObserver",
            state_manager=state_manager,
            action_manager=action_manager,
            observation_interval=observation_interval
        )
        self.engine = engine
        self.session_factory = session_factory
        self.slow_query_threshold = slow_query_threshold
        self._previous_metrics: Optional[DatabaseMetrics] = None
        self._performance_history: List[Dict[str, float]] = []
        self._query_stats: Dict[str, Dict[str, float]] = {}
        self._last_action_time: Dict[str, float] = {}
        self.ACTION_COOLDOWNS = {
            'handle_connection_crisis': 300,    # 5 minutes
            'handle_performance_crisis': 300,
            'handle_storage_crisis': 300,
            'optimize_connections': 600,        # 10 minutes
            'verify_data_integrity': 3600,      # 1 hour
            'perform_maintenance': 86400        # 24 hours
        }

    async def observe(self) -> ObservationContext:
        """Collect current database metrics and health status."""
        try:
            metrics = await self._collect_database_metrics()
            health_status = self._evaluate_health_status(metrics)

            context = ObservationContext(
                timestamp=time.time(),
                previous_state={'metrics': self._previous_metrics} if self._previous_metrics else None,
                current_state={
                    'metrics': metrics,
                    'health_status': health_status
                },
                changes_detected=self._detect_significant_changes(metrics)
            )

            self._previous_metrics = metrics
            return context

        except Exception as e:
            logger.error(f"Error during database observation: {e}", exc_info=True)
            return ObservationContext(
                timestamp=time.time(),
                error=e
            )

    async def analyze(self, context: ObservationContext) -> None:
        """Analyze database health and trigger necessary actions."""
        if not context.current_state:
            return

        metrics: DatabaseMetrics = context.current_state['metrics']
        health_status: DatabaseHealthStatus = context.current_state['health_status']

        try:
            # Handle critical conditions first
            if await self._handle_critical_conditions(metrics, health_status):
                return

            # Handle warnings
            await self._handle_warning_conditions(metrics, health_status)

            # Schedule maintenance if needed
            await self._schedule_maintenance(metrics)

            # Update observer metrics
            self._update_observer_metrics(metrics, health_status)

        except Exception as e:
            logger.error(f"Error analyzing database state: {e}", exc_info=True)
            await self.handle_error(e)

    async def handle_error(self, error: Exception) -> None:
        """Handle observation errors."""
        logger.error(f"Database observation error: {error}", exc_info=True)

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_HEALTH_CHANGED,
            {
                'service_name': self.name,
                'status': 'error',
                'error': str(error),
                'timestamp': time.time()
            }
        ))

        # Submit error handling action
        await self.action_manager.submit_action(
            'handle_database_error',
            {
                'error': str(error),
                'timestamp': time.time(),
                'severity': 'critical'
            },
            priority=ActionPriority.CRITICAL
        )

    async def _collect_database_metrics(self) -> DatabaseMetrics:
        """Collect comprehensive database metrics."""
        with self.session_factory() as session:
            try:
                # Collect pool metrics
                pool_metrics = self._get_pool_metrics()

                # Collect performance metrics
                perf_metrics = await self._get_performance_metrics(session)

                # Collect storage metrics
                storage_metrics = await self._get_storage_metrics(session)

                # Calculate data integrity score
                integrity_score = await self._calculate_integrity_score(session)

                return DatabaseMetrics(
                    pool_size=pool_metrics['pool_size'],
                    pool_checked_out=pool_metrics['checked_out'],
                    pool_overflow=pool_metrics['overflow'],
                    pool_available=pool_metrics['available'],
                    connection_count=pool_metrics['total_connections'],
                    slow_queries_count=perf_metrics['slow_queries'],
                    failed_queries_count=perf_metrics['failed_queries'],
                    avg_query_time=perf_metrics['avg_query_time'],
                    max_query_time=perf_metrics['max_query_time'],
                    transaction_count=perf_metrics['transaction_count'],
                    deadlock_count=perf_metrics['deadlock_count'],
                    table_sizes=storage_metrics['table_sizes'],
                    index_sizes=storage_metrics['index_sizes'],
                    data_integrity_score=integrity_score
                )

            except Exception as e:
                logger.error(f"Error collecting database metrics: {e}")
                raise

    def _get_pool_metrics(self) -> Dict[str, int]:
        """Get connection pool metrics."""
        try:
            pool = self.engine.pool
            if isinstance(pool, QueuePool):
                return {
                    'pool_size': pool.size(),
                    'checked_out': pool.checkedout(),
                    'overflow': pool.overflow(),
                    'available': pool.size() - pool.checkedout(),
                    'total_connections': pool.checkedout() + pool.overflow()
                }
            return {}
        except Exception as e:
            logger.error(f"Error getting pool metrics: {e}")
            return {}

    async def _get_performance_metrics(self, session) -> Dict[str, Any]:
        """Get database performance metrics."""
        try:
            # Query MySQL performance schema
            result = session.execute(text("""
                SELECT
                    COUNT(*) as total_queries,
                    SUM(COUNT_STAR) as execution_count,
                    SUM(SUM_TIMER_WAIT)/1000000000000 as total_latency,
                    MAX(MAX_TIMER_WAIT)/1000000000000 as max_latency
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SCHEMA_NAME = :schema
            """), {"schema": self.engine.url.database})

            row = result.first()

            # Get deadlock information
            deadlocks = session.execute(text("""
                SELECT COUNT(*)
                FROM performance_schema.events_statements_history
                WHERE EVENT_NAME = 'statement/sql/update'
                AND STATE = 'Deadlock'
            """)).scalar()

            return {
                'slow_queries': session.execute(text("""
                    SELECT COUNT(*)
                    FROM performance_schema.events_statements_history
                    WHERE TIMER_WAIT/1000000000000 > :threshold
                """), {"threshold": self.slow_query_threshold}).scalar(),
                'failed_queries': session.execute(text("""
                    SELECT COUNT(*)
                    FROM performance_schema.events_statements_history
                    WHERE ERRORS > 0
                """)).scalar(),
                'avg_query_time': row.total_latency / row.execution_count if row.execution_count else 0,
                'max_query_time': row.max_latency or 0,
                'transaction_count': row.total_queries or 0,
                'deadlock_count': deadlocks or 0
            }
        except Exception as e:
            logger.error(f"Error getting performance metrics: {e}")
            return {
                'slow_queries': 0,
                'failed_queries': 0,
                'avg_query_time': 0,
                'max_query_time': 0,
                'transaction_count': 0,
                'deadlock_count': 0
            }

    async def _get_storage_metrics(self, session) -> Dict[str, Dict[str, int]]:
        """Get database storage metrics."""
        try:
            # Get table sizes
            table_sizes = {}
            result = session.execute(text("""
                SELECT
                    TABLE_NAME,
                    DATA_LENGTH + INDEX_LENGTH as total_size
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
            """), {"schema": self.engine.url.database})

            for row in result:
                table_sizes[row[0]] = row[1]

            # Get index sizes
            index_sizes = {}
            result = session.execute(text("""
                SELECT
                    TABLE_NAME,
                    SUM(STAT_VALUE) * @@innodb_page_size as index_size
                FROM mysql.innodb_index_stats
                WHERE database_name = :schema
                AND stat_name = 'size'
                GROUP BY TABLE_NAME
            """), {"schema": self.engine.url.database})

            for row in result:
                index_sizes[row[0]] = row[1]

            return {
                'table_sizes': table_sizes,
                'index_sizes': index_sizes
            }
        except Exception as e:
            logger.error(f"Error getting storage metrics: {e}")
            return {
                'table_sizes': {},
                'index_sizes': {}
            }

    async def _calculate_integrity_score(self, session) -> float:
        """Calculate overall data integrity score."""
        try:
            integrity_manager = DataIntegrityManager(session)

            # Check integrity for all symbols
            symbols = session.execute(text("SELECT name FROM symbols")).scalars().all()
            integrity_results = [
                integrity_manager.verify_data_integrity(symbol)
                for symbol in symbols
            ]

            # Calculate score based on various factors
            total_symbols = len(symbols)
            if not total_symbols:
                return 1.0

            valid_symbols = sum(
                1 for result in integrity_results
                if not result.get('has_gaps') and
                not result.get('invalid_records') and
                not result.get('duplicate_count')
            )

            return valid_symbols / total_symbols

        except Exception as e:
            logger.error(f"Error calculating integrity score: {e}")
            return 0.0

    def _evaluate_health_status(self, metrics: DatabaseMetrics) -> DatabaseHealthStatus:
        """Evaluate overall database health status."""
        # Connection health
        if metrics.pool_overflow > 0 or metrics.pool_available == 0:
            connection_health = 'critical'
        elif metrics.pool_checked_out / metrics.pool_size > 0.8:
            connection_health = 'warning'
        else:
            connection_health = 'healthy'

        # Performance health
        if metrics.failed_queries_count > 0 or metrics.deadlock_count > 0:
            performance_health = 'critical'
        elif metrics.slow_queries_count > 10 or metrics.avg_query_time > self.slow_query_threshold:
            performance_health = 'warning'
        else:
            performance_health = 'healthy'

        # Storage health
        total_size = sum(metrics.table_sizes.values())
        if total_size > 0.9 * self._get_disk_space():  # 90% used
            storage_health = 'critical'
        elif total_size > 0.7 * self._get_disk_space():  # 70% used
            storage_health = 'warning'
        else:
            storage_health = 'healthy'

        # Integrity health
        if metrics.data_integrity_score < 0.9:
            integrity_health = 'critical'
        elif metrics.data_integrity_score < 0.95:
            integrity_health = 'warning'
        else:
            integrity_health = 'healthy'

        # Overall health is the worst of all statuses
        overall_health = 'critical' if 'critical' in [
            connection_health, performance_health,
            storage_health, integrity_health
        ] else 'warning' if 'warning' in [
            connection_health, performance_health,
            storage_health, integrity_health
        ] else 'healthy'

        return DatabaseHealthStatus(
            connection_health=connection_health,
            performance_health=performance_health,
            storage_health=storage_health,
            integrity_health=integrity_health,
            overall_health=overall_health
        )

    async def _handle_critical_conditions(self, metrics: DatabaseMetrics, health: DatabaseHealthStatus) -> bool:
        """Handle critical database conditions."""
        if health.overall_health != 'critical':
            return False

        current_time = time.time()
        action_params = {
            'engine': self.engine,
            'session_factory': self.session_factory,
            'metrics': metrics.__dict__,
            'severity': 'critical'
        }

        # Handle connection crisis
        if health.connection_health == 'critical' and self._can_trigger_action('handle_connection_crisis', current_time):
            await self.action_manager.submit_action(
                'handle_connection_crisis',
                action_params,
                priority=ActionPriority.CRITICAL
            )
            self._last_action_time['handle_connection_crisis'] = current_time
            return True

        # Handle performance crisis
        if health.performance_health == 'critical' and self._can_trigger_action('handle_performance_crisis', current_time):
            await self.action_manager.submit_action(
                'handle_performance_crisis',
                action_params,
                priority=ActionPriority.CRITICAL
            )
            self._last_action_time['handle_performance_crisis'] = current_time
            return True

        # Handle storage crisis
        if health.storage_health == 'critical' and self._can_trigger_action('handle_storage_crisis', current_time):
            await self.action_manager.submit_action(
                'handle_storage_crisis',
                action_params,
                priority=ActionPriority.CRITICAL
            )
            self._last_action_time['handle_storage_crisis'] = current_time
            return True

        return False

    async def _handle_warning_conditions(self, metrics: DatabaseMetrics, health: DatabaseHealthStatus) -> None:
        """Handle warning database conditions."""
        current_time = time.time()
        action_params = {
            'engine': self.engine,
            'session_factory': self.session_factory,
            'metrics': metrics.__dict__,
            'severity': 'warning'
        }

        if health.overall_health == 'warning':
            # Handle connection warnings
            if (health.connection_health == 'warning' and
                self._can_trigger_action('optimize_connections', current_time)):
                await self.action_manager.submit_action(
                    'optimize_connections',
                    action_params,
                    priority=ActionPriority.HIGH
                )
                self._last_action_time['optimize_connections'] = current_time

            # Handle integrity warnings
            if (health.integrity_health == 'warning' and
                self._can_trigger_action('verify_data_integrity', current_time)):
                await self.action_manager.submit_action(
                    'verify_data_integrity',
                    {
                        **action_params,
                        'scope': 'incremental'  # Can be 'full', 'incremental', or 'targeted'
                    },
                    priority=ActionPriority.HIGH
                )
                self._last_action_time['verify_data_integrity'] = current_time

    def _can_trigger_action(self,
                          action_name: str,
                          current_time: float,
                          cooldown: Optional[float] = None) -> bool:
        """
        Check if enough time has passed to trigger an action again.

        Args:
            action_name: Name of the action to check
            current_time: Current timestamp
            cooldown: Optional override for cooldown period

        Returns:
            bool: True if action can be triggered
        """
        last_time = self._last_action_time.get(action_name, 0)
        cooldown_period = cooldown or self.ACTION_COOLDOWNS.get(action_name, 300)  # Default 5 minutes

        return (current_time - last_time) >= cooldown_period

    async def _schedule_maintenance(self, metrics: DatabaseMetrics) -> None:
        """Schedule routine maintenance if needed."""
        current_time = time.time()

        # Check if maintenance is due (e.g., every 24 hours)
        if self._can_trigger_action('perform_maintenance', current_time, cooldown=86400):
            await self.action_manager.submit_action(
                'perform_maintenance',
                {
                    'engine': self.engine,
                    'session_factory': self.session_factory,
                    'maintenance_type': 'routine',
                    'metrics': metrics.__dict__,
                    'severity': 'warning'
                },
                priority=ActionPriority.LOW
            )
            self._last_action_time['perform_maintenance'] = current_time

    async def _perform_maintenance_checks(self, metrics: DatabaseMetrics) -> None:
        """Perform regular maintenance checks."""
        current_time = time.time()

        # Check if maintenance is due
        if current_time - self._last_maintenance > self.MAINTENANCE_INTERVAL:
            await self.action_manager.submit_action(
                'perform_db_maintenance',
                {
                    'metrics': metrics.__dict__,
                    'maintenance_type': 'routine'
                },
                priority=ActionPriority.LOW
            )
            self._last_maintenance = current_time

    def _detect_significant_changes(self, metrics: DatabaseMetrics) -> bool:
        """Detect if significant changes occurred in database state."""
        if not self._previous_metrics:
            return False

        # Define significance thresholds
        POOL_CHANGE_THRESHOLD = 5
        QUERY_TIME_THRESHOLD = 0.5
        INTEGRITY_THRESHOLD = 0.05

        return any([
            abs(metrics.pool_checked_out - self._previous_metrics.pool_checked_out) > POOL_CHANGE_THRESHOLD,
            abs(metrics.avg_query_time - self._previous_metrics.avg_query_time) > QUERY_TIME_THRESHOLD,
            abs(metrics.data_integrity_score - self._previous_metrics.data_integrity_score) > INTEGRITY_THRESHOLD,
            metrics.failed_queries_count > self._previous_metrics.failed_queries_count,
            metrics.deadlock_count > self._previous_metrics.deadlock_count
        ])

    def _update_observer_metrics(self,
                               metrics: DatabaseMetrics,
                               health: DatabaseHealthStatus) -> None:
        """Update internal metrics state."""
        self._metrics.update({
            'pool_utilization': metrics.pool_checked_out / metrics.pool_size * 100,
            'connection_count': metrics.connection_count,
            'avg_query_time': metrics.avg_query_time,
            'slow_queries': metrics.slow_queries_count,
            'failed_queries': metrics.failed_queries_count,
            'deadlocks': metrics.deadlock_count,
            'integrity_score': metrics.data_integrity_score,
            'health_status': health.overall_health,
            'last_update': time.time()
        })

        # Keep performance history
        self._performance_history.append({
            'timestamp': time.time(),
            'avg_query_time': metrics.avg_query_time,
            'connection_count': metrics.connection_count
        })

        # Limit history size
        if len(self._performance_history) > 1000:
            self._performance_history = self._performance_history[-1000:]

    def _get_disk_space(self) -> int:
        """Get available disk space in bytes."""
        try:
            with self.session_factory() as session:
                result = session.execute(text("""
                    SELECT @@datadir as datadir
                """)).scalar()

                if result:
                    import os
                    import psutil
                    usage = psutil.disk_usage(os.path.dirname(result))
                    return usage.total
                return 0
        except Exception as e:
            logger.error(f"Error getting disk space: {e}")
            return 0

    # Constants
    MAINTENANCE_INTERVAL = 86400  # 24 hours in seconds