# src/actions/database_actions.py

from typing import Dict, Any, Optional
from dataclasses import dataclass
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from src.core.actions import Action, ActionValidationError
from src.core.events import Event, EventType
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class DatabaseActionResult:
    """Result of a database management action."""
    success: bool
    changes_made: Dict[str, Any]
    action_taken: str
    metrics_before: Dict[str, Any]
    metrics_after: Dict[str, Any]
    details: Optional[str] = None

class BaseDatabaseAction(Action):
    """Base class for database management actions."""

    def __init__(self, context):
        super().__init__(context)
        self.engine: Optional[Engine] = None
        self.session_factory = None
        self._metrics_before: Dict[str, Any] = {}

    async def validate(self) -> None:
        """Validate base database action parameters."""
        if 'severity' not in self.context.params:
            raise ActionValidationError("Severity level must be specified")

        if self.context.params['severity'] not in ['warning', 'critical']:
            raise ActionValidationError("Invalid severity level")

        if 'engine' not in self.context.params:
            raise ActionValidationError("Database engine must be provided")

        if 'session_factory' not in self.context.params:
            raise ActionValidationError("Session factory must be provided")

        self.engine = self.context.params['engine']
        self.session_factory = self.context.params['session_factory']

    async def _emit_database_event(self,
                                 event_type: EventType,
                                 data: Dict[str, Any]) -> None:
        """Emit database-related event."""
        await self.emit_event(
            event_type,
            {
                **data,
                'action_id': self.context.id,
                'timestamp': time.time()
            }
        )

    async def _collect_current_metrics(self) -> Dict[str, Any]:
        """Collect current database metrics."""
        metrics = {}
        pool = self.engine.pool

        if isinstance(pool, QueuePool):
            metrics.update({
                'pool_size': pool.size(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'available': pool.size() - pool.checkedout()
            })

        return metrics

class HandleConnectionCrisisAction(BaseDatabaseAction):
    """Handles critical connection pool issues."""
    def __init__(self, context):
        super().__init__(context)
        self._killed_queries = 0  # Initialize here

    async def validate(self) -> None:
        """Validate connection crisis parameters."""
        await super().validate()
        if 'metrics' not in self.context.params:
            raise ActionValidationError("Current metrics must be provided")

    async def execute(self) -> None:
        """Execute connection crisis handling."""
        try:
            # Store initial metrics
            self._metrics_before = await self._collect_current_metrics()

            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'critical',
                    'component': 'connection_pool',
                    'metrics': self._metrics_before
                }
            )

            # Force close idle connections
            result = await self._force_close_idle_connections()

            # Emit completion event
            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'recovering',
                    'component': 'connection_pool',
                    'action_result': result.__dict__
                }
            )

        except Exception as e:
            logger.error(f"Failed to handle connection crisis: {e}", exc_info=True)
            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'error',
                    'component': 'connection_pool',
                    'error': str(e)
                }
            )
            raise

    async def _force_close_idle_connections(self) -> DatabaseActionResult:
        """Force close idle connections to free up pool."""
        pool = self.engine.pool
        closed_count = 0

        if isinstance(pool, QueuePool):
            try:
                # Dispose connections that have been in the pool too long
                pool.dispose()
                closed_count = pool.size() - pool.checkedout()

                # Collect metrics after action
                metrics_after = await self._collect_current_metrics()

                return DatabaseActionResult(
                    success=True,
                    changes_made={'connections_closed': closed_count},
                    action_taken='force_close_idle',
                    metrics_before=self._metrics_before,
                    metrics_after=metrics_after,
                    details=f"Closed {closed_count} idle connections"
                )

            except Exception as e:
                logger.error(f"Failed to close idle connections: {e}")
                return DatabaseActionResult(
                    success=False,
                    changes_made={},
                    action_taken='force_close_idle_failed',
                    metrics_before=self._metrics_before,
                    metrics_after=await self._collect_current_metrics(),
                    details=str(e)
                )

    async def rollback(self) -> None:
        """No rollback needed for connection crisis handling."""
        pass

class OptimizeConnectionsAction(BaseDatabaseAction):
    """Optimizes database connection pool settings."""

    async def execute(self) -> None:
        """Execute connection optimization."""
        try:
            # Store initial metrics
            self._metrics_before = await self._collect_current_metrics()

            # Analyze current pool usage
            pool = self.engine.pool
            if isinstance(pool, QueuePool):
                current_size = pool.size()
                checked_out = pool.checkedout()
                utilization = checked_out / current_size if current_size > 0 else 1

                if utilization > 0.8:  # High utilization
                    result = await self._increase_pool_size()
                elif utilization < 0.2:  # Low utilization
                    result = await self._decrease_pool_size()
                else:
                    result = DatabaseActionResult(
                        success=True,
                        changes_made={},
                        action_taken='no_change_needed',
                        metrics_before=self._metrics_before,
                        metrics_after=self._metrics_before,
                        details="Pool size optimal"
                    )

                await self._emit_database_event(
                    EventType.SERVICE_HEALTH_CHANGED,
                    {
                        'status': 'optimized',
                        'component': 'connection_pool',
                        'action_result': result.__dict__
                    }
                )

        except Exception as e:
            logger.error(f"Failed to optimize connections: {e}", exc_info=True)
            raise

    async def _increase_pool_size(self) -> DatabaseActionResult:
        """Increase connection pool size."""
        pool = self.engine.pool
        if not isinstance(pool, QueuePool):
            return DatabaseActionResult(
                success=False,
                changes_made={},
                action_taken='increase_pool_failed',
                metrics_before=self._metrics_before,
                metrics_after=self._metrics_before,
                details="Not a QueuePool"
            )

        try:
            original_size = pool.size()
            new_size = min(original_size * 2, 100)  # Double size, max 100
            pool._pool.maxsize = new_size

            metrics_after = await self._collect_current_metrics()

            return DatabaseActionResult(
                success=True,
                changes_made={'pool_size_change': new_size - original_size},
                action_taken='increase_pool_size',
                metrics_before=self._metrics_before,
                metrics_after=metrics_after,
                details=f"Increased pool size from {original_size} to {new_size}"
            )

        except Exception as e:
            logger.error(f"Failed to increase pool size: {e}")
            return DatabaseActionResult(
                success=False,
                changes_made={},
                action_taken='increase_pool_failed',
                metrics_before=self._metrics_before,
                metrics_after=await self._collect_current_metrics(),
                details=str(e)
            )

    async def _decrease_pool_size(self) -> DatabaseActionResult:
        """Decrease connection pool size."""
        pool = self.engine.pool
        if not isinstance(pool, QueuePool):
            return DatabaseActionResult(
                success=False,
                changes_made={},
                action_taken='decrease_pool_failed',
                metrics_before=self._metrics_before,
                metrics_after=self._metrics_before,
                details="Not a QueuePool"
            )

        try:
            original_size = pool.size()
            new_size = max(original_size // 2, 5)  # Halve size, min 5
            pool._pool.maxsize = new_size

            metrics_after = await self._collect_current_metrics()

            return DatabaseActionResult(
                success=True,
                changes_made={'pool_size_change': new_size - original_size},
                action_taken='decrease_pool_size',
                metrics_before=self._metrics_before,
                metrics_after=metrics_after,
                details=f"Decreased pool size from {original_size} to {new_size}"
            )

        except Exception as e:
            logger.error(f"Failed to decrease pool size: {e}")
            return DatabaseActionResult(
                success=False,
                changes_made={},
                action_taken='decrease_pool_failed',
                metrics_before=self._metrics_before,
                metrics_after=await self._collect_current_metrics(),
                details=str(e)
            )

    async def rollback(self) -> None:
        """Restore original pool size if needed."""
        if hasattr(self, '_original_pool_size'):
            pool = self.engine.pool
            if isinstance(pool, QueuePool):
                pool._pool.maxsize = self._original_pool_size

class HandlePerformanceCrisisAction(BaseDatabaseAction):
    """Handles critical performance issues."""
    def __init__(self, context):
        super().__init__(context)
        self._killed_queries = 0  # Initialize here

    async def validate(self) -> None:
        """Validate performance crisis parameters."""
        await super().validate()
        required = ['metrics', 'session_factory']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

    async def execute(self) -> None:
        try:
            session_factory = self.context.params['session_factory']
            self._metrics_before = await self._collect_current_metrics()

            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'critical',
                    'component': 'performance',
                    'metrics': self._metrics_before
                }
            )

            with session_factory() as session:
                # Kill long-running queries if critical
                if self.context.params['severity'] == 'critical':
                    await self._kill_long_running_queries(session)

                # Flush caches and buffers
                await self._optimize_buffers(session)

                # Update statistics for query optimizer
                await self._update_statistics(session)

            metrics_after = await self._collect_current_metrics()

            result = DatabaseActionResult(
                success=True,
                changes_made={
                    'queries_killed': self._killed_queries,
                    'buffers_flushed': True,
                    'stats_updated': True
                },
                action_taken='performance_crisis_handled',
                metrics_before=self._metrics_before,
                metrics_after=metrics_after,
                details="Performed emergency performance optimization"
            )

            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'recovering',
                    'component': 'performance',
                    'action_result': result.__dict__
                }
            )

        except Exception as e:
            logger.error(f"Failed to handle performance crisis: {e}", exc_info=True)
            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'error',
                    'component': 'performance',
                    'error': str(e)
                }
            )
            raise

    async def _kill_long_running_queries(self, session) -> int:
        """Kill queries running longer than threshold."""
        try:
            # Find long-running queries
            result = session.execute(text("""
                SELECT ID, TIME, INFO
                FROM information_schema.processlist
                WHERE COMMAND != 'Sleep'
                AND TIME > 300  -- 5 minutes
                AND USER != 'system user'
            """))

            killed_count = 0
            for row in result:
                try:
                    session.execute(text(f"KILL {row.ID}"))
                    killed_count += 1
                except Exception as e:
                    logger.error(f"Failed to kill query {row.ID}: {e}")

            self._killed_queries = killed_count
            return killed_count

        except Exception as e:
            logger.error(f"Error killing long queries: {e}")
            return 0

    async def _optimize_buffers(self, session) -> None:
        """Optimize database buffers."""
        try:
            # Flush tables
            session.execute(text("FLUSH TABLES"))
            # Flush query cache
            session.execute(text("FLUSH QUERY CACHE"))
            # Flush buffer pool
            session.execute(text("SET GLOBAL innodb_buffer_pool_dump_now = 1"))
        except Exception as e:
            logger.error(f"Failed to optimize buffers: {e}")

    async def _update_statistics(self, session) -> None:
        """Update table statistics."""
        try:
            # Get all tables
            tables = session.execute(text("""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
            """), {"schema": self.engine.url.database}).scalars().all()

            # Analyze tables
            for table in tables:
                try:
                    session.execute(text(f"ANALYZE TABLE {table}"))
                except Exception as e:
                    logger.error(f"Failed to analyze table {table}: {e}")

        except Exception as e:
            logger.error(f"Failed to update statistics: {e}")

    async def rollback(self) -> None:
        """No rollback for performance crisis handling."""
        pass

class HandleStorageCrisisAction(BaseDatabaseAction):
    """Handles critical storage issues."""

    async def validate(self) -> None:
        """Validate storage crisis parameters."""
        await super().validate()
        required = ['metrics', 'session_factory']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

    async def execute(self) -> None:
        try:
            session_factory = self.context.params['session_factory']
            self._metrics_before = await self._collect_current_metrics()

            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'critical',
                    'component': 'storage',
                    'metrics': self._metrics_before
                }
            )

            with session_factory() as session:
                # Emergency cleanup actions
                space_freed = await self._emergency_cleanup(session)

                # Optimize table spaces
                optimized_tables = await self._optimize_table_spaces(session)

                metrics_after = await self._collect_current_metrics()

                result = DatabaseActionResult(
                    success=True,
                    changes_made={
                        'space_freed_mb': space_freed,
                        'tables_optimized': optimized_tables
                    },
                    action_taken='storage_crisis_handled',
                    metrics_before=self._metrics_before,
                    metrics_after=metrics_after,
                    details=f"Freed {space_freed}MB of space"
                )

                await self._emit_database_event(
                    EventType.SERVICE_HEALTH_CHANGED,
                    {
                        'status': 'recovering',
                        'component': 'storage',
                        'action_result': result.__dict__
                    }
                )

        except Exception as e:
            logger.error(f"Failed to handle storage crisis: {e}", exc_info=True)
            await self._emit_database_event(
                EventType.SERVICE_HEALTH_CHANGED,
                {
                    'status': 'error',
                    'component': 'storage',
                    'error': str(e)
                }
            )
            raise

    async def _emergency_cleanup(self, session) -> float:
        """Perform emergency cleanup to free space."""
        space_freed = 0.0
        try:
            # Truncate old temporary tables if they exist
            result = session.execute(text("""
                SELECT TABLE_NAME,
                       ((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) as size_mb
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
                AND TABLE_NAME LIKE 'temp_%'
            """), {"schema": self.engine.url.database})

            for row in result:
                try:
                    session.execute(text(f"DROP TABLE IF EXISTS {row.TABLE_NAME}"))
                    space_freed += row.size_mb
                except Exception as e:
                    logger.error(f"Failed to drop temp table {row.TABLE_NAME}: {e}")

            # Cleanup old data based on retention policy
            old_data = await self._cleanup_old_data(session)
            space_freed += old_data

            return space_freed

        except Exception as e:
            logger.error(f"Error during emergency cleanup: {e}")
            return space_freed

    async def _cleanup_old_data(self, session) -> float:
        """Clean up old data based on retention policy."""
        space_freed = 0.0
        try:
            # Get retention period from configuration
            retention_days = 365  # 1 year default
            cutoff_time = int(time.time() * 1000) - (retention_days * 24 * 60 * 60 * 1000)

            # Delete old data
            result = session.execute(text("""
                DELETE FROM kline_data
                WHERE start_time < :cutoff_time
            """), {"cutoff_time": cutoff_time})

            # Estimate space freed
            if result.rowcount:
                space_freed = result.rowcount * 0.0001  # Estimate MB per record

            return space_freed

        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return space_freed

    async def _optimize_table_spaces(self, session) -> int:
        """Optimize table spaces to reclaim storage."""
        optimized_count = 0
        try:
            # Get fragmented tables
            result = session.execute(text("""
                SELECT TABLE_NAME,
                       DATA_FREE,
                       ((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) as size_mb
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
                AND ENGINE = 'InnoDB'
                AND DATA_FREE > 0
            """), {"schema": self.engine.url.database})

            for row in result:
                try:
                    # Optimize table if significant space can be reclaimed
                    if row.DATA_FREE > 1024 * 1024:  # More than 1MB free
                        session.execute(text(f"OPTIMIZE TABLE {row.TABLE_NAME}"))
                        optimized_count += 1
                except Exception as e:
                    logger.error(f"Failed to optimize table {row.TABLE_NAME}: {e}")

            return optimized_count

        except Exception as e:
            logger.error(f"Failed to optimize table spaces: {e}")
            return optimized_count

    async def rollback(self) -> None:
        """No rollback for storage crisis handling."""
        pass

# src/actions/database_actions.py (continued)

class VerifyDataIntegrityAction(BaseDatabaseAction):
    """Verifies and repairs data integrity issues."""

    async def validate(self) -> None:
        """Validate integrity check parameters."""
        await super().validate()
        required = ['session_factory', 'scope']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

        valid_scopes = ['full', 'incremental', 'targeted']
        if self.context.params['scope'] not in valid_scopes:
            raise ActionValidationError(f"Invalid scope: {self.context.params['scope']}")

    async def execute(self) -> None:
        try:
            session_factory = self.context.params['session_factory']
            scope = self.context.params['scope']
            self._metrics_before = await self._collect_current_metrics()

            await self._emit_database_event(
                EventType.DATA_INTEGRITY_CHECK_STARTED,
                {
                    'scope': scope,
                    'metrics': self._metrics_before
                }
            )

            with session_factory() as session:
                if scope == 'full':
                    result = await self._perform_full_integrity_check(session)
                elif scope == 'incremental':
                    result = await self._perform_incremental_check(session)
                else:  # targeted
                    result = await self._perform_targeted_check(session)

                await self._emit_database_event(
                    EventType.DATA_INTEGRITY_CHECK_COMPLETED,
                    {
                        'scope': scope,
                        'result': result.__dict__
                    }
                )

        except Exception as e:
            logger.error(f"Failed to verify data integrity: {e}", exc_info=True)
            await self._emit_database_event(
                EventType.DATA_INTEGRITY_CHECK_FAILED,
                {
                    'scope': scope,
                    'error': str(e)
                }
            )
            raise

    async def _perform_full_integrity_check(self, session) -> DatabaseActionResult:
        """Perform full database integrity check."""
        try:
            integrity_issues = []
            repairs_made = []

            # Check table structures
            tables_result = await self._verify_table_structures(session)
            integrity_issues.extend(tables_result['issues'])
            repairs_made.extend(tables_result['repairs'])

            # Check data constraints
            constraints_result = await self._verify_data_constraints(session)
            integrity_issues.extend(constraints_result['issues'])
            repairs_made.extend(constraints_result['repairs'])

            # Check for orphaned records
            orphans_result = await self._verify_orphaned_records(session)
            integrity_issues.extend(orphans_result['issues'])
            repairs_made.extend(orphans_result['repairs'])

            metrics_after = await self._collect_current_metrics()

            return DatabaseActionResult(
                success=len(integrity_issues) == 0,
                changes_made={'repairs': repairs_made},
                action_taken='full_integrity_check',
                metrics_before=self._metrics_before,
                metrics_after=metrics_after,
                details=f"Found {len(integrity_issues)} issues, made {len(repairs_made)} repairs"
            )

        except Exception as e:
            logger.error(f"Full integrity check failed: {e}")
            raise

    async def _verify_table_structures(self, session) -> Dict[str, list]:
        """Verify table structures and repair if needed."""
        issues = []
        repairs = []
        try:
            # Check each table
            results = session.execute(text("""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = :schema
            """), {"schema": self.engine.url.database})

            for row in results:
                table_name = row[0]
                try:
                    check_result = session.execute(text(f"CHECK TABLE {table_name} EXTENDED")).first()
                    if check_result and check_result.Msg_type == 'error':
                        issues.append(f"Table {table_name}: {check_result.Msg_text}")
                        # Attempt repair
                        repair_result = session.execute(text(f"REPAIR TABLE {table_name}")).first()
                        if repair_result and repair_result.Msg_type != 'error':
                            repairs.append(f"Repaired table {table_name}")
                except Exception as e:
                    logger.error(f"Error checking table {table_name}: {e}")
                    issues.append(f"Check failed for {table_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error verifying table structures: {e}")
            issues.append(f"Structure verification failed: {str(e)}")

        return {'issues': issues, 'repairs': repairs}

    async def _verify_data_constraints(self, session) -> Dict[str, list]:
        """Verify data constraints and relationships."""
        issues = []
        repairs = []
        try:
            # Check for invalid kline data
            invalid_klines = session.execute(text("""
                SELECT k.id, k.symbol_id, k.start_time
                FROM kline_data k
                WHERE k.high_price < k.low_price
                   OR k.open_price < k.low_price
                   OR k.open_price > k.high_price
                   OR k.close_price < k.low_price
                   OR k.close_price > k.high_price
            """)).fetchall()

            for invalid in invalid_klines:
                issues.append(f"Invalid kline data: ID {invalid.id}")
                try:
                    # Delete invalid records
                    session.execute(text("""
                        DELETE FROM kline_data
                        WHERE id = :id
                    """), {"id": invalid.id})
                    repairs.append(f"Removed invalid kline: ID {invalid.id}")
                except Exception as e:
                    logger.error(f"Failed to remove invalid kline {invalid.id}: {e}")

        except Exception as e:
            logger.error(f"Error verifying data constraints: {e}")
            issues.append(f"Constraint verification failed: {str(e)}")

        return {'issues': issues, 'repairs': repairs}

    async def _verify_orphaned_records(self, session) -> Dict[str, list]:
        """Check for and clean up orphaned records."""
        issues = []
        repairs = []
        try:
            # Find orphaned klines
            orphans = session.execute(text("""
                SELECT k.id
                FROM kline_data k
                LEFT JOIN symbols s ON k.symbol_id = s.id
                WHERE s.id IS NULL
            """)).fetchall()

            if orphans:
                issues.append(f"Found {len(orphans)} orphaned kline records")
                try:
                    # Remove orphans
                    session.execute(text("""
                        DELETE k FROM kline_data k
                        LEFT JOIN symbols s ON k.symbol_id = s.id
                        WHERE s.id IS NULL
                    """))
                    repairs.append(f"Removed {len(orphans)} orphaned records")
                except Exception as e:
                    logger.error(f"Failed to remove orphaned records: {e}")

        except Exception as e:
            logger.error(f"Error checking for orphaned records: {e}")
            issues.append(f"Orphan check failed: {str(e)}")

        return {'issues': issues, 'repairs': repairs}

    async def rollback(self) -> None:
        """No rollback for integrity checks."""
        pass

class PerformMaintenanceAction(BaseDatabaseAction):
    """Performs routine database maintenance tasks."""

    async def validate(self) -> None:
        """Validate maintenance parameters."""
        await super().validate()
        if 'maintenance_type' not in self.context.params:
            raise ActionValidationError("Maintenance type must be specified")

        valid_types = ['routine', 'scheduled', 'emergency']
        if self.context.params['maintenance_type'] not in valid_types:
            raise ActionValidationError(f"Invalid maintenance type")

    async def execute(self) -> None:
        try:
            maintenance_type = self.context.params['maintenance_type']
            self._metrics_before = await self._collect_current_metrics()

            await self._emit_database_event(
                EventType.MAINTENANCE_STARTED,
                {
                    'type': maintenance_type,
                    'metrics': self._metrics_before
                }
            )

            # Perform maintenance tasks
            tasks_completed = []

            # Analyze and optimize tables
            if await self._analyze_and_optimize():
                tasks_completed.append('table_optimization')

            # Purge old data
            if await self._purge_old_data():
                tasks_completed.append('data_purge')

            # Update statistics
            if await self._update_statistics():
                tasks_completed.append('statistics_update')

            # Check and repair tables
            if maintenance_type in ['scheduled', 'emergency']:
                if await self._check_and_repair_tables():
                    tasks_completed.append('table_repair')

            metrics_after = await self._collect_current_metrics()

            result = DatabaseActionResult(
                success=len(tasks_completed) > 0,
                changes_made={'completed_tasks': tasks_completed},
                action_taken=f'{maintenance_type}_maintenance',
                metrics_before=self._metrics_before,
                metrics_after=metrics_after,
                details=f"Completed {len(tasks_completed)} maintenance tasks"
            )

            await self._emit_database_event(
                EventType.MAINTENANCE_COMPLETED,
                {
                    'type': maintenance_type,
                    'result': result.__dict__
                }
            )

        except Exception as e:
            logger.error(f"Failed to perform maintenance: {e}", exc_info=True)
            await self._emit_database_event(
                EventType.MAINTENANCE_FAILED,
                {
                    'type': maintenance_type,
                    'error': str(e)
                }
            )
            raise

    async def _analyze_and_optimize(self) -> bool:
        """Analyze and optimize database tables."""
        try:
            with self.session_factory() as session:
                # Get tables needing optimization
                fragmented_tables = session.execute(text("""
                    SELECT TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = :schema
                    AND DATA_FREE > 0
                """), {"schema": self.engine.url.database}).fetchall()

                for table in fragmented_tables:
                    try:
                        session.execute(text(f"OPTIMIZE TABLE {table.TABLE_NAME}"))
                    except Exception as e:
                        logger.error(f"Failed to optimize table {table.TABLE_NAME}: {e}")

            return True
        except Exception as e:
            logger.error(f"Failed to analyze and optimize tables: {e}")
            return False

    async def _purge_old_data(self) -> bool:
        """Purge old data based on retention policy."""
        try:
            with self.session_factory() as session:
                # Set retention period (e.g., 1 year)
                retention_period = 365 * 24 * 60 * 60 * 1000  # milliseconds
                cutoff_time = int(time.time() * 1000) - retention_period

                # Delete old data
                session.execute(text("""
                    DELETE FROM kline_data
                    WHERE start_time < :cutoff_time
                """), {"cutoff_time": cutoff_time})

            return True
        except Exception as e:
            logger.error(f"Failed to purge old data: {e}")
            return False

    async def _update_statistics(self) -> bool:
        """Update database statistics."""
        try:
            with self.session_factory() as session:
                tables = session.execute(text("""
                    SELECT TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = :schema
                """), {"schema": self.engine.url.database}).fetchall()

                for table in tables:
                    try:
                        session.execute(text(f"ANALYZE TABLE {table.TABLE_NAME}"))
                    except Exception as e:
                        logger.error(f"Failed to analyze table {table.TABLE_NAME}: {e}")

            return True
        except Exception as e:
            logger.error(f"Failed to update statistics: {e}")
            return False

    async def _check_and_repair_tables(self) -> bool:
        """Check and repair database tables."""
        try:
            with self.session_factory() as session:
                tables = session.execute(text("""
                    SELECT TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = :schema
                """), {"schema": self.engine.url.database}).fetchall()

                for table in tables:
                    try:
                        check_result = session.execute(text(f"CHECK TABLE {table.TABLE_NAME} EXTENDED")).first()
                        if check_result and check_result.Msg_type == 'error':
                            session.execute(text(f"REPAIR TABLE {table.TABLE_NAME}"))
                    except Exception as e:
                        logger.error(f"Failed to check/repair table {table.TABLE_NAME}: {e}")

            return True
        except Exception as e:
            logger.error(f"Failed to check and repair tables: {e}")
            return False

    async def rollback(self) -> None:
        """No rollback for maintenance tasks."""
        pass

# Factory for creating database actions
class DatabaseActionFactory:
    """Factory for creating database management actions."""

    _actions = {
        'handle_connection_crisis': HandleConnectionCrisisAction,
        'optimize_connections': OptimizeConnectionsAction,
        'handle_performance_crisis': HandlePerformanceCrisisAction,
        'handle_storage_crisis': HandleStorageCrisisAction,
        'verify_data_integrity': VerifyDataIntegrityAction,
        'perform_maintenance': PerformMaintenanceAction
    }

    @classmethod
    def create_action(cls, action_type: str, context) -> Action:
        """Create appropriate database action."""
        action_class = cls._actions.get(action_type)
        if not action_class:
            raise ValueError(f"Unknown action type: {action_type}")
        return action_class(context)