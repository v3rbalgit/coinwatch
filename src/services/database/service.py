# src/services/database/service.py

import asyncio
import os
from typing import Optional, AsyncGenerator, Union
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from contextlib import asynccontextmanager
from sqlalchemy import text

from ..base import ServiceBase
from ...services.database.recovery import DatabaseRecovery
from ...models.base import Base
from ...models import Kline, Symbol, TokenMetadata
from ...core.monitoring import DatabaseMetrics
from ...core.coordination import Command, CommandResult, MonitoringCommand, ServiceCoordinator
from ...core.exceptions import ServiceError, ValidationError
from ...config import DatabaseConfig
from ...utils.logger import LoggerSetup
from ...utils.domain_types import CriticalCondition, DatabaseErrorType, IsolationLevel, ServiceStatus
from ...utils.error import ErrorTracker
from ...utils.retry import RetryConfig, RetryStrategy
from ...utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class DatabaseService(ServiceBase):
    """
    Database service with connection pooling and session management

    Handles PostgreSQL connections with TimescaleDB support, providing:
    - Connection pooling
    - Transaction management
    - Health monitoring
    - Error handling
    - Resource optimization
    """

    def __init__(self, coordinator: ServiceCoordinator, config: DatabaseConfig):
        super().__init__(config)
        self.coordinator = coordinator
        self.engine: Optional[AsyncEngine] = None
        self._connection_url = config.url
        self._status = ServiceStatus.STOPPED
        self._start_time: Optional[int] = None
        self._timescale_config = config.timescale

        # Enhanced error tracking and recovery
        self._error_tracker = ErrorTracker()
        self._last_error: Optional[Exception] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._pool_lock = asyncio.Lock()

        # Initialize recovery manager
        self.recovery = DatabaseRecovery(self)

        # Configure retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            max_retries=3,
            jitter_factor=0.1
        ))
        self._configure_retry_strategy()

        # Track maintenance windows
        self._last_maintenance: Optional[int] = None
        self._maintenance_due: bool = False

        asyncio.create_task(self._register_command_handlers())

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior with PostgreSQL-specific error handling"""
        from sqlalchemy.exc import (
            OperationalError,
            InternalError,
            DisconnectionError,
            TimeoutError
        )

        # Retryable errors with specific handling
        self._retry_strategy.add_retryable_error(
            ConnectionError,
            OperationalError,        # Covers most PostgreSQL errors
            SQLAlchemyError,
            DisconnectionError,      # Connection issues
            TimeoutError             # Statement timeout
        )

        # Configure specific delays for different error types
        self._retry_strategy.configure_error_delays({
            OperationalError: RetryConfig(
                base_delay=0.1,        # Fast retry for operational errors
                max_delay=5.0,
                max_retries=5,
                jitter_factor=0.1
            ),
            DisconnectionError: RetryConfig(
                base_delay=2.0,        # Longer delay for connection issues
                max_delay=30.0,
                max_retries=3,
                jitter_factor=0.25
            )
        })

        # Non-retryable errors
        self._retry_strategy.add_non_retryable_error(
            ServiceError,              # Application errors
            ValidationError,           # Data validation errors
            InternalError              # Serious SQLAlchemy internal errors
        )

    async def start(self) -> None:
        """
        Start the database service with TimescaleDB initialization.

        Raises:
            ServiceError: If there's an error during the database initialization process.

        Returns:
            None
        """
        try:
            self._status = ServiceStatus.STARTING
            self._start_time = TimeUtils.get_current_timestamp()
            self._last_maintenance = TimeUtils.get_current_timestamp()
            logger.info("Starting database service")

            self.engine = self._create_engine()

            # Create tables if they don't exist
            async with self.engine.begin() as conn:
                try:
                    # Initialize TimescaleDB extensions
                    await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                    await conn.run_sync(Base.metadata.create_all)

                    logger.info("Database tables created successfully")
                except Exception as table_error:
                    logger.error(f"Error creating tables: {table_error}")
                    raise

            await self._setup_timescaledb(self.engine)

            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False  # Optimize for bulk operations
            )

            await self._register_command_handlers()

            # Start pool monitoring
            self._monitor_task = asyncio.create_task(self._monitor_database())

            self._status = ServiceStatus.RUNNING
            logger.info("Database service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Failed to start database service: {e}")
            raise ServiceError(f"Database initialization failed: {str(e)}")

    async def stop(self) -> None:
        """Cleanup database connections"""
        try:
            self._status = ServiceStatus.STOPPING

            await self._unregister_command_handlers()

            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass

            if self.engine:
                await self.engine.dispose()
                self.engine = None

            self._status = ServiceStatus.STOPPED
            logger.info("Database service stopped")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping database service: {e}")
            raise ServiceError(f"Database shutdown failed: {str(e)}")

    @asynccontextmanager
    async def get_session(self,
                          isolation_level: Optional[IsolationLevel] = None,
                          use_transaction: bool = True) -> AsyncGenerator[AsyncSession, None]:
        """
        Transaction management with PostgreSQL-specific optimizations

        Args:
            isolation_level: Transaction isolation level (READ COMMITTED, REPEATABLE READ, etc.)
            use_transaction: Whether to wrap session in a transaction (default: True)

        Returns:
            AsyncGenerator yielding an AsyncSession

        Raises:
            ServiceError: If database is not initialized or operation fails
        """
        if not self.session_factory:
            raise ServiceError("Database service not initialized")

        attempt = 0

        while True:
            try:
                session = self.session_factory()
                try:
                    if use_transaction:
                        async with session.begin():
                            if isolation_level:
                                await session.execute(
                                    text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level.value}")
                                )
                            yield session
                    else:
                        if isolation_level:
                            logger.warning("Isolation level ignored for non-transactional session")
                        yield session
                finally:
                    await session.close()
                break

            except Exception as e:
                # Track error for monitoring
                await self._error_tracker.record_error(
                    e,
                    context={
                        "isolation_level": isolation_level.value if isolation_level else None,
                        "attempt": attempt + 1
                    }
                )

                # Get retry strategy for this specific error
                should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                if should_retry:
                    attempt += 1
                    delay = self._retry_strategy.get_delay(attempt, e)

                    logger.warning(
                        f"Database error, retry {attempt} after {delay:.2f}s: {str(e)}"
                    )

                    await asyncio.sleep(delay)
                    continue

                logger.error(
                    f"Database error not retryable ({reason}): {str(e)}"
                )
                raise ServiceError(f"Database operation failed: {str(e)}") from e

    def _create_engine(self) -> AsyncEngine:
        """
        Create and configure a SQLAlchemy asynchronous engine with PostgreSQL optimizations.

        This method initializes an AsyncEngine with specific configurations for PostgreSQL,
        including connection pooling, JSON handling, and health checks. It also sets up
        PostgreSQL-specific session configurations for optimal performance.

        Returns:
            AsyncEngine: A configured SQLAlchemy asynchronous engine instance optimized for PostgreSQL.

        Note:
            - Uses AsyncAdaptedQueuePool for connection pooling.
            - Disables SQLAlchemy's JSON serialization to use PostgreSQL's native JSON handling.
            - Enables connection health checks with pool_pre_ping.
            - Sets up PostgreSQL session for parallel query execution and statement timeout.
        """
        return create_async_engine(
            self._connection_url,
            pool_pre_ping=True,    # Enable connection health checks
            json_serializer=None,  # Use PostgreSQL native JSON handling
            json_deserializer=None,
            **self._config.get_engine_options()
        )

    async def _setup_timescaledb(self, engine: AsyncEngine) -> None:
        """Setup TimescaleDB features with separate transactions"""
        try:
            # Create hypertable if not exists
            await self._create_hypertable(engine)

            # Setup compression if not already set
            await self._setup_compression(engine)

            # Setup retention policy if configured and not already set
            await self._setup_retention(engine)

            # Create materialized views if not exists
            await self._create_materialized_views(engine)

            # Setup continuous aggregate policies if not already set
            await self._setup_continuous_aggregate_policies(engine)

            logger.info("TimescaleDB setup completed successfully")

        except Exception as e:
            logger.error(f"Error setting up TimescaleDB: {e}")
            raise

    async def _create_hypertable(self, engine: AsyncEngine) -> None:
        """Create hypertable if it does not already exist"""
        async with engine.begin() as conn:
            try:
                await conn.execute(text("""
                    SELECT create_hypertable(
                        'kline_data',
                        'timestamp',
                        chunk_time_interval => INTERVAL '1 week',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                """))
                logger.info("Hypertable created or already exists")
            except Exception as e:
                logger.error(f"Error creating hypertable: {e}")
                raise

    async def _setup_compression(self, engine: AsyncEngine) -> None:
        """Setup compression policies if not already set"""
        async with engine.begin() as conn:
            try:
                # Enable compression on the table
                await conn.execute(text("""
                    ALTER TABLE kline_data SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'timestamp',
                        timescaledb.compress_segmentby = 'symbol_id,timeframe'
                    );
                """))
                logger.info("Compression settings applied to kline_data table")

                # Add compression policy
                await conn.execute(text("""
                    SELECT add_compression_policy(
                        'kline_data',
                        INTERVAL '30 days',
                        if_not_exists => TRUE
                    );
                """))
                logger.info("Compression policy updated")
            except Exception as e:
                logger.error(f"Error setting up compression: {e}")
                raise

    async def _setup_retention(self, engine: AsyncEngine) -> None:
        """Setup retention policies if configured and not already set"""
        if self._timescale_config.retention_days and self._timescale_config.retention_days > 0:
            async with engine.begin() as conn:
                try:
                    # Add retention policy
                    await conn.execute(text("""
                        SELECT add_retention_policy(
                            'kline_data',
                            INTERVAL :days_interval,
                            if_not_exists => TRUE
                        );
                    """), {
                        "days_interval": f"{self._timescale_config.retention_days} days"
                    })
                    logger.info("Retention policy updated")
                except Exception as e:
                    logger.error(f"Error setting up retention policy: {e}")
                    raise
        else:
            logger.info("Retention policy not configured")

    async def _create_materialized_views(self, engine: AsyncEngine) -> None:
        """Create materialized views if they do not already exist"""
        default_timeframe = os.getenv('DEFAULT_TIMEFRAME', '5')
        view_definitions = [
            {
                "name": "kline_1h",
                "bucket": "1 hour",
                "timeframe": "60",
                "end_interval": "1 hour",
                "schedule_interval": "5 minutes"
            },
            {
                "name": "kline_4h",
                "bucket": "4 hours",
                "timeframe": "240",
                "end_interval": "4 hours",
                "schedule_interval": "20 minutes"
            },
            {
                "name": "kline_1d",
                "bucket": "1 day",
                "timeframe": "D",
                "end_interval": "1 day",
                "schedule_interval": "1 hour"
            }
        ]

        async with engine.begin() as conn:
            for view in view_definitions:
                try:
                    # Create materialized view if it does not exist
                    await conn.execute(text(f"""
                        CREATE MATERIALIZED VIEW IF NOT EXISTS {view['name']}
                        WITH (timescaledb.continuous) AS
                        SELECT
                            time_bucket('{view['bucket']}', timestamp, 'UTC') AS bucket,
                            symbol_id,
                            '{view['timeframe']}' as timeframe,
                            first(open_price, timestamp) as open_price,
                            max(high_price) as high_price,
                            min(low_price) as low_price,
                            last(close_price, timestamp) as close_price,
                            sum(volume) as volume,
                            sum(turnover) as turnover
                        FROM kline_data
                        WHERE
                            timeframe = '{default_timeframe}'
                            AND timestamp < time_bucket('{view['bucket']}', now(), 'UTC')
                        GROUP BY
                            time_bucket('{view['bucket']}', timestamp, 'UTC'),
                            symbol_id
                        WITH NO DATA;
                    """))
                    logger.info(f"Materialized view '{view['name']}' created or already exists")
                except ProgrammingError as pe:
                    if 'already exists' in str(pe):
                        logger.warning(f"Materialized view '{view['name']}' already exists.")
                    else:
                        logger.error(f"Programming error while creating materialized view '{view['name']}': {pe}")
                        raise
                except Exception as e:
                    logger.error(f"Error creating materialized view '{view['name']}': {e}")
                    raise

    async def _setup_continuous_aggregate_policies(self, engine: AsyncEngine) -> None:
        """Setup continuous aggregate policies if not already set"""
        policies = [
            {
                "view_name": "kline_1h",
                "start_offset": "3 hours",
                "end_offset": "1 hour",
                "schedule_interval": "5 minutes"
            },
            {
                "view_name": "kline_4h",
                "start_offset": "12 hours",
                "end_offset": "4 hours",
                "schedule_interval": "20 minutes"
            },
            {
                "view_name": "kline_1d",
                "start_offset": "3 days",
                "end_offset": "1 day",
                "schedule_interval": "1 hour"
            }
        ]

        async with engine.begin() as conn:
            for policy in policies:
                try:
                    # Add continuous aggregate policy
                    await conn.execute(text(f"""
                        SELECT add_continuous_aggregate_policy('{policy['view_name']}',
                            start_offset => INTERVAL '{policy['start_offset']}',
                            end_offset => INTERVAL '{policy['end_offset']}',
                            schedule_interval => INTERVAL '{policy['schedule_interval']}',
                            if_not_exists => TRUE
                        );
                    """))
                    logger.info(f"Continuous aggregate policy for {policy['view_name']} updated")
                except Exception as e:
                    logger.error(f"Error setting up continuous aggregate policy for {policy['view_name']}: {e}")
                    raise

    async def _register_command_handlers(self) -> None:
        """Register command handlers for service monitoring"""
        handlers = {
            MonitoringCommand.REPORT_METRICS: self._handle_metrics_report
        }

        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _unregister_command_handlers(self) -> None:
        """Register command handlers for service monitoring"""
        handlers = {
            MonitoringCommand.REPORT_METRICS: self._handle_metrics_report
        }

        for command, handler in handlers.items():
            await self.coordinator.unregister_handler(command, handler)
            logger.debug(f"Unregistered handler for {command.value}")

    async def _handle_metrics_report(self, command: Command) -> CommandResult:
        """Handle metrics report command by returning current metrics"""
        try:
            metrics = await self._collect_metrics()
            return CommandResult.success(metrics)
        except Exception as e:
            logger.error(f"Error handling metrics report: {e}")
            return CommandResult.error(f"Failed to collect database metrics: {str(e)}")

    async def _collect_metrics(self) -> DatabaseMetrics:
        """Collect comprehensive database metrics"""
        try:
            async with self.get_session() as session:
                # Get connection stats
                result = await session.execute(text("""
                    SELECT count(*) as active_connections
                    FROM pg_stat_activity
                    WHERE application_name LIKE 'coinwatch%'
                    AND state = 'active';
                """))
                active_connections = result.scalar() or 0

                # Get deadlock stats
                result = await session.execute(text("""
                    SELECT count(*) as deadlock_count
                    FROM pg_locks blocked_locks
                    JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                    JOIN pg_locks blocking_locks
                        ON blocking_locks.pid != blocked_locks.pid
                        AND blocking_locks.granted
                    WHERE NOT blocked_locks.granted;
                """))
                deadlocks = result.scalar() or 0

                # Get long-running queries
                result = await session.execute(text("""
                    SELECT count(*) as long_running_count
                    FROM pg_stat_activity
                    WHERE application_name LIKE 'coinwatch%'
                    AND state = 'active'
                    AND NOW() - query_start > INTERVAL '25 seconds';
                """))
                long_queries = result.scalar() or 0

                # Add replication lag query
                result = await session.execute(text("""
                    SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))::INT
                    AS lag_seconds
                    WHERE pg_last_xact_replay_timestamp() IS NOT NULL;
                """))
                replication_lag = result.scalar() or 0

                # Calculate uptime
                uptime = 0.0
                if self._start_time is not None:
                    uptime = (TimeUtils.get_current_timestamp() - self._start_time) / 1000

                return DatabaseMetrics(
                    service_name="database",
                    status=self._status.value,
                    uptime_seconds=uptime,
                    last_error=str(self._last_error) if self._last_error else None,
                    error_count=len(self._error_tracker.get_recent_errors(60)),
                    warning_count=len([e for e in self._error_tracker.get_recent_errors(60)
                                    if 'warning' in str(e).lower()]),
                    timestamp=TimeUtils.get_current_datetime(),
                    additional_metrics={
                        'pool_recycle': self._config.pool_recycle,
                        'pool_timeout': self._config.pool_timeout,
                        'maintenance_window': self._config.maintenance_window
                    },
                    active_connections=active_connections,
                    pool_size=self._config.pool_size,
                    max_overflow=self._config.max_overflow,
                    available_connections=self._config.pool_size - active_connections,
                    deadlocks=deadlocks,
                    long_queries=long_queries,
                    maintenance_due=self._maintenance_due,
                    replication_lag_seconds=replication_lag
                )

        except Exception as e:
            logger.error(f"Error collecting database metrics: {e}")
            raise

    async def _monitor_database(self) -> None:
        """
        Comprehensive database monitoring checking all critical health metrics.
        """
        while True:
            try:
                if not self.engine:
                    await asyncio.sleep(1)
                    continue

                metrics = await self._collect_metrics()

                await self.recovery.check_and_recover(metrics)

                if metrics.deadlocks > 0:
                    await self.handle_critical_condition({
                        "type": DatabaseErrorType.DEADLOCK,
                        "severity": "warning" if metrics.deadlocks < 5 else "critical",
                        "message": f"Detected {metrics.deadlocks} deadlocks",
                        "timestamp": TimeUtils.get_current_timestamp(),
                        "error_type": DatabaseErrorType.DEADLOCK,
                        "context": {
                            "deadlock_count": metrics.deadlocks
                        }
                    })

                if (not self._last_maintenance or
                    TimeUtils.get_current_timestamp() - self._last_maintenance > self._config.maintenance_window * 1000):
                    await self.handle_critical_condition({
                        "type": DatabaseErrorType.MAINTENANCE_REQUIRED,
                        "severity": "warning",
                        "message": "Database maintenance required",
                        "timestamp": TimeUtils.get_current_timestamp(),
                        "error_type": DatabaseErrorType.MAINTENANCE_REQUIRED,
                        "context": {
                            "last_maintenance": self._last_maintenance
                        }
                    })

                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Database monitoring error: {e}")
                await asyncio.sleep(5)

    async def _handle_deadlock(self, condition: CriticalCondition) -> None:
        """Handle deadlock conditions with analysis"""
        async with self._pool_lock:
            error_frequency = await self._error_tracker.get_error_frequency(
                DatabaseErrorType.DEADLOCK,
                window_minutes=60
            )

            deadlock_count = condition["context"].get("deadlock_count", 0)

            if condition["severity"] == "critical" or error_frequency > 5:
                logger.error(
                    f"Severe deadlock situation: {condition['message']}, "
                    f"found {deadlock_count} deadlocks"
                )

                async with self.get_session() as session:
                    result = await session.execute(text("""
                        SELECT blocked_locks.pid AS blocked_pid,
                               blocking_locks.pid AS blocking_pid,
                               blocked_activity.query AS blocked_query,
                               blocking_activity.query AS blocking_query
                        FROM pg_catalog.pg_locks blocked_locks
                        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                        JOIN pg_catalog.pg_locks blocking_locks
                            ON blocking_locks.locktype = blocked_locks.locktype
                            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                            AND blocking_locks.pid != blocked_locks.pid
                        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
                        WHERE NOT blocked_locks.granted;
                    """))
                    deadlocks = result.fetchall()

                    if deadlocks:
                        logger.error(
                            f"Resolving {len(deadlocks)} deadlocks by terminating "
                            f"blocking transactions"
                        )
                        for deadlock in deadlocks:
                            if deadlock.blocking_pid:
                                await session.execute(text(
                                    f"SELECT pg_terminate_backend({deadlock.blocking_pid})"
                                ))
            else:
                logger.warning(f"Deadlock warning: {condition['message']}")

    async def _handle_maintenance_required(self, condition: CriticalCondition) -> None:
        """Handle maintenance requirements"""
        async with self._pool_lock:
            current_time = TimeUtils.get_current_timestamp()

            logger.info(f"Starting maintenance: {condition['message']}")
            attempt = 0

            while True:
                try:
                    # Use non-transactional session for VACUUM operations
                    async with self.get_session(use_transaction=False) as session:
                        logger.info("Running VACUUM ANALYZE on critical tables")
                        await session.execute(text("COMMIT"))  # Ensure no active transaction
                        await session.execute(text("VACUUM ANALYZE kline_data"))
                        await session.execute(text("VACUUM ANALYZE symbols"))

                    # Use transactional session for regular ANALYZE
                    async with self.get_session() as session:
                        logger.info("Updating table statistics")
                        await session.execute(text("ANALYZE"))

                    self._last_maintenance = current_time
                    self._maintenance_due = False
                    logger.info("Database maintenance completed successfully")
                    return

                except Exception as e:
                    should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                    if should_retry:
                        attempt += 1
                        delay = self._retry_strategy.get_delay(attempt, e)

                        logger.warning(
                            f"Maintenance error, "
                            f"retry {attempt} after {delay:.2f}s: {str(e)}"
                        )

                        delay = min(delay * 2, 300)  # Max 5 minutes
                        await asyncio.sleep(delay)
                        continue

                    logger.error(f"Maintenance failed: {e}")
                    self._maintenance_due = True
                    raise ServiceError(f"Database maintenance failed: {str(e)}")

    async def _reconfigure_pool(self, **config_updates: Union[int, bool]) -> None:
        """
        Safely reconfigure connection pool with enhanced error handling

        Args:
            **config_updates: Configuration updates as keyword arguments
        """
        async with self._pool_lock:
            old_engine = self.engine
            attempt = 0

            while True:
                try:
                    # Create dictionary of valid updates
                    validated_updates = {
                        k: v for k, v in config_updates.items()
                        if hasattr(self._config, k)
                    }

                    # Update configuration using the update method
                    self._config.update(validated_updates)

                    # Create new engine with updated config
                    self.engine = self._create_engine()
                    self.session_factory = async_sessionmaker(
                        bind=self.engine,
                        class_=AsyncSession,
                        expire_on_commit=False,
                        autoflush=False
                    )

                    # Verify new configuration
                    async with self.get_session() as session:
                        await session.execute(text("SELECT 1"))

                    if old_engine:
                        await old_engine.dispose()

                    logger.info(f"Pool reconfigured with {validated_updates}")
                    return

                except Exception as e:
                    should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                    if should_retry:
                        attempt += 1
                        delay = self._retry_strategy.get_delay(attempt, e)

                        logger.warning(
                            f"Pool reconfiguration error, "
                            f"retry {attempt} after {delay:.2f}s: {str(e)}"
                        )

                        await asyncio.sleep(delay)
                        continue

                    self.engine = old_engine
                    logger.error(f"Pool reconfiguration failed: {e}")
                    raise ServiceError(f"Pool reconfiguration failed: {str(e)}")

    async def _initiate_emergency_recovery(self, reason: str) -> None:
        """Emergency recovery procedure"""
        logger.critical(f"Initiating emergency recovery: {reason}")

        async with self._pool_lock:
            try:
                # Stop accepting new connections
                self._status = ServiceStatus.STOPPING

                # Dispose current engine
                if self.engine:
                    await self.engine.dispose()

                # Reset configuration to conservative values
                self._config = DatabaseConfig(
                    host=self._config.host,
                    port=self._config.port,
                    user=self._config.user,
                    password=self._config.password,
                    database=self._config.database,
                    pool_size=max(5, self._config.pool_size // 2),
                    max_overflow=5,
                    pool_timeout=10,
                    pool_recycle=300,
                    echo=self._config.echo,
                    dialect=self._config.dialect,
                    driver=self._config.driver,
                    timescale=self._config.timescale
                )

                # Reinitialize engine and session factory
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                    autoflush=False
                )

                # Verify database connection
                async with self.get_session() as session:
                    await session.execute(text("SELECT 1"))

                self._status = ServiceStatus.RUNNING
                logger.info("Emergency recovery completed successfully")

            except Exception as e:
                self._status = ServiceStatus.ERROR
                self._last_error = e
                logger.critical(f"Emergency recovery failed: {e}")
                raise ServiceError(f"Emergency recovery failed: {str(e)}")

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Enhanced critical condition handling with multiple recovery tasks"""
        try:
            severity = condition["severity"]
            error_type = condition["error_type"]

            if severity == "error" or severity == "critical":
                self._last_error = Exception(condition["message"])

            # Track error frequency for adaptive response
            await self._error_tracker.record_error(
                Exception(condition["message"]),
                error_type=error_type,
                severity=severity
            )

            if condition["type"] == DatabaseErrorType.EMERGENCY:
                await self._initiate_emergency_recovery(condition["message"])
            elif condition["type"] == DatabaseErrorType.DEADLOCK:
                await self._handle_deadlock(condition)
            elif condition["type"] == DatabaseErrorType.MAINTENANCE_REQUIRED:
                await self._handle_maintenance_required(condition)

        except Exception as e:
            self._last_error = e
            logger.error(f"Error in critical condition handler: {e}")
            if severity == "critical":
                await self._initiate_emergency_recovery(str(e))

    def get_service_status(self) -> str:
        """Get service status with recovery information"""
        current_time = TimeUtils.get_current_timestamp()

        status_lines = [
            "Database Service Status:",
            f"Status: {self._status.value}",
            f"Pool Configuration:",
            f"  Pool Size: {self._config.pool_size}",
            f"  Max Overflow: {self._config.max_overflow}",
            f"  Timeout: {self._config.pool_timeout}s",
            "Critical Errors (Last Hour):"
        ]

        error_summary = self._error_tracker.get_error_summary(window_minutes=60)
        for error_type, count in error_summary.items():
            status_lines.append(f"  {error_type}: {count}")

        if self._last_maintenance:
            time_since_maintenance = (current_time - self._last_maintenance) / (60 * 60 * 1000)
            status_lines.extend([
                "",
                "Maintenance Status:",
                f"Hours Since Last Maintenance: {time_since_maintenance:.1f}",
                f"Maintenance Due: {'Yes' if self._maintenance_due else 'No'}"
            ])

        status_lines.extend([
            "",
            self.recovery.get_status()
        ])

        return "\n".join(status_lines)