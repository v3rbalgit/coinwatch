# src/services/database.py

import asyncio
from typing import Optional, AsyncGenerator, Set, Dict, TypeVar, Callable, Awaitable, Union
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.exc import OperationalError, DBAPIError
from contextlib import asynccontextmanager
from sqlalchemy import text, event
from enum import Enum


from ..config import DatabaseConfig
from ..services.base import ServiceBase
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup
from ..utils.domain_types import CriticalCondition, ServiceStatus
from ..utils.error import ErrorTracker
from ..utils.retry import RetryConfig, RetryStrategy
from ..utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

T = TypeVar('T')

class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

class DatabaseErrorType(str, Enum):
    """Specific database error categories"""
    CONNECTION_OVERFLOW = "connection_overflow"
    CONNECTION_TIMEOUT = "connection_timeout"
    DEADLOCK = "deadlock"
    QUERY_TIMEOUT = "query_timeout"
    DISK_FULL = "disk_full"
    REPLICATION_LAG = "replication_lag"
    LOCK_TIMEOUT = "lock_timeout"
    MAINTENANCE_REQUIRED = "maintenance_required"

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

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.engine: Optional[AsyncEngine] = None
        self._connection_url = config.url
        self._status = ServiceStatus.STOPPED
        self._pool_stats = {
            'checkout_count': 0,
            'checkin_count': 0,
            'overflow_count': 0,
            'timeout_count': 0
        }
        # Enhanced error tracking and recovery
        self._pool_monitor_task: Optional[asyncio.Task] = None
        self._error_tracker = ErrorTracker()
        self._recovery_lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self._maintenance_lock = asyncio.Lock()
        self._active_sessions: Set[AsyncSession] = set()
        self._session_lock = asyncio.Lock()

        # Configure retry strategy
        retry_config = RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            max_retries=3,
            jitter_factor=0.1
        )
        self._retry_strategy = RetryStrategy(retry_config)
        self._configure_retry_strategy()

        self._transaction_semaphore = asyncio.BoundedSemaphore(config.pool_size)

        self._timescale_config = config.timescale

        # Track maintenance windows
        self._last_maintenance: Optional[int] = None
        self._maintenance_due: bool = False

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
        engine = create_async_engine(
            self._connection_url,
            poolclass=AsyncAdaptedQueuePool,
            json_serializer=None,  # Use PostgreSQL native JSON handling
            json_deserializer=None,
            pool_pre_ping=True,    # Enable connection health checks
            **self._config
        )

        # Set PostgreSQL-specific session configuration
        @event.listens_for(engine.sync_engine, "connect")
        def set_pg_session_config(dbapi_connection, connection_record):
            # Enable parallel query execution
            dbapi_connection.set_session(
                enable_parallel_query=True,
                statement_timeout=30000  # 30 seconds timeout
            )

        return engine

    async def start(self) -> None:
        """
        Start the database service with TimescaleDB initialization.

        This asynchronous method initializes the database engine, creates necessary tables,
        sets up TimescaleDB extensions, configures the session factory, and starts the
        connection pool monitoring.

        The method performs the following steps:
        1. Creates the database engine
        2. Creates tables if they don't exist
        3. Initializes TimescaleDB extensions
        4. Sets chunk interval for TimescaleDB if specified
        5. Configures the session factory
        6. Starts the pool monitoring task
        7. Calls the parent class's start method

        Raises:
            ServiceError: If there's an error during the database initialization process.

        Returns:
            None
        """
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting database service")

            self.engine = self._create_engine()

            # Create tables if they don't exist
            async with self.engine.begin() as conn:
                from ..models.base import Base
                await conn.run_sync(Base.metadata.create_all)

                # Initialize TimescaleDB extensions
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))

                # Set chunk interval if specified
                if self._timescale_config.chunk_interval:
                    await conn.execute(text(
                        "SELECT set_chunk_time_interval('kline_data', interval :interval);"
                    ), {"interval": self._timescale_config.chunk_interval})

            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False  # Optimize for bulk operations
            )

            # Start pool monitoring
            self._pool_monitor_task = asyncio.create_task(
                self._monitor_pool()
            )

            await super().start()
            self._status = ServiceStatus.RUNNING
            logger.info("Database service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Failed to start database service: {e}")
            raise ServiceError(f"Database initialization failed: {str(e)}")

    async def stop(self) -> None:
        """Cleanup database connections"""
        try:
            self._status = ServiceStatus.STOPPING

            if self._pool_monitor_task:
                self._pool_monitor_task.cancel()
                try:
                    await self._pool_monitor_task
                except asyncio.CancelledError:
                    pass

            if self.engine:
                await self.engine.dispose()
                self.engine = None

            self._status = ServiceStatus.STOPPED
            logger.info("Database service stopped")
            await super().stop()

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping database service: {e}")
            raise ServiceError(f"Database shutdown failed: {str(e)}")

    async def _monitor_pool(self) -> None:
        """
        Essential pool monitoring focused on critical health metrics.
        Detailed monitoring will be handled by the monitoring service.
        """
        while True:
            try:
                if not self.engine:
                    await asyncio.sleep(1)
                    continue

                async with self.transaction() as session:
                    # Check only critical metrics
                    result = await session.execute(text("""
                        SELECT count(*) as active_connections
                        FROM pg_stat_activity
                        WHERE application_name LIKE 'coinwatch%'
                        AND state = 'active';
                    """))
                    active_connections = result.scalar() or 0

                    # Only react to critical conditions
                    if active_connections >= self._config["pool_size"]:
                        await self.handle_critical_condition({
                            "type": DatabaseErrorType.CONNECTION_OVERFLOW,
                            "severity": "warning",
                            "message": f"High connection usage: {active_connections}/{self._config['pool_size']}",
                            "timestamp": TimeUtils.get_current_timestamp(),
                            "error_type": DatabaseErrorType.CONNECTION_OVERFLOW,
                            "context": {
                                "active_connections": active_connections,
                                "pool_size": self._config["pool_size"]
                            }
                        })

                    # Check for maintenance needs (once per day)
                    current_time = TimeUtils.get_current_timestamp()
                    if (not self._last_maintenance or
                        current_time - self._last_maintenance > 24 * 60 * 60 * 1000):  # 24 hours in ms
                        self._maintenance_due = True

                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                logger.error(f"Pool monitoring error: {e}")
                await asyncio.sleep(5)

    async def _get_pool_stats(self) -> Dict[str, int]:
        """
        Retrieve current database connection pool statistics using PostgreSQL system views.

        This asynchronous method queries the PostgreSQL system views to gather information
        about the current state of the connection pool. It provides details on the pool size,
        number of checked-out connections, and any overflow connections.

        Returns:
            Dict[str, int]: A dictionary containing the following keys:
                - 'size': The configured size of the connection pool.
                - 'checked_out': The number of currently active connections.
                - 'overflow': The number of connections exceeding the pool size.

        Note:
            If the database engine is not initialized or an error occurs during the query,
            the method returns default values (0) for all statistics.
        """
        if not self.engine:
            return {'size': 0, 'checked_out': 0, 'overflow': 0}

        try:
            async with self.transaction() as session:
                result = await session.execute(text("""
                    SELECT count(*) as connections
                    FROM pg_stat_activity
                    WHERE application_name LIKE 'coinwatch%';
                """))
                active_connections = result.scalar() or 0

                return {
                    'size': self._config['pool_size'],
                    'checked_out': active_connections,
                    'overflow': max(0, active_connections - self._config['pool_size'])
                }
        except Exception as e:
            logger.error(f"Error getting pool stats: {e}")
            return {'size': 0, 'checked_out': 0, 'overflow': 0}

    @asynccontextmanager
    async def transaction(self,
                         isolation_level: Optional[IsolationLevel] = None,
                         retry_count: int = 3,
                         retry_delay: float = 1.0) -> AsyncGenerator[AsyncSession, None]:
        """
        Transaction management with PostgreSQL-specific optimizations

        Args:
            isolation_level: Transaction isolation level
            retry_count: Number of retries for deadlocks
            retry_delay: Delay between retries
        """
        if not self.session_factory:
            raise ServiceError("Database service not initialized")

        async with self._transaction_semaphore:
            attempt = 0
            while attempt <= retry_count:
                async with self.session_factory() as session:
                    try:
                        if isolation_level:
                            await session.execute(
                                text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level.value}")
                            )

                        # Set statement timeout for this transaction
                        await session.execute(text("SET statement_timeout = '30s';"))

                        async with session.begin():
                            yield session
                        return

                    except OperationalError as e:
                        if "deadlock detected" in str(e).lower():
                            if attempt < retry_count:
                                logger.warning(
                                    f"Deadlock detected, retrying in {retry_delay}s... "
                                    f"({retry_count - attempt} attempts left)"
                                )
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                attempt += 1
                                continue
                            else:
                                logger.error("Max retries exceeded")
                                raise
                        else:
                            logger.error(f"Operational error: {e}")
                            raise
                    except Exception as e:
                        logger.error(f"Transaction error: {e}")
                        raise
                attempt += 1

    @asynccontextmanager
    async def savepoint(self, session: AsyncSession):
        """Nested transaction using PostgreSQL savepoint"""
        async with session.begin_nested():
            yield

    async def execute_in_transaction(self,
                                   func: Callable[[AsyncSession], Awaitable[T]],
                                   isolation_level: Optional[IsolationLevel] = None) -> T:
        """Execute an async function within a transaction"""
        async with self.transaction(isolation_level) as session:
            return await func(session)

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a session for backward compatibility.
        Wraps the transaction context manager for existing code.
        """
        async with self.transaction() as session:
            yield session

    def _configure_retry_strategy(self) -> None:
        """Configure retry behavior for database operations"""
        self._retry_strategy.add_retryable_error(
            OperationalError,
            DBAPIError,
            ConnectionError,
            TimeoutError
        )
        self._retry_strategy.add_non_retryable_error(
            ServiceError
        )

    async def _handle_connection_overflow(self, condition: CriticalCondition) -> None:
        """Progressive connection overflow handling with backoff"""
        async with self._recovery_lock:
            current_overflow = self._config["max_overflow"]
            error_frequency = await self._error_tracker.get_error_frequency(
                DatabaseErrorType.CONNECTION_OVERFLOW,
                window_minutes=60
            )

            if error_frequency > 10:
                if current_overflow >= 100:
                    await self._initiate_emergency_recovery("Connection overflow limit reached")
                    return

                new_overflow = min(current_overflow * 2, 100)
                logger.warning(f"High connection overflow frequency, increasing to {new_overflow}")
                await self._reconfigure_pool(max_overflow=new_overflow)
            else:
                new_overflow = min(current_overflow + 5, 50)
                logger.info(f"Moderate connection overflow, adjusting to {new_overflow}")
                await self._reconfigure_pool(max_overflow=new_overflow)

    async def _handle_connection_timeout(self, condition: CriticalCondition) -> None:
        """Handle connection timeout with adaptive timeout adjustment"""
        async with self._recovery_lock:
            current_timeout = self._config["pool_timeout"]
            error_frequency = await self._error_tracker.get_error_frequency(
                DatabaseErrorType.CONNECTION_TIMEOUT,
                window_minutes=60
            )

            if error_frequency > 10:
                if current_timeout >= 60:  # Max 1 minute timeout
                    await self._initiate_emergency_recovery("Connection timeout limit reached")
                    return

                new_timeout = min(current_timeout * 2, 60)
                logger.warning(f"High timeout frequency, increasing to {new_timeout}s")
            else:
                new_timeout = min(current_timeout + 5, 30)
                logger.info(f"Moderate timeout frequency, adjusting to {new_timeout}s")

            await self._reconfigure_pool(pool_timeout=new_timeout)

    async def _handle_deadlock(self, condition: CriticalCondition) -> None:
        """Handle deadlock conditions with analysis"""
        async with self._recovery_lock:
            error_frequency = await self._error_tracker.get_error_frequency(
                DatabaseErrorType.DEADLOCK,
                window_minutes=60
            )

            if error_frequency > 5:
                # Analyze deadlocks
                async with self.transaction() as session:
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
                        logger.error(f"Found {len(deadlocks)} deadlock situations")
                        # Terminate long-running queries if necessary
                        for deadlock in deadlocks:
                            if deadlock.blocking_pid:
                                await session.execute(text(
                                    f"SELECT pg_terminate_backend({deadlock.blocking_pid})"
                                ))

    async def _reconfigure_pool(self, **config_updates: Union[int, bool]) -> None:
        """
        Safely reconfigure connection pool with type-safe updates.

        Args:
            **config_updates: Updates to pool configuration (pool_size, max_overflow, etc.)
        """
        async with self._pool_lock:
            old_engine = self.engine
            try:
                # Type-safe config update
                validated_updates = {
                    k: v for k, v in config_updates.items()
                    if k in self._config
                }
                self._config.update(validated_updates)  # type: ignore

                # Create new engine with updated config
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                    autoflush=False
                )

                if old_engine:
                    await old_engine.dispose()

                logger.info(f"Pool reconfigured with {validated_updates}")

            except Exception as e:
                self.engine = old_engine
                logger.error(f"Pool reconfiguration failed: {e}")
                raise ServiceError(f"Pool reconfiguration failed: {str(e)}")

    async def _initiate_emergency_recovery(self, reason: str) -> None:
        """Emergency recovery procedure"""
        logger.critical(f"Initiating emergency recovery: {reason}")

        async with self._recovery_lock:
            try:
                # Stop accepting new connections
                self._status = ServiceStatus.STOPPING

                # Close all active sessions
                async with self._session_lock:
                    for session in self._active_sessions:
                        await session.close()
                    self._active_sessions.clear()

                # Dispose current engine
                if self.engine:
                    await self.engine.dispose()

                # Reset configuration to conservative values
                self._config.update({
                    "pool_size": max(5, self._config["pool_size"] // 2),
                    "max_overflow": 5,
                    "pool_timeout": 10,
                    "pool_recycle": 300
                })

                # Reinitialize engine and session factory
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                    autoflush=False
                )

                # Verify database connection
                async with self.transaction() as session:
                    await session.execute(text("SELECT 1"))

                self._status = ServiceStatus.RUNNING
                logger.info("Emergency recovery completed successfully")

            except Exception as e:
                self._status = ServiceStatus.ERROR
                logger.critical(f"Emergency recovery failed: {e}")
                raise ServiceError(f"Emergency recovery failed: {str(e)}")

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Enhanced critical condition handling with progressive recovery"""
        try:
            severity = condition["severity"]
            error_type = condition["error_type"]

            # Track error frequency for adaptive response
            await self._error_tracker.record_error(
                Exception(condition["message"]),
                error_type=error_type,
                severity=severity
            )

            # Handle specific error types
            if condition["type"] == DatabaseErrorType.CONNECTION_OVERFLOW:
                await self._handle_connection_overflow(condition)
            elif condition["type"] == DatabaseErrorType.CONNECTION_TIMEOUT:
                await self._handle_connection_timeout(condition)
            elif condition["type"] == DatabaseErrorType.DEADLOCK:
                await self._handle_deadlock(condition)
            elif condition["type"] == DatabaseErrorType.MAINTENANCE_REQUIRED:
                await self._handle_maintenance_required(condition)

        except Exception as e:
            logger.error(f"Error in critical condition handler: {e}")
            if severity == "critical":
                await self._initiate_emergency_recovery(str(e))

    async def _handle_maintenance_required(self, condition: CriticalCondition) -> None:
        """Handle maintenance requirements"""
        async with self._maintenance_lock:
            current_time = TimeUtils.get_current_timestamp()
            if (not self._last_maintenance or
                current_time - self._last_maintenance > 24 * 60 * 60 * 1000):  # 24 hours in ms

                try:
                    async with self.transaction() as session:
                        # Run VACUUM ANALYZE on critical tables
                        await session.execute(text("""
                            VACUUM ANALYZE kline_data;
                            VACUUM ANALYZE symbols;
                        """))

                        # Update table statistics
                        await session.execute(text("ANALYZE;"))

                        self._last_maintenance = current_time
                        self._maintenance_due = False

                        logger.info("Database maintenance completed successfully")

                except Exception as e:
                    logger.error(f"Maintenance failed: {e}")
                    self._maintenance_due = True
                    raise ServiceError(f"Database maintenance failed: {str(e)}")

    def get_service_status(self) -> str:
        """Get service status focused on essential metrics"""
        current_time = TimeUtils.get_current_timestamp()

        status_lines = [
            "Database Service Status:",
            f"Status: {self._status.value}",
            f"Pool Configuration:",
            f"  Pool Size: {self._config['pool_size']}",
            f"  Max Overflow: {self._config['max_overflow']}",
            f"  Timeout: {self._config['pool_timeout']}s",
            f"Active Sessions: {len(self._active_sessions)}",
            "",
            "Critical Errors (Last Hour):"
        ]

        # Add critical error statistics
        error_summary = self._error_tracker.get_error_summary(window_minutes=60)
        for error_type, count in error_summary.items():
            status_lines.append(f"  {error_type}: {count}")

        # Add maintenance status
        if self._last_maintenance:
            time_since_maintenance = (current_time - self._last_maintenance) / (60 * 60 * 1000)  # hours
            status_lines.extend([
                "",
                "Maintenance Status:",
                f"Hours Since Last Maintenance: {time_since_maintenance:.1f}",
                f"Maintenance Due: {'Yes' if self._maintenance_due else 'No'}"
            ])

        return "\n".join(status_lines)