# src/services/database.py

import asyncio
from typing import Optional, AsyncGenerator, TypedDict, Dict, TypeVar, Callable, Awaitable
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.exc import OperationalError
from contextlib import asynccontextmanager
from sqlalchemy import text, event
from enum import Enum

from ..config import DatabaseConfig
from ..services.base import ServiceBase
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup
from ..utils.domain_types import CriticalCondition, ServiceStatus
from ..utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

T = TypeVar('T')

class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

class ConnectionConfig(TypedDict):
    pool_size: int
    max_overflow: int
    pool_timeout: int
    pool_recycle: int
    echo: bool

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
        self._pool_monitor_task: Optional[asyncio.Task] = None

        self._transaction_semaphore = asyncio.BoundedSemaphore(config.pool_size)

        # PostgreSQL-specific configuration
        self._config: ConnectionConfig = {
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "pool_timeout": config.pool_timeout,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo
        }

        # Store TimescaleDB config for initialization
        self._timescale_config = config.timescale

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

    async def _monitor_pool(self) -> None:
        """
        Continuously monitor the health of the database connection pool.

        This asynchronous method runs in an infinite loop, periodically checking
        the connection pool statistics and database size. It handles critical
        conditions such as connection overflow and timeouts, and logs the current
        database size for monitoring purposes.

        The method performs the following actions:
        1. Retrieves current pool statistics.
        2. Checks for connection overflow and timeouts, handling them as critical conditions.
        3. Monitors and logs the current database size.
        4. Sleeps for 60 seconds before the next iteration.
        5. In case of any errors, logs the error and retries after 5 seconds.

        Returns:
            None

        Raises:
            No exceptions are raised; all exceptions are caught and logged.
        """
        while True:
            try:
                stats = await self._get_pool_stats()

                if stats.get('overflow', 0) > 0:
                    await self.handle_critical_condition({
                        "type": "connection_overflow",
                        "severity": "critical",
                        "message": f"Pool overflow: {stats['overflow']}",
                        "timestamp": TimeUtils.get_current_timestamp(),
                        "error_type": "ConnectionOverflow",
                        "context": {}
                    })
                if self._pool_stats['timeout_count'] > 0:
                    await self.handle_critical_condition({
                        "type": "connection_timeout",
                        "severity": "error",
                        "message": f"Pool timeout count: {self._pool_stats['timeout_count']}",
                        "timestamp": TimeUtils.get_current_timestamp(),
                        "error_type": "ConnectionTimeout",
                        "context": {}
                    })

                # Monitor database size
                async with self.transaction() as session:
                    result = await session.execute(text("""
                        SELECT pg_size_pretty(pg_database_size(current_database()));
                    """))
                    db_size = result.scalar()
                    logger.debug(f"Current database size: {db_size}")

                await asyncio.sleep(60)

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

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical conditions with PostgreSQL-specific recovery"""
        if condition["type"] == "connection_overflow":
            if self.engine:
                await self.engine.dispose()
                self._config["max_overflow"] += 5
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                    autoflush=False
                )
                logger.info(
                    f"Connection pool reset and max_overflow increased to {self._config['max_overflow']}"
                )
        elif condition["type"] == "connection_timeout":
            if self.engine:
                await self.engine.dispose()
                self._config["pool_timeout"] += 10
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                    autoflush=False
                )
                logger.info(
                    f"Pool timeout increased to {self._config['pool_timeout']}"
                )

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

    async def check_health(self) -> bool:
        """
        Comprehensive database health check including TimescaleDB features

        Checks:
        - Basic connectivity
        - TimescaleDB extension status
        - Hypertable configuration
        - Database size and connection status
        """
        try:
            async with self.transaction() as session:
                # Basic connectivity check
                await session.execute(text("SELECT 1"))

                # Check TimescaleDB extension
                result = await session.execute(text("""
                    SELECT extname, extversion
                    FROM pg_extension
                    WHERE extname = 'timescaledb';
                """))
                timescale_info = result.first()
                if not timescale_info:
                    logger.warning("TimescaleDB extension not found")
                    return False

                # Check hypertable configuration
                result = await session.execute(text("""
                    SELECT * FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'kline_data';
                """))
                if not result.first():
                    logger.warning("Kline hypertable not properly configured")
                    return False

                # Check database size and connection count
                result = await session.execute(text("""
                    SELECT
                        COALESCE(pg_size_pretty(pg_database_size(current_database())), 'Unknown') as db_size,
                        COALESCE((SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()), 0) as connection_count;
                """))
                stats = result.first()
                if stats:
                    logger.info(
                        f"Database size: {stats[0]}, "
                        f"Active connections: {stats[1]}"
                    )

                # Check chunk information
                result = await session.execute(text("""
                    SELECT
                        COALESCE(count(*), 0) as chunk_count,
                        COALESCE(pg_size_pretty(sum(total_bytes)), 'Unknown') as total_size
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'kline_data';
                """))
                chunk_info = result.first()
                if chunk_info:
                    logger.info(
                        f"TimescaleDB chunks: {chunk_info[0]}, "
                        f"Total size: {chunk_info[1]}"
                    )

                return True

        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False