# src/services/database.py

import time
import asyncio
import re
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
from sqlalchemy import text
from enum import Enum

from src.config import DatabaseConfig

from src.services.base import ServiceBase
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup
from ..utils.domain_types import CriticalCondition, ServiceStatus

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
    """Database service with connection pooling and session management"""

    def __init__(self, config: DatabaseConfig):
        super().__init__()
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

        # Default configuration
        self._config: ConnectionConfig = {
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "pool_timeout": config.pool_timeout,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo
        }

    def _create_engine(self) -> AsyncEngine:
        """Create SQLAlchemy engine with optimal settings"""
        return create_async_engine(
            self._connection_url,
            poolclass=AsyncAdaptedQueuePool,
            **self._config
        )

    async def start(self) -> None:
        """Start the database service with pool monitoring"""
        try:
            self._status = ServiceStatus.STARTING
            self.engine = self._create_engine()

            # Create tables if they don't exist
            async with self.engine.begin() as conn:
                from ..models.base import Base
                await conn.run_sync(Base.metadata.create_all)

            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
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
            raise ServiceError(f"Database initialization failed: {str(e)}")

    async def _monitor_pool(self) -> None:
        """Monitor connection pool health"""
        while True:
            try:
                stats = await self._get_pool_stats()
                if stats.get('overflow', 0) > 0:
                    await self.handle_critical_condition(
                        CriticalCondition(
                            type="connection_overflow",
                            message=f"Pool overflow: {stats['overflow']}",
                            severity="warning",
                            timestamp=time.time()
                        )
                    )
                if self._pool_stats['timeout_count'] > 0:
                    await self.handle_critical_condition(
                        CriticalCondition(
                            type="connection_timeout",
                            message=f"Pool timeout count: {self._pool_stats['timeout_count']}",
                            severity="error",
                            timestamp=time.time()
                        )
                    )
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Pool monitoring error: {e}")

    # TODO: consider monitoring of the checked-in parameter
    async def _get_pool_stats(self) -> Dict[str, int]:
        """Get current pool statistics"""
        if not self.engine or not hasattr(self.engine, 'sync_engine'):
            return {'size': 0, 'checked_out': 0, 'overflow': 0}

        pool = self.engine.sync_engine.pool
        status_str = pool.status()
        stats = {
            'size': 0,
            'checked_out': 0,
            'overflow': 0
        }

        try:
            # Adjust the regular expression based on the actual format
            pattern = (
                r'Pool size: (?P<size>\d+)\s+'
                r'Connections in pool: \d+\s+'
                r'Current Overflow: (?P<overflow>-?\d+)\s+'
                r'Current Checked out connections: (?P<checked_out>\d+)'
            )
            match = re.search(pattern, status_str)
            if match:
                stats['size'] = int(match.group('size'))
                stats['checked_out'] = int(match.group('checked_out'))
                stats['overflow'] = int(match.group('overflow'))
        except Exception as e:
            logger.error(f"Error parsing pool status: {e}")

        return stats

    @asynccontextmanager
    async def transaction(self,
                          isolation_level: Optional[IsolationLevel] = None,
                          retry_count: int = 3,
                          retry_delay: float = 1.0) -> AsyncGenerator[AsyncSession, None]:
        """Transaction management with isolation levels and retry logic"""
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
                        async with session.begin():
                            yield session
                        return  # Exit if successful
                    except OperationalError as e:
                        if "deadlock" in str(e).lower():
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
        """Nested transaction using savepoint"""
        async with session.begin_nested():
            yield

    async def execute_in_transaction(self,
                                     func: Callable[[AsyncSession], Awaitable[T]],
                                     isolation_level: Optional[IsolationLevel] = None) -> T:
        """Execute an async function within a transaction"""
        async with self.transaction(isolation_level) as session:
            return await func(session)

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a session for backward compatibility"""
        async with self.transaction() as session:
            yield session

    async def check_health(self) -> bool:
        """Check database connection health"""
        try:
            async with self.transaction() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical conditions like pool overflow or timeout"""
        if condition["type"] == "connection_overflow":
            if self.engine:
                await self.engine.dispose()
                self._config["max_overflow"] += 5
                self.engine = self._create_engine()
                self.session_factory = async_sessionmaker(
                    bind=self.engine,
                    class_=AsyncSession,
                    expire_on_commit=False
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
                    expire_on_commit=False
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
                # self.session_factory = None

            self._status = ServiceStatus.STOPPED
            logger.info("Database service stopped")
            await super().stop()

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping database service: {e}")
            raise ServiceError(f"Database shutdown failed: {str(e)}")
