# src/services/database.py

from typing import Optional, AsyncGenerator, TypedDict
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy.pool import AsyncAdaptedQueuePool
from contextlib import asynccontextmanager
from sqlalchemy import text

from config import DatabaseConfig

from .base import ServiceBase
from ..core.exceptions import ServiceError
from ..utils.logger import LoggerSetup
from ..domain_types import CriticalCondition, ServiceStatus

logger = LoggerSetup.setup(__name__)

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
        self.session_factory: Optional[async_sessionmaker[AsyncSession]] = None
        self._connection_url = config.url
        self._status = ServiceStatus.STOPPED

        # Default configuration
        self._config: ConnectionConfig = {
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "pool_timeout": config.pool_timeout,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo
        }

    async def start(self) -> None:
        """Initialize database connection"""
        try:
            self._status = ServiceStatus.STARTING
            self.engine = self._create_engine()
            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Test connection
            async with self.get_session() as session:
                await session.execute(text("SELECT 1"))

            self._status = ServiceStatus.RUNNING
            logger.info("Database service started successfully")
            await super().start()

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Failed to start database service: {e}")
            raise ServiceError(f"Database initialization failed: {str(e)}")

    async def stop(self) -> None:
        """Cleanup database connections"""
        try:
            self._status = ServiceStatus.STOPPING
            if self.engine:
                await self.engine.dispose()
                self.engine = None
                self.session_factory = None

            self._status = ServiceStatus.STOPPED
            logger.info("Database service stopped")
            await super().stop()

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping database service: {e}")
            raise ServiceError(f"Database shutdown failed: {str(e)}")

    def _create_engine(self) -> AsyncEngine:
        """Create SQLAlchemy engine with optimal settings"""
        return create_async_engine(
            self._connection_url,
            poolclass=AsyncAdaptedQueuePool,
            **self._config
        )

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session with automatic cleanup"""
        if not self.session_factory:
            raise ServiceError("Database service not initialized")

        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Session error: {e}")
                raise

    async def check_health(self) -> bool:
        """Check database connection health"""
        try:
            async with self.get_session() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def handle_critical_condition(self, condition: CriticalCondition) -> None:
        """Handle critical system conditions"""
        if condition["type"] == "connection_overflow":
            # Reset connection pool
            if self.engine:
                await self.engine.dispose()
                self.engine = self._create_engine()
                logger.info("Connection pool reset due to overflow")
        elif condition["type"] == "connection_timeout":
            # Adjust pool settings
            self._config["pool_timeout"] += 10
            if self.engine:
                await self.engine.dispose()
                self.engine = self._create_engine()
                logger.info(f"Pool timeout increased to {self._config['pool_timeout']}")