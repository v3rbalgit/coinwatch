from enum import Enum
from typing import Optional, AsyncGenerator, Any
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from contextlib import asynccontextmanager

from shared.core.config import DatabaseConfig

class IsolationLevel(str, Enum):
    """Transaction isolation levels"""
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

class DatabaseConnection:
    """
    Database connection manager for microservices.
    Provides schema isolation and transaction management.
    """
    def __init__(self, config: DatabaseConfig, schema: str):
        self.url = config.url
        self.schema = schema
        self.engine: Optional[AsyncEngine] = None
        self.session_factory = None

        # Connection pool settings
        self._pool_size = config.pool_size
        self._max_overflow = config.max_overflow
        self._pool_timeout = config.pool_timeout
        self._pool_recycle = config.pool_recycle
        self._echo = config.echo

    async def initialize(self) -> None:
        """Initialize database connection with optimized settings"""
        if self.engine is None:
            self.engine = create_async_engine(
                self.url,
                pool_pre_ping=True,    # Enable connection health checks
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                pool_timeout=self._pool_timeout,
                pool_recycle=self._pool_recycle,
                echo=self._echo,
                json_serializer=None,  # Use PostgreSQL native JSON handling
                json_deserializer=None,
            )

            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False
            )

            # Verify connection and schema
            async with self.session() as session:
                await session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))

    async def close(self) -> None:
        """Close database connection and cleanup"""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.session_factory = None

    @asynccontextmanager
    async def session(
        self,
        isolation_level: Optional[IsolationLevel] = None,
        use_transaction: bool = True
    ) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a database session with schema and isolation level set.

        Args:
            isolation_level: Optional transaction isolation level
            use_transaction: Whether to wrap session in a transaction

        Raises:
            SQLAlchemyError: If database is not initialized
        """
        if not self.session_factory:
            raise SQLAlchemyError("Database not initialized")

        session = self.session_factory()
        try:
            # Set schema search path
            await session.execute(text(f"SET search_path TO {self.schema}"))

            if use_transaction:
                async with session.begin():
                    if isolation_level:
                        await session.execute(
                            text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level.value}")
                        )
                    yield session
            else:
                if isolation_level:
                    await session.execute(
                        text(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {isolation_level.value}")
                    )
                yield session
        finally:
            await session.close()

    async def check_health(self) -> dict[str, Any]:
        """
        Comprehensive health check including connection pool stats.

        Returns:
            dict: Health check results including:
                - connection_ok: bool
                - pool_size: int
                - active_connections: int
                - schema: str
        """
        try:
            async with self.session() as session:
                # Basic connection test
                await session.execute(text("SELECT 1"))

                # Get pool statistics
                pool_stats = await session.execute(text("""
                    SELECT count(*) as active_connections
                    FROM pg_stat_activity
                    WHERE application_name LIKE 'sqlalchemy%'
                    AND state = 'active';
                """))
                active = pool_stats.scalar() or 0

                return {
                    "connection_ok": True,
                    "pool_size": self._pool_size,
                    "active_connections": active,
                    "schema": self.schema
                }
        except Exception as e:
            return {
                "connection_ok": False,
                "error": str(e),
                "schema": self.schema
            }

# Usage example:
# db = DatabaseConnection(
#     url="postgresql+asyncpg://user:pass@localhost/db",
#     schema="market_data",
#     pool_size=5
# )
# await db.initialize()
#
# async with db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
#     await session.execute(text("SELECT * FROM symbols"))
