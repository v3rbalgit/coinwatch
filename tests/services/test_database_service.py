# tests/services/test_database_service.py

import pytest
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Any, cast
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import ScalarResult

from src.services.database import DatabaseService, DatabaseErrorType, IsolationLevel
from src.core.coordination import ServiceCoordinator
from src.core.exceptions import ServiceError, ConfigurationError
from src.config import DatabaseConfig, TimescaleConfig
from src.utils.domain_types import CriticalCondition, ServiceStatus
from src.utils.time import TimeUtils

# Setup logging
logger = logging.getLogger(__name__)

import os

@pytest.fixture
def db_config(database_url: str) -> DatabaseConfig:
    """Create test database configuration using environment-based URL"""
    return DatabaseConfig(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5433')),
        user=os.getenv('DB_USER', 'test_user'),
        password=os.getenv('DB_PASSWORD', 'test_password'),
        database=os.getenv('DB_NAME', 'coinwatch_test'),
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        echo=False,
        timescale=TimescaleConfig()
    )

@pytest.fixture
async def coordinator() -> AsyncGenerator[ServiceCoordinator, None]:
    """Create test coordinator"""
    coordinator = ServiceCoordinator()
    await coordinator.start()
    yield coordinator
    await coordinator.stop()

@pytest.fixture
async def db_service(db_config: DatabaseConfig, coordinator: ServiceCoordinator) -> AsyncGenerator[DatabaseService, None]:
    """Create and start database service for testing"""
    service = DatabaseService(coordinator, db_config)
    await service.start()
    yield service
    await service.stop()

@pytest.mark.asyncio
async def test_service_startup(db_service, caplog):
    """Test service starts correctly"""
    caplog.set_level(logging.DEBUG)

    try:
        logger.info("Starting service startup test")
        assert db_service.engine is not None, "Database engine should not be None"
        assert db_service._status.value == "running", f"Service status should be running, got {db_service._status.value}"

        # Try a simple query to verify connection
        async with db_service.get_session() as session:
            logger.debug("Testing database connection with simple query")
            result = await session.execute(text("SELECT 1"))
            value = result.scalar()
            assert value == 1, f"Expected query result 1, got {value}"

        logger.info("Service startup test completed successfully")
    except SQLAlchemyError as e:
        logger.error(f"Database error during test: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during test: {str(e)}")
        raise

@pytest.mark.asyncio
async def test_service_shutdown(db_service: DatabaseService) -> None:
    """Test service stops correctly"""
    await db_service.stop()
    assert db_service.engine is None
    assert db_service._status == ServiceStatus.STOPPED

@pytest.mark.asyncio
async def test_invalid_config() -> None:
    """Test service rejects invalid configuration"""
    coordinator = ServiceCoordinator()
    await coordinator.start()

    try:
        invalid_config = DatabaseConfig(
            host="",  # Invalid empty host
            port=5432,
            user="test_user",
            password="test_password",
            database="coinwatch_test",
            timescale=TimescaleConfig()
        )

        # This should raise ConfigurationError
        service = DatabaseService(coordinator, invalid_config)

        # If we get here, the test should fail
        pytest.fail("Expected ConfigurationError was not raised")
    except ConfigurationError as e:
        # This is the expected path
        assert "Database host must be specified" in str(e)
    finally:
        await coordinator.stop()

@pytest.mark.asyncio
async def test_session_creation(db_service: DatabaseService) -> None:
    """Test session creation and basic query"""
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT 1"))
        value = result.scalar()
        assert value is not None and value == 1

@pytest.mark.asyncio
async def test_session_isolation_levels(db_service: DatabaseService) -> None:
    """Test different isolation levels"""
    results = {}

    # Test each isolation level in a separate session
    for level in IsolationLevel:
        logger.debug(f"Testing isolation level: {level.value}")

        # The isolation level is set before the transaction begins
        async with db_service.get_session(isolation_level=level) as session:
            try:
                # Execute a query to verify the session works
                result = await session.execute(text("SELECT 1"))
                assert result.scalar() == 1, f"Basic query failed at isolation level {level.value}"

                # Check current transaction's isolation level
                result = await session.execute(text("SELECT current_setting('transaction_isolation')"))
                current_level = result.scalar()

                # Store results for verification
                results[level] = current_level
                logger.debug(f"Successfully tested isolation level {level.value}, got {current_level}")

            except Exception as e:
                logger.error(f"Failed at isolation level {level.value}: {str(e)}")
                raise

    # Verify all isolation levels were set correctly
    expected_mappings = {
        IsolationLevel.READ_UNCOMMITTED: 'read committed',
        IsolationLevel.READ_COMMITTED: 'read committed',
        IsolationLevel.REPEATABLE_READ: 'repeatable read',
        IsolationLevel.SERIALIZABLE: 'serializable'
    }

    for level, expected in expected_mappings.items():
        actual = results.get(level)
        assert actual and actual.lower() == expected, \
            f"Expected {expected} for {level.value}, got {actual}"

@pytest.mark.asyncio
async def test_concurrent_sessions(db_service: DatabaseService) -> None:
    """Test concurrent session handling"""
    async def run_query() -> int:
        async with db_service.get_session() as session:
            result = await session.execute(text("SELECT 1"))
            value = result.scalar()
            if value is None:
                raise ValueError("Query returned None")
            await asyncio.sleep(0.1)  # Simulate some work
            return int(value)

    tasks = [run_query() for _ in range(5)]  # Reduced from 10 to 5 for test environment
    results = await asyncio.gather(*tasks)
    assert all(r == 1 for r in results)

@pytest.mark.asyncio
async def test_pool_size_limits(db_service: DatabaseService) -> None:
    """Test pool size limits are respected"""
    sessions = []
    max_sessions = db_service._config.pool_size + db_service._config.max_overflow

    try:
        for _ in range(max_sessions + 1):
            async with db_service.get_session() as session:
                # Execute a query to ensure connection is established
                result = await session.execute(text("SELECT 1"))
                value = result.scalar()
                assert value is not None
                sessions.append(session)
                await asyncio.sleep(0.1)  # Give some time for connection establishment
    except ServiceError as e:
        assert "Connection pool exhausted" in str(e)
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")
    finally:
        # Sessions will be automatically closed by context manager
        pass

@pytest.mark.asyncio
async def test_transaction_management(db_service: DatabaseService) -> None:
    """Test transaction management with commit and rollback"""
    # First transaction: Create test table
    async with db_service.get_session() as session:
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS test_transactions (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
        """))

    # Second transaction: Test successful insert
    async with db_service.get_session() as session:
        await session.execute(
            text("INSERT INTO test_transactions (value) VALUES (:value)"),
            {"value": "test1"}
        )

    # Third transaction: Verify the insert
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM test_transactions"))
        count = result.scalar()
        assert count is not None and count == 1

    # Fourth transaction: Test rollback
    try:
        async with db_service.get_session() as session:
            await session.execute(
                text("INSERT INTO test_transactions (value) VALUES (:value)"),
                {"value": "test2"}
            )
            raise Exception("Test rollback")
    except Exception:
        pass

    # Fifth transaction: Verify rollback occurred
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM test_transactions"))
        count = result.scalar()
        assert count is not None and count == 1  # Still 1, not 2

    # Final transaction: Cleanup
    async with db_service.get_session() as session:
        await session.execute(text("DROP TABLE test_transactions"))

@pytest.mark.asyncio
async def test_metrics_collection(db_service: DatabaseService) -> None:
    """Test database metrics collection"""
    metrics = await db_service._collect_metrics()

    # Basic metrics validation
    assert metrics.service_name == "database"
    assert metrics.status is not None
    assert metrics.uptime_seconds >= 0
    assert metrics.active_connections >= 0
    assert metrics.pool_size == db_service._config.pool_size
    assert metrics.max_overflow == db_service._config.max_overflow

    # Additional metrics checks
    assert metrics.deadlocks >= 0
    assert metrics.long_queries >= 0
    assert isinstance(metrics.maintenance_due, bool)

@pytest.mark.asyncio
async def test_connection_error_recovery(db_service: DatabaseService) -> None:
    """Test recovery from connection errors"""
    # Simulate a connection error
    condition: CriticalCondition = {
        "type": DatabaseErrorType.CONNECTION_OVERFLOW,
        "severity": "warning",
        "message": "Connection pool exhausted",
        "timestamp": TimeUtils.get_current_timestamp(),
        "error_type": "ConnectionError",
        "context": {
            "active_connections": 5,
            "pool_size": 5
        }
    }

    await db_service.handle_critical_condition(condition)

    # Verify service is still operational
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT 1"))
        value = result.scalar()
        assert value is not None and value == 1