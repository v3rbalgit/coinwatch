# tests/services/test_database_service.py

import pytest
import asyncio
import logging
from typing import Any, AsyncGenerator, List, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker
)

from src.services.database.service import DatabaseService, DatabaseErrorType, IsolationLevel
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
async def test_timescaledb_setup(db_service: DatabaseService):
    """Test TimescaleDB features setup"""
    async with db_service.get_session() as session:
        # Verify hypertable creation
        result = await session.execute(text("""
            SELECT hypertable_schema, hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'kline_data';
        """))
        hypertable = result.fetchone()
        assert hypertable is not None, "Hypertable was not created"

        # Verify compression settings
        result = await session.execute(text("""
            SELECT compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'kline_data';
        """))
        compression = result.scalar()
        assert compression is True, "Compression not enabled"

@pytest.mark.asyncio
async def test_maintenance_operations(db_service: DatabaseService):
    """Test database maintenance operations"""
    condition: CriticalCondition = {
        "type": DatabaseErrorType.MAINTENANCE_REQUIRED,
        "severity": "warning",
        "message": "Maintenance required",
        "timestamp": TimeUtils.get_current_timestamp(),
        "error_type": DatabaseErrorType.MAINTENANCE_REQUIRED,
        "context": {
            "last_maintenance": None
        }
    }

    await db_service.handle_critical_condition(condition)
    assert db_service._last_maintenance is not None

    # Verify maintenance was performed by checking system catalogs
    async with db_service.get_session() as session:
        result = await session.execute(text("""
            SELECT last_vacuum FROM pg_stat_user_tables
            WHERE relname = 'kline_data'
        """))
        last_vacuum = result.scalar()
        assert last_vacuum is not None

@pytest.mark.asyncio
async def test_deadlock_handling(db_service: DatabaseService):
    """Test deadlock detection and resolution"""
    condition: CriticalCondition = {
        "type": DatabaseErrorType.DEADLOCK,
        "severity": "critical",
        "message": "Deadlock detected",
        "timestamp": TimeUtils.get_current_timestamp(),
        "error_type": DatabaseErrorType.DEADLOCK,
        "context": {
            "deadlock_count": 3
        }
    }

    await db_service.handle_critical_condition(condition)
    # Service should remain operational
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar() == 1

@pytest.mark.asyncio
async def test_emergency_recovery(db_service: DatabaseService):
    """Test emergency recovery procedures"""
    # Store original configuration values
    original_pool_size = db_service._config.pool_size
    original_max_overflow = db_service._config.max_overflow

    try:
        # Force a critical condition
        await db_service._initiate_emergency_recovery("Test emergency recovery")

        # Verify service recovered with conservative settings
        assert db_service._status == ServiceStatus.RUNNING
        assert db_service.engine is not None
        assert db_service._config.pool_size == max(5, original_pool_size // 2)
        assert db_service._config.max_overflow == 5
        assert db_service._config.pool_timeout == 10
        assert db_service._config.pool_recycle == 300

        # Verify service is operational
        async with db_service.get_session() as session:
            result = await session.execute(text("SELECT 1"))
            assert result.scalar() == 1

        # Test concurrent connections with proper cleanup
        async def test_connection():
            async with db_service.get_session() as session:
                result = await session.execute(text("SELECT 1"))
                return result.scalar()

        # Use asyncio.gather with proper timeout
        results = await asyncio.wait_for(
            asyncio.gather(
                *(test_connection() for _ in range(3)),
                return_exceptions=True
            ),
            timeout=5.0  # Add reasonable timeout
        )
        assert all(r == 1 for r in results), "Some connections failed after recovery"

        # Allow connections to clean up
        await asyncio.sleep(0.1)

    except Exception as e:
        pytest.fail(f"Emergency recovery test failed: {str(e)}")

@pytest.mark.asyncio
async def test_pool_reconfiguration(db_service: DatabaseService):
    """Test dynamic pool reconfiguration"""
    # Test increasing pool size - pass as dictionary
    updates = {
        'pool_size': 10,
        'max_overflow': 15
    }

    await db_service._reconfigure_pool(**updates)

    # Verify new configuration
    assert db_service._config.pool_size == 10
    assert db_service._config.max_overflow == 15

    # Verify pool is operational
    async with db_service.get_session() as session:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar() == 1

@pytest.mark.asyncio
async def test_continuous_aggregate_management(db_service: DatabaseService):
    """Test continuous aggregate management"""
    if not db_service.engine:
        pytest.fail("Database engine not initialized")

    try:
        async with db_service.get_session() as session:
            # Create test data
            await session.execute(text("""
                INSERT INTO symbols (name, exchange)
                VALUES ('BTCUSDT', 'bybit')
                ON CONFLICT DO NOTHING;
            """))

            result = await session.execute(text(
                "SELECT id FROM symbols WHERE name = 'BTCUSDT'"
            ))
            symbol_id = result.scalar()
            if not symbol_id:
                pytest.fail("Failed to create test symbol")

            # Insert some test kline data
            await session.execute(text("""
                INSERT INTO kline_data (
                    symbol_id, timestamp, timeframe,
                    open_price, high_price, low_price, close_price,
                    volume, turnover
                ) VALUES (
                    :symbol_id, NOW(), '5',
                    100.0, 101.0, 99.0, 100.5,
                    1000.0, 100000.0
                )
            """), {"symbol_id": symbol_id})

            # Set up continuous aggregates
            await db_service._setup_continuous_aggregate_policies(db_service.engine)

            # Verify views were created
            result = await session.execute(text("""
                SELECT COUNT(*)
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name IN ('kline_1h', 'kline_4h', 'kline_1d')
            """))
            view_count = result.scalar()
            assert view_count is not None and view_count > 0, "No continuous aggregates were created"

    except Exception as e:
        pytest.fail(f"Test failed with error: {str(e)}")

@pytest.mark.asyncio
async def test_session_without_transaction(db_service: DatabaseService):
    """Test session management without transaction wrapping"""
    async with db_service.get_session(use_transaction=False) as session:
        # Execute some DDL that requires no transaction
        await session.execute(text("COMMIT"))  # Ensure no active transaction
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS test_no_transaction (
                id SERIAL PRIMARY KEY
            )
        """))

        # Cleanup
        await session.execute(text("DROP TABLE IF EXISTS test_no_transaction"))

@pytest.mark.asyncio
async def test_database_monitoring(db_service: DatabaseService, monkeypatch):
    """Test database monitoring functionality"""
    # Mock sleep to speed up test
    async def mock_sleep(_):
        pass
    monkeypatch.setattr(asyncio, 'sleep', mock_sleep)

    # Start monitoring
    monitor_task = asyncio.create_task(db_service._monitor_database())

    # Wait a bit for monitoring to run
    await asyncio.sleep(0)

    # Cancel monitoring
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

@pytest.mark.asyncio
async def test_error_tracking(db_service: DatabaseService):
    """Test error tracking and frequency calculation"""
    error1 = Exception("Test error 1")
    error2 = Exception("Test error 2")

    # Record some errors
    await db_service._error_tracker.record_error(error1)
    await db_service._error_tracker.record_error(error2)

    # Get error frequency
    frequency = await db_service._error_tracker.get_error_frequency(
        "Exception",
        window_minutes=60
    )
    assert frequency > 0

@pytest.mark.asyncio
async def test_service_status_reporting(db_service: DatabaseService):
    """Test service status reporting"""
    status_report = db_service.get_service_status()

    assert "Database Service Status:" in status_report
    assert f"Status: {db_service._status.value}" in status_report
    assert "Pool Configuration:" in status_report
    assert "Critical Errors (Last Hour):" in status_report

@pytest.mark.asyncio
async def test_non_transactional_maintenance(db_service: DatabaseService):
    """Test maintenance operations without transaction wrapping"""
    condition: CriticalCondition = {
        "type": DatabaseErrorType.MAINTENANCE_REQUIRED,
        "severity": "warning",
        "message": "Maintenance required",
        "timestamp": TimeUtils.get_current_timestamp(),
        "error_type": DatabaseErrorType.MAINTENANCE_REQUIRED,
        "context": {
            "last_maintenance": None
        }
    }

    # Run maintenance with non-transactional session
    await db_service._handle_maintenance_required(condition)

    assert db_service._last_maintenance is not None
    assert not db_service._maintenance_due

@pytest.mark.asyncio
async def test_session_cleanup_on_error(db_service: DatabaseService):
    """Test session cleanup when errors occur"""
    # Instead of checking _active_sessions, we'll verify through pool metrics
    async with db_service.get_session() as session:
        # Get initial metrics
        initial_metrics = await db_service._collect_metrics()
        initial_connections = initial_metrics.active_connections

        try:
            await session.execute(text("SELECT pg_sleep(1)"))  # Long query
            raise Exception("Test error")
        except Exception:
            pass

    # Allow time for cleanup
    await asyncio.sleep(0.1)

    # Verify connections were cleaned up
    final_metrics = await db_service._collect_metrics()
    assert final_metrics.active_connections <= initial_connections, \
        "Connections were not properly cleaned up"

@pytest.mark.asyncio
async def test_retry_strategy_configuration(db_service: DatabaseService):
    """Test retry strategy configuration and behavior"""
    # Test retryable error
    retryable_error = ConnectionError("Test connection error")
    should_retry, reason = db_service._retry_strategy.should_retry(0, retryable_error)
    assert should_retry is True
    assert reason == "retryable_error"

    # Test non-retryable error
    non_retryable_error = ValueError("Test value error")
    should_retry, reason = db_service._retry_strategy.should_retry(0, non_retryable_error)
    assert should_retry is False
    assert reason == "unknown_error"

    # Test max retries exceeded
    should_retry, reason = db_service._retry_strategy.should_retry(5, retryable_error)
    assert should_retry is False
    assert reason == "max_retries_exceeded"
