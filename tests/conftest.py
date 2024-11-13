# tests/conftest.py

import os
import pytest
import asyncio
from typing import AsyncGenerator
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
import logging

from src.services.database import DatabaseService
from src.utils.error import ErrorTracker

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database configuration
def get_db_config():
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5433')),
        'user': os.getenv('DB_USER', 'test_user'),
        'password': os.getenv('DB_PASSWORD', 'test_password'),
        'database': os.getenv('DB_NAME', 'coinwatch_test')
    }
    logger.debug(f"Database configuration (without password): {dict(config, password='*****')}")
    return config

db_config = get_db_config()

async def wait_for_database(max_attempts: int = 30, delay: float = 1.0) -> None:
    """Wait for database to become available"""
    attempt = 0
    last_error = None

    logger.info(f"Attempting to connect to database at {db_config['host']}:{db_config['port']}")

    while attempt < max_attempts:
        try:
            logger.debug(f"Connection attempt {attempt + 1}/{max_attempts}")
            conn = await asyncpg.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            await conn.execute('SELECT 1')
            await conn.close()
            logger.info("Successfully connected to database")
            return
        except Exception as e:
            last_error = e
            attempt += 1
            logger.warning(f"Database connection attempt {attempt} failed: {str(e)}")
            await asyncio.sleep(delay)

    raise Exception(f"Database not available after {max_attempts} attempts. Last error: {last_error}")

# Changed scope to function to match event_loop scope
@pytest.fixture
async def database_url() -> str:
    """Get database URL for testing"""
    url = f"postgresql+asyncpg://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    logger.debug(f"Database URL (without password): {url.replace(db_config['password'], '*****')}")
    return url

# Changed scope to function to match event_loop scope
@pytest.fixture
async def db_engine(database_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """Create database engine for testing"""
    logger.info("Creating database engine")
    engine = create_async_engine(
        database_url,
        echo=True,
        pool_size=5,
        max_overflow=10
    )

    try:
        await wait_for_database()

        # Create tables
        async with engine.begin() as conn:
            from src.models.base import Base
            logger.info("Creating database tables")
            await conn.run_sync(Base.metadata.create_all)

        yield engine
    except Exception as e:
        logger.error(f"Error setting up database engine: {str(e)}")
        raise
    finally:
        logger.info("Disposing database engine")
        await engine.dispose()

@pytest.fixture
async def setup_test_data(db_engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Setup test data for database tests"""
    async with db_engine.begin() as conn:
        # Create test symbol
        await conn.execute(text("""
            INSERT INTO symbols (name, exchange, first_trade_time)
            VALUES ('BTCUSDT', 'bybit', extract(epoch from now()) * 1000)
            ON CONFLICT DO NOTHING
        """))

        # Get symbol id
        result = await conn.execute(text(
            "SELECT id FROM symbols WHERE name = 'BTCUSDT'"
        ))
        symbol_id = result.scalar()

        # Insert test kline data
        if symbol_id:
            await conn.execute(text("""
                INSERT INTO kline_data (
                    symbol_id, timestamp, timeframe,
                    open_price, high_price, low_price, close_price,
                    volume, turnover
                )
                SELECT
                    :symbol_id,
                    generate_series(
                        now() - interval '1 day',
                        now(),
                        interval '5 minutes'
                    ),
                    '5',
                    100.0, 101.0, 99.0, 100.5,
                    1000.0, 100000.0
            """), {"symbol_id": symbol_id})

    yield

    # Cleanup happens in cleanup_database fixture

@pytest.fixture
def mock_time_utils(monkeypatch):
    """Mock TimeUtils for consistent timestamps in tests"""
    class MockTimeUtils:
        @staticmethod
        def get_current_timestamp():
            return 1637000000000  # Fixed timestamp for testing

        @staticmethod
        def get_current_datetime():
            from datetime import datetime, timezone
            return datetime.fromtimestamp(1637000000, tz=timezone.utc)

    monkeypatch.setattr('src.utils.time.TimeUtils', MockTimeUtils)

@pytest.fixture
async def cleanup_service_resources(db_service: DatabaseService) -> AsyncGenerator[None, None]:
    """Ensure proper cleanup of service resources after tests"""
    yield

    # Reset any semaphores
    try:
        if hasattr(db_service, '_session_semaphore'):
            # Release any waiting operations
            while True:
                try:
                    db_service._session_semaphore.release()
                except ValueError:
                    break

            # Create fresh semaphore
            db_service._session_semaphore = asyncio.BoundedSemaphore(db_service._config.pool_size)
    except Exception as e:
        logger.warning(f"Error cleaning up semaphores: {e}")

    # Cancel any monitoring tasks
    if db_service._monitor_task and not db_service._monitor_task.done():
        db_service._monitor_task.cancel()
        try:
            await db_service._monitor_task
        except asyncio.CancelledError:
            pass

    # Reset error tracker
    db_service._error_tracker = ErrorTracker()


@pytest.fixture
async def cleanup_database(db_engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Clean up database after each test"""
    logger.info("Starting database cleanup")
    yield
    async with db_engine.begin() as conn:
        # First remove the policies
        await conn.execute(text("""
            DO $$
            BEGIN
                -- Remove policies if they exist
                PERFORM remove_continuous_aggregate_policy('kline_1h');
                PERFORM remove_continuous_aggregate_policy('kline_4h');
                PERFORM remove_continuous_aggregate_policy('kline_1d');
            EXCEPTION
                WHEN undefined_object THEN
                    NULL;
            END $$;
        """))
        logger.debug("Removed continuous aggregate policies")

        # Then drop the materialized views
        await conn.execute(text("""
            DROP MATERIALIZED VIEW IF EXISTS kline_1d;
            DROP MATERIALIZED VIEW IF EXISTS kline_4h;
            DROP MATERIALIZED VIEW IF EXISTS kline_1h;
        """))
        logger.debug("Dropped materialized views")

        # Finally truncate the tables
        tables = ['symbols', 'kline_data']
        for table in tables:
            logger.debug(f"Truncating table: {table}")
            await conn.execute(text(f'TRUNCATE TABLE {table} CASCADE;'))

    logger.info("Database cleanup completed")