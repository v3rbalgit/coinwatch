# src/db/init_db.py

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError, DBAPIError
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import time
import logging
from typing import Generator, Optional, Dict, Any
from config import DATABASE_URL
from models.base import Base
from models.symbol import Symbol
from models.kline import Kline
from models.checkpoint import Checkpoint
from utils.exceptions import DatabaseError, SessionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class SessionManager:
    def __init__(self, session: Session):
        self.session = session
        self.transaction_depth = 0

    def begin_nested(self) -> None:
        """Begin a nested transaction."""
        self.transaction_depth += 1
        if self.transaction_depth == 1:
            self.session.begin_nested()

    def commit(self) -> None:
        """Commit the current transaction."""
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
            if self.transaction_depth == 0:
                try:
                    self.session.commit()
                except SQLAlchemyError as e:
                    self.session.rollback()
                    raise SessionError(f"Failed to commit transaction: {str(e)}")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self.transaction_depth > 0:
            self.transaction_depth = 0
            self.session.rollback()

class DatabaseManager:
    def __init__(self, url: str):
        self.url = url
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[scoped_session[Session]] = None
        self._setup_engine()
        self._health_check_interval = 300  # 5 minutes
        self._last_health_check = 0
        self.session_stats: Dict[str, Any] = {
            'active_sessions': 0,
            'total_created': 0,
            'errors': 0
        }

    def _setup_pool_listeners(self) -> None:
        """Set up connection pool event listeners."""
        from sqlalchemy import event

        @event.listens_for(self.engine, 'checkout')
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Listener for connection checkout events."""
            if hasattr(dbapi_connection, 'ping'):
                try:
                    dbapi_connection.ping(reconnect=True)
                except Exception:
                    # Force disconnect handling on next use
                    connection_proxy._pool.dispose()
                    raise

    def _setup_engine(self, max_retries: int = 5, retry_interval: int = 10) -> None:
        """Initialize database engine with optimized connection pooling."""
        for attempt in range(max_retries):
            try:
                self.engine = create_engine(
                    self.url,
                    poolclass=QueuePool,
                    pool_size=10,           # Matches our thread count
                    max_overflow=5,         # Limited overflow
                    pool_timeout=30,        # Connection timeout
                    pool_recycle=1800,      # 30 minutes
                    pool_pre_ping=True,     # Verify connections
                    pool_use_lifo=True,     # Better connection reuse
                    # Add these MySQL-specific arguments
                    connect_args={
                        'connect_timeout': 10,
                        'read_timeout': 30,
                        'write_timeout': 30,
                        'keepalive': True,
                        'keepalive_interval': 180,  # 3 minutes
                    }
                )

                # Test connection and set up event listeners
                self._setup_pool_listeners()
                self._test_connection()

                session_factory = sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=self.engine,
                    expire_on_commit=False
                )
                self.SessionLocal = scoped_session(session_factory)
                logger.info("Database engine initialized successfully")
                break

            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    raise DatabaseError(f"Failed to initialize database after {max_retries} attempts: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((OperationalError, DBAPIError))
    )
    def _test_connection(self) -> None:
        """Test database connection with retry logic."""
        if self.engine is None:
            raise DatabaseError("Database engine is not initialized")

        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                self._last_health_check = time.time()
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise

    def cleanup(self) -> None:
        """Ensure proper cleanup of database resources."""
        if self.engine:
            # Close all checked-out connections
            if isinstance(self.engine.pool, QueuePool):
                self.engine.pool.dispose()

            # Dispose of the engine itself
            self.engine.dispose()
            logger.info("Database engine disposed")

        if self.SessionLocal:
            # Remove any thread-local sessions
            self.SessionLocal.remove()
            logger.info("Session factory cleaned up")

    def __del__(self):
        """Ensure cleanup is called on deletion."""
        self.cleanup()

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations with retry logic."""
        if not self.SessionLocal:
            raise SessionError("Database session factory not initialized")

        session = None
        try:
            session = self.SessionLocal()
            self.session_stats['active_sessions'] += 1
            self.session_stats['total_created'] += 1

            # Perform health check if needed
            current_time = time.time()
            if current_time - self._last_health_check > self._health_check_interval:
                self._test_connection()

            yield session
            session.commit()
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            self.session_stats['errors'] += 1
            raise SessionError(f"Database session error: {str(e)}")
        finally:
            if session:
                session.close()
                self.session_stats['active_sessions'] -= 1

    @contextmanager
    def get_managed_session(self) -> Generator[SessionManager, None, None]:
        """Provide a managed session with transaction control."""
        with self.get_session() as session:
            manager = SessionManager(session)
            try:
                yield manager
            except Exception:
                manager.rollback()
                raise

    def get_session_stats(self) -> Dict[str, Any]:
        """Get current session statistics."""
        return self.session_stats.copy()

# Create global database manager instance
db_manager = DatabaseManager(DATABASE_URL)

@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Provide a transactional scope around a series of operations.
    """
    if not db_manager.SessionLocal:
        raise SessionError("Database session factory not initialized")

    session = db_manager.SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Session rollback due to error: {e}")
        raise
    finally:
        session.close()
        db_manager.SessionLocal.remove()

def init_db() -> None:
    """Initialize database tables with proper indexes."""
    try:
        if not db_manager.engine or not db_manager.SessionLocal:
            raise DatabaseError("Database engine or SessionLocal is not initialized")

        # Create tables
        Base.metadata.create_all(bind=db_manager.engine)

        # Ensure indexes exist
        with db_manager.engine.connect() as connection:
            # Check for symbol name index
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_symbol_name
                ON symbols (name)
            """))

            # Check for kline timestamp index
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_kline_time
                ON kline_data (start_time)
            """))

            # Commit the changes
            connection.commit()

        logger.info("Database tables and indexes created successfully")

    except Exception as e:
        logger.critical(f"Failed to initialize database tables: {e}")
        raise DatabaseError(f"Database initialization failed: {str(e)}")