# src/db/init_db.py

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError, DBAPIError
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import time
from typing import Generator, Optional, Dict, Any
from src.config import DATABASE_URL
from src.models.base import Base
from src.models.symbol import Symbol
from src.models.kline import Kline
from src.models.checkpoint import Checkpoint
from src.utils.exceptions import DatabaseError, SessionError
from src.utils.logger import LoggerSetup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = LoggerSetup.setup(__name__)

class SessionManager:
    """
    Manages database transaction depth and state.
    Provides nested transaction support with proper commit/rollback handling.
    """

    def __init__(self, session: Session):
        self.session = session
        self.transaction_depth = 0

    def begin_nested(self) -> None:
        """Begin a new nested transaction level."""
        self.transaction_depth += 1
        if self.transaction_depth == 1:
            self.session.begin_nested()

    def commit(self) -> None:
        """
        Commit the current transaction level.
        Raises SessionError if commit fails.
        """
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
            if self.transaction_depth == 0:
                try:
                    self.session.commit()
                except SQLAlchemyError as e:
                    self.session.rollback()
                    logger.error(f"Transaction commit failed: {e}")
                    raise SessionError(f"Failed to commit transaction: {str(e)}")

    def rollback(self) -> None:
        """Rollback the current transaction and reset depth."""
        if self.transaction_depth > 0:
            self.transaction_depth = 0
            self.session.rollback()
            logger.debug("Transaction rolled back")

class DatabaseManager:
    """
    Manages database connections, session factory, and connection pooling.
    Provides health checks and session statistics tracking.
    """

    def __init__(self, url: str):
        """
        Initialize DatabaseManager with connection URL.

        Args:
            url: Database connection URL
        """
        self.url = url
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[scoped_session[Session]] = None
        self._health_check_interval = 300  # 5 minutes
        self._last_health_check = 0
        self.session_stats: Dict[str, Any] = {
            'active_sessions': 0,
            'total_created': 0,
            'errors': 0
        }
        self._setup_engine()

    def _setup_pool_listeners(self) -> None:
        """
        Set up connection pool event listeners.
        Implements connection verification on checkout.
        """
        from sqlalchemy import event

        @event.listens_for(self.engine, 'checkout')
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            if hasattr(dbapi_connection, 'ping'):
                try:
                    dbapi_connection.ping(reconnect=True)
                except Exception:
                    logger.warning("Connection ping failed, forcing pool disposal")
                    connection_proxy._pool.dispose()
                    raise

    def _setup_engine(self, max_retries: int = 5, retry_interval: int = 10) -> None:
        """
        Initialize database engine with optimized connection pooling.

        Args:
            max_retries: Maximum number of connection attempts
            retry_interval: Seconds to wait between retries

        Raises:
            DatabaseError: If engine initialization fails after all retries
        """
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
                    connect_args={
                        'connect_timeout': 60,
                        'pool_name': 'coinwatch_pool',
                        'pool_size': 10,
                        'allow_local_infile': True,
                        'use_pure': False,  # Use C extension for better performance
                        'raise_on_warnings': True,
                        'autocommit': False,
                        'get_warnings': True,
                        'compress': True,    # Enable compression for better network performance
                        'use_unicode': True,
                        'charset': 'utf8mb4'
                    }
                )

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
                logger.warning(f"Database initialization attempt {attempt + 1}/{max_retries} failed")
                if attempt < max_retries - 1:
                    logger.debug(f"Retrying in {retry_interval} seconds")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Database initialization failed after {max_retries} attempts")
                    raise DatabaseError(f"Failed to initialize database: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((OperationalError, DBAPIError))
    )
    def _test_connection(self) -> None:
        """
        Test database connection with retry logic.
        Updates last health check timestamp on success.

        Raises:
            DatabaseError: If connection test fails
        """
        if self.engine is None:
            logger.error("Cannot test connection - engine not initialized")
            raise DatabaseError("Database engine is not initialized")

        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                self._last_health_check = time.time()
                logger.debug("Database connection test successful")
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise

    def cleanup(self) -> None:
        """
        Clean up database resources.
        Disposes connection pool and removes thread-local sessions.
        """
        if self.engine:
            if isinstance(self.engine.pool, QueuePool):
                self.engine.pool.dispose()
                logger.debug("Connection pool disposed")

            self.engine.dispose()
            logger.info("Database engine disposed")

        if self.SessionLocal:
            self.SessionLocal.remove()
            logger.info("Session factory cleaned up")

    def __del__(self):
        """Ensure cleanup on object deletion."""
        self.cleanup()

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations.

        Yields:
            Session: Database session

        Raises:
            SessionError: If session creation fails or transaction errors occur
        """
        if not self.SessionLocal:
            logger.error("Session factory not initialized")
            raise SessionError("Database session factory not initialized")

        session = None
        try:
            session = self.SessionLocal()
            self.session_stats['active_sessions'] += 1
            self.session_stats['total_created'] += 1
            logger.debug(f"Session created. Active sessions: {self.session_stats['active_sessions']}")

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
            logger.error(f"Session error: {e}")
            raise SessionError(f"Database session error: {str(e)}")
        finally:
            if session:
                session.close()
                self.session_stats['active_sessions'] -= 1
                logger.debug(f"Session closed. Active sessions: {self.session_stats['active_sessions']}")

    @contextmanager
    def get_managed_session(self) -> Generator[SessionManager, None, None]:
        """
        Provide a managed session with transaction control.

        Yields:
            SessionManager: Managed session wrapper
        """
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
    Ensures proper session cleanup and error handling.

    Yields:
        Session: Database session

    Raises:
        SessionError: If session factory is not initialized
        SQLAlchemyError: If database operations fail
    """
    if not db_manager.SessionLocal:
        logger.error("Session factory not initialized")
        raise SessionError("Database session factory not initialized")

    session = db_manager.SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Transaction rolled back: {e}")
        raise
    finally:
        session.close()
        db_manager.SessionLocal.remove()

def init_db() -> None:
    """
    Initialize database tables and ensure proper indexes exist.
    Creates required tables and indexes if they don't exist.

    Raises:
        DatabaseError: If initialization fails
    """
    try:
        if not db_manager.engine or not db_manager.SessionLocal:
            logger.error("Database manager not properly initialized")
            raise DatabaseError("Database engine or SessionLocal is not initialized")

        # Create tables
        Base.metadata.create_all(bind=db_manager.engine)
        logger.info("Database tables created")

        # Ensure indexes exist
        with db_manager.engine.connect() as connection:
            # Check and create symbol name index
            symbol_index_exists = connection.execute(text("""
                SELECT COUNT(1)
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                AND table_name = 'symbols'
                AND index_name = 'idx_symbol_name'
            """)).scalar()

            if not symbol_index_exists:
                connection.execute(text("""
                    CREATE INDEX idx_symbol_name
                    ON symbols (name)
                """))
                logger.info("Created symbol name index")

            # Check and create kline timestamp index
            kline_index_exists = connection.execute(text("""
                SELECT COUNT(1)
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
                AND index_name = 'idx_kline_time'
            """)).scalar()

            if not kline_index_exists:
                connection.execute(text("""
                    CREATE INDEX idx_kline_time
                    ON kline_data (start_time)
                """))
                logger.info("Created kline timestamp index")

            connection.commit()

        logger.info("Database initialization completed successfully")

    except Exception as e:
        logger.critical(f"Database initialization failed: {e}")
        raise DatabaseError(f"Database initialization failed: {str(e)}")