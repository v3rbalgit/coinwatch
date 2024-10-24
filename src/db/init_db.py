# src/db/init_db.py

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
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
        self.SessionLocal: Optional[scoped_session[Session]] = None  # Updated type
        self._setup_engine()
        self._health_check_interval = 300  # 5 minutes
        self._last_health_check = 0
        self.session_stats: Dict[str, Any] = {
            'active_sessions': 0,
            'total_created': 0,
            'errors': 0
        }

    def _setup_engine(self, max_retries: int = 5, retry_interval: int = 10) -> None:
        """Initialize database engine with optimized connection pooling."""
        for attempt in range(max_retries):
            try:
                self.engine = create_engine(
                    self.url,
                    pool_size=20,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=3600,
                    pool_pre_ping=True,
                )

                self._test_connection()
                session_factory = sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=self.engine
                )
                self.SessionLocal = scoped_session(session_factory)  # Assign scoped_session
                logger.info("Database engine initialized successfully")
                break

            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    raise DatabaseError(f"Failed to initialize database after {max_retries} attempts: {str(e)}")

    def _test_connection(self) -> None:
        """Test database connection."""
        if self.engine is None:
            raise DatabaseError("Database engine is not initialized")

        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                self._last_health_check = time.time()
        except Exception as e:
            raise DatabaseError(f"Database connection test failed: {str(e)}")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations."""
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
    """Initialize database tables."""
    try:
        if db_manager.engine and db_manager.SessionLocal:
            Base.metadata.create_all(bind=db_manager.engine)
            logger.info("Database tables created successfully")
        else:
            raise DatabaseError("Database engine or SessionLocal is not initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database tables: {e}")
        raise
