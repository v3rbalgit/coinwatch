# src/db/init_db.py
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from contextlib import contextmanager
import time
import logging
from typing import Generator, Optional
from config import DATABASE_URL
from models.base import Base

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, url: str):
        self.url = url
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker[Session]] = None
        self._setup_engine()

    def _setup_engine(self, max_retries: int = 5, retry_interval: int = 10) -> None:
        """Initialize database engine with optimized connection pooling."""
        for attempt in range(max_retries):
            try:
                self.engine = create_engine(
                    self.url,
                    pool_size=20,  # Maximum number of persistent connections
                    max_overflow=10,  # Maximum number of connections that can be created beyond pool_size
                    pool_timeout=30,  # Seconds to wait before timing out on getting a connection from the pool
                    pool_recycle=3600,  # Recycle connections after one hour
                    pool_pre_ping=True,  # Enable connection health checks
                    echo=False  # Set to True for SQL query logging
                )

                # Test the connection
                with self.engine.connect() as connection:
                    connection.execute(text("SELECT 1"))

                self.SessionLocal = sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=self.engine
                )
                logger.info("Database engine initialized successfully")
                break

            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logger.error("Failed to connect to database after maximum retries")
                    raise

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations."""
        if not self.SessionLocal:
            raise RuntimeError("Database session factory not initialized")

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()

    def check_connection(self) -> bool:
        """Verify database connection is healthy."""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False

    def init_db(self) -> None:
        """Initialize database schema."""
        from models.base import Base
        if not self.engine:
            raise RuntimeError("Database engine not initialized")

        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise

# Create global database manager instance
db_manager = DatabaseManager(DATABASE_URL)

# Convenience function to get database session
def get_db() -> Generator[Session, None, None]:
    with db_manager.get_session() as session:
        yield session

# Initialize database tables
def init_db() -> None:
    db_manager.init_db()