# src/services/background_service.py

import logging
from sqlalchemy.orm import scoped_session
from db.init_db import db_manager, init_db
import threading
import atexit

logger = logging.getLogger(__name__)

class BackgroundService:
    """Base class for background services that need database access."""

    def __init__(self, name: str):
        self.name = name
        self._stop_event = threading.Event()

        # Ensure database is initialized
        if not hasattr(db_manager, 'SessionLocal') or db_manager.SessionLocal is None:
            init_db()

        if not isinstance(db_manager.SessionLocal, scoped_session):
            raise RuntimeError("Database session factory not properly initialized")

        # Register cleanup on exit
        atexit.register(self.cleanup)

    def stop(self) -> None:
        """Signal the service to stop."""
        logger.info(f"Stopping {self.name} service")
        self._stop_event.set()

    def cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        logger.info(f"Cleaning up {self.name} service")
        self.stop()

    def should_stop(self) -> bool:
        """Check if the service should stop."""
        return self._stop_event.is_set()

    def _handle_error(self, error: Exception, context: str) -> None:
        """Handle service errors with proper logging and recovery."""
        logger.error(f"Error in {self.name} service - {context}: {str(error)}")

        # Log additional context if available
        if hasattr(error, '__cause__') and error.__cause__:
            logger.error(f"Caused by: {str(error.__cause__)}")
