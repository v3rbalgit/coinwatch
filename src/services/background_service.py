# src/services/background_service.py

import threading
import atexit
from sqlalchemy.orm import scoped_session
from src.db.init_db import db_manager, init_db
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class BackgroundService:
    """
    Base class for background services that need database access.

    Provides common functionality for:
    - Service lifecycle management
    - Database session verification
    - Graceful shutdown handling
    - Error recovery

    Attributes:
        name: Service identifier
        _stop_event: Threading event for stop signal
    """

    def __init__(self, name: str):
        """
        Initialize background service.

        Args:
            name: Service identifier for logging and management

        Raises:
            RuntimeError: If database session factory is not properly initialized
        """
        self.name = name
        self._stop_event = threading.Event()
        self._initialize_database()

        # Register cleanup on exit
        atexit.register(self.cleanup)
        logger.debug(f"Background service '{self.name}' initialized")

    def _initialize_database(self) -> None:
        """
        Ensure database and session factory are properly initialized.

        Raises:
            RuntimeError: If session factory initialization fails
        """
        try:
            if not hasattr(db_manager, 'SessionLocal') or db_manager.SessionLocal is None:
                logger.info(f"Initializing database for service '{self.name}'")
                init_db()

            if not isinstance(db_manager.SessionLocal, scoped_session):
                err_msg = f"Database session factory not properly initialized for service '{self.name}'"
                logger.error(err_msg)
                raise RuntimeError(err_msg)

        except Exception as e:
            logger.error(f"Failed to initialize database for service '{self.name}': {e}")
            raise

    def stop(self) -> None:
        """
        Signal the service to stop.
        Sets the stop event to trigger graceful shutdown.
        """
        logger.info(f"Stopping service '{self.name}'")
        self._stop_event.set()

    def cleanup(self) -> None:
        """
        Cleanup resources on shutdown.
        Called automatically on program exit.
        """
        try:
            logger.info(f"Cleaning up service '{self.name}'")
            self.stop()
        except Exception as e:
            logger.error(f"Error during cleanup of service '{self.name}': {e}")

    def should_stop(self) -> bool:
        """
        Check if the service should stop.

        Returns:
            bool: True if stop signal has been received
        """
        return self._stop_event.is_set()

    def _handle_error(self, error: Exception, context: str) -> None:
        """
        Handle service errors with proper logging and recovery.

        Args:
            error: The exception that occurred
            context: Description of what was happening when the error occurred

        Note:
            Logs both the error and its cause (if available) for better debugging
        """
        logger.error(
            f"Error in service '{self.name}' - {context}: {str(error)}",
            exc_info=True
        )

        cause = getattr(error, '__cause__', None)
        if cause:
            logger.error(f"Caused by: {str(cause)}", exc_info=cause)

    def get_status(self) -> dict:
        """
        Get current service status.

        Returns:
            dict: Service status information including:
                  - name: Service name
                  - running: Whether service is running
                  - database_initialized: Database initialization status
        """
        return {
            'name': self.name,
            'running': not self._stop_event.is_set(),
            'database_initialized': (
                hasattr(db_manager, 'SessionLocal') and
                isinstance(db_manager.SessionLocal, scoped_session)
            )
        }