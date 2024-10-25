# src/services/database_monitor_service.py

import time
from sqlalchemy.orm import Session
from src.services.background_service import BackgroundService
from src.services.monitor_service import DatabaseMonitor
from src.db.init_db import session_scope
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class DatabaseMonitorService(BackgroundService):
    """
    Service for monitoring database health and collecting statistics.

    Features:
    - Periodic database statistics collection
    - Exponential backoff on failures
    - Automatic recovery attempts
    - Critical failure detection
    """

    def __init__(self):
        """Initialize database monitoring service."""
        super().__init__("DatabaseMonitor")
        self._consecutive_failures = 0
        self.MAX_CONSECUTIVE_FAILURES = 5
        self.BACKOFF_MULTIPLIER = 2
        self._last_success = None
        logger.debug("Database monitor service initialized")

    def calculate_wait_time(self) -> int:
        """
        Calculate wait time using exponential backoff.

        Returns:
            int: Wait time in seconds, capped at 1 hour
        """
        base_wait = 300  # 5 minutes
        wait_time = min(
            base_wait * self.BACKOFF_MULTIPLIER ** (self._consecutive_failures - 1),
            3600  # Cap at 1 hour
        )
        return wait_time

    def handle_failure(self, error: Exception) -> int:
        """
        Handle monitoring failure with exponential backoff.

        Args:
            error: Exception that caused the failure
        """
        self._consecutive_failures += 1
        wait_time = self.calculate_wait_time()

        logger.error(
            f"Monitoring cycle failed - Attempt {self._consecutive_failures}: {str(error)}",
            exc_info=True
        )

        if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
            logger.critical(
                f"Service exceeded maximum failures ({self.MAX_CONSECUTIVE_FAILURES}). "
                "Manual intervention required."
            )
        else:
            logger.warning(
                f"Backing off for {wait_time} seconds before retry "
                f"(Failure {self._consecutive_failures}/{self.MAX_CONSECUTIVE_FAILURES})"
            )

        return wait_time

    def run_monitoring_loop(self, check_interval: int = 300) -> None:
        """
        Run continuous monitoring loop with error handling.

        Args:
            check_interval: Time between checks in seconds (default: 300)
        """
        logger.info(f"Starting monitoring service with {check_interval}s interval")

        while not self.should_stop():
            try:
                with session_scope() as session:
                    self.collect_stats(session)

                    # Update success metrics
                    self._consecutive_failures = 0
                    self._last_success = time.time()
                    logger.debug("Monitoring cycle completed successfully")

                time.sleep(check_interval)

            except Exception as e:
                wait_time = self.handle_failure(e)
                time.sleep(wait_time)

    def collect_stats(self, session: Session) -> None:
        """
        Collect and store database statistics.

        Args:
            session: Database session for collecting statistics

        Raises:
            Exception: If statistics collection fails
        """
        try:
            logger.debug("Beginning statistics collection")
            monitor = DatabaseMonitor(session)
            monitor.collect_stats()
            logger.debug("Statistics collection completed")

        except Exception as e:
            logger.error(f"Failed to collect database statistics: {e}")
            raise

    def get_status(self) -> dict:
        """
        Get detailed service status.

        Returns:
            dict: Status information including:
                - Base service status
                - Consecutive failures
                - Last successful run
                - Current backoff time
        """
        base_status = super().get_status()
        return {
            **base_status,
            'consecutive_failures': self._consecutive_failures,
            'last_success': self._last_success,
            'current_backoff': self.calculate_wait_time() if self._consecutive_failures > 0 else 0
        }