# src/services/database_monitor_service.py

import logging
from sqlalchemy.orm import Session
import time
from src.services.background_service import BackgroundService
from src.services.monitor_service import DatabaseMonitor
from src.db.init_db import session_scope

logger = logging.getLogger(__name__)

class DatabaseMonitorService(BackgroundService):
    def __init__(self):
        super().__init__("DatabaseMonitor")
        self._consecutive_failures = 0
        self.MAX_CONSECUTIVE_FAILURES = 5
        self.BACKOFF_MULTIPLIER = 2

    def run_monitoring_loop(self, check_interval: int = 300) -> None:
        """Run monitoring loop periodically."""
        logger.info(f"Starting {self.name} service")

        while not self.should_stop():
            try:
                with session_scope() as session:
                    self.collect_stats(session)
                    self._consecutive_failures = 0  # Reset on success
                time.sleep(check_interval)

            except Exception as e:
                self._consecutive_failures += 1
                wait_time = min(300 * self.BACKOFF_MULTIPLIER ** (self._consecutive_failures - 1), 3600)

                logger.error(f"Error in {self.name} loop: {e}")
                logger.warning(f"Consecutive failures: {self._consecutive_failures}, "
                             f"waiting {wait_time} seconds before retry")

                if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    logger.critical(f"{self.name} service exceeded maximum consecutive failures. "
                                  "Manual intervention may be required.")

                time.sleep(wait_time)

    def collect_stats(self, session: Session) -> None:
        """Collect database statistics with proper error handling."""
        try:
            monitor = DatabaseMonitor(session)
            monitor.collect_stats()
        except Exception as e:
            logger.error(f"Error collecting database stats: {e}")
            raise  # Let the main loop handle the retry logic