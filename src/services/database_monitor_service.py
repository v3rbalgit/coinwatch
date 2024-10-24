# src/services/database_monitor_service.py

from services.background_service import BackgroundService
from services.monitor_service import DatabaseMonitor
import logging
from sqlalchemy.orm import Session
import time
from db.init_db import session_scope

logger = logging.getLogger(__name__)

class DatabaseMonitorService(BackgroundService):
    def __init__(self):
        super().__init__("DatabaseMonitor")

    def run_monitoring_loop(self, check_interval: int = 300) -> None:
        """Run monitoring loop periodically."""
        logger.info(f"Starting {self.name} service")
        while not self.should_stop():
            try:
                with session_scope() as session:
                    self.collect_stats(session)
                time.sleep(check_interval)
            except Exception as e:
                logger.error(f"Error in {self.name} loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retry

    def collect_stats(self, session: Session) -> None:
        """Collect database statistics."""
        monitor = DatabaseMonitor(session)
        monitor.collect_stats()