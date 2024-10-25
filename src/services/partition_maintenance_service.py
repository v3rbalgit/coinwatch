# src/services/partition_maintenance_service.py

import threading
import time
from typing import Optional, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from src.db.init_db import session_scope
from src.services.background_service import BackgroundService
from src.db.partition_manager import PartitionManager
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class PartitionMaintenanceService(BackgroundService):
    """
    Service for managing database partition maintenance.

    Features:
    - Periodic partition checks
    - Automatic maintenance when needed
    - Lock-based concurrency control
    - Timeout handling
    - Deadlock recovery
    """

    def __init__(self):
        """Initialize partition maintenance service with default settings."""
        super().__init__("PartitionMaintenance")
        self.check_interval = 3600  # 1 hour default
        self.maintenance_lock = threading.Lock()
        self._last_maintenance = 0
        self.MAINTENANCE_TIMEOUT = 7200  # 2 hours
        logger.debug("Partition maintenance service initialized")

    def _format_partition_stats(self, stats: list) -> str:
        """
        Format partition statistics for logging.

        Args:
            stats: List of partition statistics

        Returns:
            str: Formatted statistics string
        """
        return "\n".join(
            f"Partition {stat['partition_name']}: {stat['rows']:,} rows, "
            f"Size: {stat['data_size']/1024/1024:.2f}MB"
            for stat in stats
        )

    def check_and_maintain(self, session: Session) -> None:
        """
        Check partition status and perform maintenance if needed.

        Uses MySQL advisory locks to prevent concurrent maintenance operations.

        Args:
            session: Database session for maintenance operations

        Raises:
            Exception: If maintenance operations fail
        """
        try:
            partition_manager = PartitionManager(session)

            # Log current partition state
            try:
                before_stats = partition_manager.get_partition_stats()
                logger.info("Current partition state:\n%s", self._format_partition_stats(before_stats))
            except Exception as e:
                logger.error(f"Failed to get partition stats: {e}")
                return

            # Try to acquire advisory lock
            lock_acquired = session.execute(text("""
                SELECT GET_LOCK('partition_maintenance', 0)
            """)).scalar()

            if not lock_acquired:
                logger.warning("Maintenance lock held by another process, skipping cycle")
                return

            try:
                if partition_manager.needs_maintenance():
                    logger.info("Starting partition maintenance")
                    partition_manager.perform_maintenance()

                    # Verify maintenance results
                    after_stats = partition_manager.get_partition_stats()
                    logger.info("Maintenance completed. New state:\n%s",
                              self._format_partition_stats(after_stats))
                else:
                    logger.debug("No maintenance required")

            finally:
                # Always release the lock
                session.execute(text("""
                    SELECT RELEASE_LOCK('partition_maintenance')
                """))
                logger.debug("Maintenance lock released")

        except Exception as e:
            logger.error("Partition maintenance failed", exc_info=e)
            raise

    def _check_stuck_maintenance(self, current_time: float) -> bool:
        """
        Check if previous maintenance operation is stuck.

        Args:
            current_time: Current timestamp

        Returns:
            bool: True if maintenance is stuck, False otherwise
        """
        if (self.maintenance_lock.locked() and
            current_time - self._last_maintenance > self.MAINTENANCE_TIMEOUT):
            logger.warning(
                f"Maintenance operation timed out after "
                f"{timedelta(seconds=self.MAINTENANCE_TIMEOUT)}. Forcing reset."
            )
            self.maintenance_lock = threading.Lock()
            return True
        return False

    def run_maintenance_loop(self, check_interval: Optional[int] = None) -> None:
        """
        Run continuous maintenance loop with safety checks.

        Args:
            check_interval: Optional override for check interval (seconds)
        """
        if check_interval is not None:
            self.check_interval = check_interval

        logger.info(
            f"Starting maintenance service (interval: {timedelta(seconds=self.check_interval)})"
        )

        while not self.should_stop():
            try:
                current_time = time.time()

                # Handle stuck maintenance
                self._check_stuck_maintenance(current_time)

                # Try to acquire lock with timeout
                if not self.maintenance_lock.acquire(timeout=300):
                    logger.warning("Failed to acquire maintenance lock after 5 minutes")
                    time.sleep(300)
                    continue

                try:
                    self._last_maintenance = current_time
                    with session_scope() as session:
                        self.check_and_maintain(session)
                finally:
                    self.maintenance_lock.release()
                    logger.debug("Maintenance cycle completed")

                time.sleep(self.check_interval)

            except Exception as e:
                logger.error("Error in maintenance loop", exc_info=e)
                time.sleep(300)  # 5 minute retry delay

    def get_status(self) -> Dict:
        """
        Get detailed service status.

        Returns:
            Dict containing:
                - Base service status
                - Last maintenance time
                - Lock status
                - Next scheduled check
        """
        base_status = super().get_status()
        current_time = time.time()

        return {
            **base_status,
            'last_maintenance': datetime.fromtimestamp(self._last_maintenance).isoformat()
                              if self._last_maintenance > 0 else None,
            'lock_held': self.maintenance_lock.locked(),
            'maintenance_stuck': self._check_stuck_maintenance(current_time),
            'next_check': datetime.fromtimestamp(
                self._last_maintenance + self.check_interval
            ).isoformat() if self._last_maintenance > 0 else None
        }