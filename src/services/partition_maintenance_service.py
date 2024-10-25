# src/services/partition_maintenance_service.py

from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Optional
import logging
import threading
import time
from src.db.init_db import session_scope
from src.services.background_service import BackgroundService
from src.db.partition_manager import PartitionManager

logger = logging.getLogger(__name__)

class PartitionMaintenanceService(BackgroundService):
    def __init__(self):
        super().__init__("PartitionMaintenance")
        self.check_interval = 3600  # 1 hour default
        self.maintenance_lock = threading.Lock()
        self._last_maintenance = 0
        self.MAINTENANCE_TIMEOUT = 7200  # 2 hours

    def check_and_maintain(self, session: Session) -> None:
        """Check if maintenance is needed and perform it if necessary."""
        try:
            partition_manager = PartitionManager(session)

            # Get partition stats before maintenance
            before_stats = partition_manager.get_partition_stats()
            logger.info("Current partition statistics:")
            for stat in before_stats:
                logger.info(f"Partition {stat['partition_name']}: {stat['rows']} rows")

            # Use advisory lock to prevent concurrent maintenance
            lock_acquired = session.execute(text("""
                SELECT GET_LOCK('partition_maintenance', 0)
            """)).scalar()

            if not lock_acquired:
                logger.warning("Another maintenance operation is in progress, skipping")
                return

            try:
                # Perform maintenance if needed
                if partition_manager.needs_maintenance():
                    logger.info("Partition maintenance required")
                    partition_manager.perform_maintenance()

                    # Log stats after maintenance
                    after_stats = partition_manager.get_partition_stats()
                    logger.info("Partition statistics after maintenance:")
                    for stat in after_stats:
                        logger.info(f"Partition {stat['partition_name']}: {stat['rows']} rows")
                else:
                    logger.debug("No partition maintenance required")

            finally:
                # Release the lock
                session.execute(text("""
                    SELECT RELEASE_LOCK('partition_maintenance')
                """))

        except Exception as e:
            logger.error(f"Error in partition maintenance: {e}")
            raise

    def run_maintenance_loop(self, check_interval: Optional[int] = None) -> None:
        """
        Run maintenance loop checking partitions periodically.
        Added timeout and lock handling for safety.
        """
        if check_interval is not None:
            self.check_interval = check_interval

        logger.info(f"Starting {self.name} service with {self.check_interval}s check interval")

        while not self.should_stop():
            try:
                current_time = time.time()

                # Check if previous maintenance is stuck
                if self.maintenance_lock.locked() and \
                   current_time - self._last_maintenance > self.MAINTENANCE_TIMEOUT:
                    logger.warning("Previous maintenance operation timed out, forcing reset")
                    self.maintenance_lock = threading.Lock()  # Create new lock

                # Try to acquire lock with timeout
                if not self.maintenance_lock.acquire(timeout=300):  # 5 minutes timeout
                    logger.warning("Could not acquire maintenance lock, skipping cycle")
                    time.sleep(300)
                    continue

                try:
                    self._last_maintenance = current_time
                    with session_scope() as session:
                        self.check_and_maintain(session)
                finally:
                    self.maintenance_lock.release()

                time.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Error in {self.name} loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retry
