# src/services/partition_maintenance_service.py

from services.background_service import BackgroundService
from db.partition_manager import PartitionManager
import logging
from sqlalchemy.orm import Session
from typing import Optional
import time
from db.init_db import session_scope  # Import session_scope

logger = logging.getLogger(__name__)

class PartitionMaintenanceService(BackgroundService):
    def __init__(self):
        super().__init__("PartitionMaintenance")
        self.check_interval = 3600  # 1 hour default

    def check_and_maintain(self, session: Session) -> None:
        """Check if maintenance is needed and perform it if necessary."""
        try:
            partition_manager = PartitionManager(session)

            # Get partition stats before maintenance
            before_stats = partition_manager.get_partition_stats()
            logger.info("Current partition statistics:")
            for stat in before_stats:
                logger.info(f"Partition {stat['partition_name']}: {stat['rows']} rows")

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

        except Exception as e:
            logger.error(f"Error in partition maintenance: {e}")
            raise

    def run_maintenance_loop(self, check_interval: Optional[int] = None) -> None:
        """
        Run maintenance loop checking partitions periodically.

        Args:
            check_interval: Optional interval in seconds between checks.
                          If None, uses the default interval (3600 seconds).
        """
        if check_interval is not None:
            self.check_interval = check_interval

        logger.info(f"Starting {self.name} service with {self.check_interval}s check interval")
        while not self.should_stop():
            try:
                with session_scope() as session:
                    self.check_and_maintain(session)
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in {self.name} loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retry
