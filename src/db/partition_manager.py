# src/db/partition_manager.py

import logging
from typing import List, Dict
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class PartitionBoundary:
    def __init__(self, partition_name: str, timestamp: int):
        self.partition_name = partition_name
        self.timestamp = timestamp

class PartitionManager:
    PARTITION_DEFINITIONS = [
        ('p_historical', 90),  # days
        ('p_recent', 30),      # days
        ('p_current', None)    # MAXVALUE
    ]

    def __init__(self, session: Session):
        self.session = session

    def _check_table_exists(self) -> bool:
        """Check if kline_data table exists."""
        try:
            result = self.session.execute(text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
            """))
            count = result.scalar()
            return bool(count and count > 0)
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise

    def _create_backup_table(self) -> None:
        """Create backup table and copy data."""
        try:
            logger.info("Creating backup of existing data...")
            self.session.execute(text("""
                CREATE TABLE IF NOT EXISTS kline_data_backup (
                    id BIGINT AUTO_INCREMENT,
                    symbol_id BIGINT NOT NULL,
                    start_time BIGINT NOT NULL,
                    open_price DECIMAL(50,10) NOT NULL,
                    high_price DECIMAL(50,10) NOT NULL,
                    low_price DECIMAL(50,10) NOT NULL,
                    close_price DECIMAL(50,10) NOT NULL,
                    volume DECIMAL(50,10) NOT NULL,
                    turnover DECIMAL(50,10) NOT NULL,
                    PRIMARY KEY (id),
                    INDEX idx_symbol_time (symbol_id, start_time)
                )
            """))

            self.session.execute(text("""
                INSERT INTO kline_data_backup
                SELECT * FROM kline_data
            """))

            self.session.commit()
            logger.info("Backup created successfully")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error creating backup: {e}")
            raise

    def setup_partitioning(self) -> None:
        """Set up partitioned table structure."""
        try:
            # Check if table exists and create backup
            if self._check_table_exists():
                self._create_backup_table()
                self.session.execute(text("DROP TABLE kline_data"))

            # Create new partitioned table
            logger.info("Creating partitioned table...")
            self.session.execute(text("""
                CREATE TABLE kline_data (
                    id BIGINT AUTO_INCREMENT,
                    symbol_id BIGINT NOT NULL,
                    start_time BIGINT NOT NULL,
                    open_price DECIMAL(50,10) NOT NULL,
                    high_price DECIMAL(50,10) NOT NULL,
                    low_price DECIMAL(50,10) NOT NULL,
                    close_price DECIMAL(50,10) NOT NULL,
                    volume DECIMAL(50,10) NOT NULL,
                    turnover DECIMAL(50,10) NOT NULL,
                    PRIMARY KEY (id, start_time),
                    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
                    INDEX idx_symbol_time (symbol_id, start_time)
                )
                PARTITION BY RANGE (start_time) (
                    PARTITION p_historical VALUES LESS THAN (UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 90 DAY)) * 1000),
                    PARTITION p_recent VALUES LESS THAN (UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 30 DAY)) * 1000),
                    PARTITION p_current VALUES LESS THAN MAXVALUE
                )
            """))

            # Restore data if backup exists
            if self._check_table_exists():
                logger.info("Restoring data to partitioned table...")
                self.session.execute(text("""
                    INSERT INTO kline_data
                    SELECT * FROM kline_data_backup
                """))

                logger.info("Removing backup table...")
                self.session.execute(text("DROP TABLE kline_data_backup"))

            self.session.commit()
            logger.info("Partitioning setup completed successfully")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to set up partitioning: {e}")
            raise

    def check_partition_boundaries(self) -> bool:
        """Check if partition boundaries need updating."""
        try:
            result = self.session.execute(text("""
                SELECT partition_name, partition_description
                FROM information_schema.partitions
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
            """))

            current_partitions = result.fetchall()
            # Implementation of boundary checking logic
            return False  # Placeholder
        except Exception as e:
            logger.error(f"Error checking partition boundaries: {e}")
            return False

    def get_current_partitions(self) -> Dict[str, int]:
        """Get current partition boundaries."""
        try:
            result = self.session.execute(text("""
                SELECT partition_name, partition_description
                FROM information_schema.partitions
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
                ORDER BY partition_ordinal_position
            """))

            partitions = {}
            for row in result:
                if row.partition_description != 'MAXVALUE':
                    partitions[row.partition_name] = int(row.partition_description)
                else:
                    partitions[row.partition_name] = float('inf')

            return partitions
        except Exception as e:
            logger.error(f"Error getting partition information: {e}")
            raise

    def calculate_new_boundaries(self) -> Dict[str, int]:
        """Calculate new partition boundaries based on current time."""
        current_time = int(datetime.now().timestamp() * 1000)
        boundaries = {}

        for partition_name, days in self.PARTITION_DEFINITIONS:
            if days is not None:
                boundary_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
                boundaries[partition_name] = boundary_time
            else:
                boundaries[partition_name] = float('inf')

        return boundaries

    def needs_maintenance(self) -> bool:
        """Check if partition maintenance is needed."""
        try:
            current_boundaries = self.get_current_partitions()
            new_boundaries = self.calculate_new_boundaries()

            for partition_name, new_boundary in new_boundaries.items():
                if partition_name not in current_boundaries:
                    return True
                if isinstance(new_boundary, (int, float)) and isinstance(current_boundaries[partition_name], (int, float)):
                    # Allow for small difference to prevent too frequent updates
                    if abs(new_boundary - current_boundaries[partition_name]) > (3600 * 1000):  # 1 hour difference
                        return True
            return False
        except Exception as e:
            logger.error(f"Error checking partition maintenance need: {e}")
            return False

    def reorganize_partitions(self) -> None:
        """Reorganize partitions with new boundaries."""
        try:
            new_boundaries = self.calculate_new_boundaries()

            # Create temporary partition for reorganization
            self.session.execute(text("""
                ALTER TABLE kline_data
                ADD PARTITION (PARTITION p_temp VALUES LESS THAN MAXVALUE)
            """))

            # Reorganize each partition
            for partition_name, boundary in new_boundaries.items():
                if isinstance(boundary, (int, float)) and boundary != float('inf'):
                    self.session.execute(text(f"""
                        ALTER TABLE kline_data REORGANIZE PARTITION p_temp INTO (
                            PARTITION {partition_name} VALUES LESS THAN ({boundary}),
                            PARTITION p_temp VALUES LESS THAN MAXVALUE
                        )
                    """))

            # Final reorganization for current partition
            self.session.execute(text("""
                ALTER TABLE kline_data REORGANIZE PARTITION p_temp INTO (
                    PARTITION p_current VALUES LESS THAN MAXVALUE
                )
            """))

            self.session.commit()
            logger.info("Partition reorganization completed successfully")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error reorganizing partitions: {e}")
            raise

    def perform_maintenance(self) -> None:
        """Perform partition maintenance if needed."""
        try:
            if self.needs_maintenance():
                logger.info("Starting partition maintenance...")

                # Get table size before maintenance
                size_before = self.get_table_size()

                # Perform reorganization
                self.reorganize_partitions()

                # Verify data integrity
                size_after = self.get_table_size()
                if abs(size_before - size_after) > 1000:  # Allow for small difference
                    raise ValueError(f"Data size mismatch after maintenance: before={size_before}, after={size_after}")

                logger.info("Partition maintenance completed successfully")
            else:
                logger.debug("No partition maintenance needed")

        except Exception as e:
            logger.error(f"Error during partition maintenance: {e}")
            raise

    def get_table_size(self) -> int:
        """Get total number of rows in the table."""
        try:
            result = self.session.execute(text("""
                SELECT COUNT(*) as count FROM kline_data
            """))
            count = result.scalar()
            if count is None:
                return 0
            return int(count)
        except Exception as e:
            logger.error(f"Error getting table size: {e}")
            raise

    def get_partition_stats(self) -> List[Dict[str, int]]:
        """Get statistics for each partition."""
        try:
            result = self.session.execute(text("""
                SELECT
                    partition_name,
                    COALESCE(table_rows, 0) as table_rows,
                    COALESCE(data_length, 0) as data_length,
                    COALESCE(index_length, 0) as index_length
                FROM information_schema.partitions
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
            """))

            stats = []
            for row in result:
                stats.append({
                    'partition_name': str(row.partition_name),
                    'rows': int(row.table_rows),
                    'data_size': int(row.data_length),
                    'index_size': int(row.index_length)
                })
            return stats
        except Exception as e:
            logger.error(f"Error getting partition statistics: {e}")
            raise