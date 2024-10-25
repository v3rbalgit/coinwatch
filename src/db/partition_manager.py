# src/db/partition_manager.py

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.utils.db_retry import with_db_retry

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

    def _check_table_exists(self, table_name: str) -> bool:
        """Check if specified table exists."""
        try:
            result = self.session.execute(text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = :table_name
            """), {'table_name': table_name})
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
                    PRIMARY KEY (id, start_time),
                    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
                    UNIQUE KEY uix_symbol_id_start_time (symbol_id, start_time)
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

    def _calculate_partition_bounds(self) -> tuple[int, int]:
        """Calculate static partition boundaries."""
        now = datetime.now(timezone.utc)

        historical_bound = int((now - timedelta(days=90)).timestamp() * 1000)
        recent_bound = int((now - timedelta(days=30)).timestamp() * 1000)

        return historical_bound, recent_bound

    def setup_partitioning(self) -> None:
        """Set up partitioned table structure with proper error handling."""
        try:
            historical_bound, recent_bound = self._calculate_partition_bounds()

            if self._check_table_exists('kline_data'):
                logger.info("Existing kline_data table found, preparing backup...")

                logger.info("Creating backup table...")
                self.session.execute(text("""
                    CREATE TABLE kline_data_backup LIKE kline_data
                """))

                logger.info("Copying data to backup...")
                self.session.execute(text("""
                    INSERT INTO kline_data_backup
                    SELECT * FROM kline_data
                """))

                orig_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data
                """)).scalar() or 0

                backup_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data_backup
                """)).scalar() or 0

                if orig_count != backup_count:
                    raise ValueError(
                        f"Backup verification failed: original {orig_count} != backup {backup_count}"
                    )

                logger.info(f"Successfully backed up {backup_count} records")

                logger.info("Dropping original table...")
                self.session.execute(text("DROP TABLE kline_data"))

            # Create new partitioned table with static bounds
            logger.info("Creating new partitioned table...")
            create_table_sql = text(f"""
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
                    UNIQUE KEY uix_symbol_id_start_time (symbol_id, start_time),
                    INDEX idx_symbol_time (symbol_id, start_time)
                ) ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_unicode_ci
                ROW_FORMAT=DYNAMIC
                PARTITION BY RANGE (start_time) (
                    PARTITION p_historical VALUES LESS THAN ({historical_bound}),
                    PARTITION p_recent VALUES LESS THAN ({recent_bound}),
                    PARTITION p_current VALUES LESS THAN MAXVALUE
                )
            """)

            self.session.execute(create_table_sql)

            if self._check_table_exists('kline_data_backup'):
                logger.info("Restoring data from backup...")
                self.session.execute(text("""
                    INSERT INTO kline_data
                    SELECT * FROM kline_data_backup
                """))

                restored_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data
                """)).scalar() or 0

                if restored_count != backup_count:
                    raise ValueError(
                        f"Restoration verification failed: restored {restored_count} != backup {backup_count}"
                    )

                logger.info(f"Successfully restored {restored_count} records")

                logger.info("Removing backup table...")
                self.session.execute(text("DROP TABLE kline_data_backup"))

            self.session.commit()
            logger.info("Partitioning setup completed successfully")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to set up partitioning: {e}")
            raise

    def check_partition_boundaries(self) -> bool:
        """
        Check if partition boundaries need updating by comparing current boundaries
        with target boundaries based on current time.

        Returns:
            bool: True if boundaries need updating, False otherwise
        """
        try:
            # Get current partition boundaries
            current_boundaries = self.get_current_partitions()
            if not current_boundaries:
                logger.warning("No partitions found")
                return True

            # Calculate target boundaries based on current time
            target_boundaries = self.calculate_new_boundaries()

            # Compare current with target boundaries
            for partition_name, target_ts in target_boundaries.items():
                if partition_name not in current_boundaries:
                    logger.info(f"Missing partition: {partition_name}")
                    return True

                current_ts = current_boundaries[partition_name]

                # Skip comparison for MAXVALUE partition
                if current_ts == float('inf') or target_ts == float('inf'):
                    continue

                # Allow 1-hour tolerance to prevent too frequent updates
                if abs(current_ts - target_ts) > (3600 * 1000):  # 1 hour in milliseconds
                    logger.info(f"Partition {partition_name} boundary needs updating: "
                            f"current={current_ts}, target={target_ts}")
                    return True

            return False

        except Exception as e:
            logger.error(f"Error checking partition boundaries: {e}")
            return True  # Conservative approach: trigger maintenance on error

    @with_db_retry(max_attempts=3)
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
                if row[1] != 'MAXVALUE':
                    partitions[row[0]] = int(row[1])
                else:
                    partitions[row[0]] = float('inf')

            return partitions
        except Exception as e:
            logger.error(f"Error getting partition information: {e}")
            raise

    def calculate_new_boundaries(self) -> Dict[str, int]:
        """Calculate new partition boundaries based on current time."""
        boundaries = {}

        # Sort partitions by age (oldest first)
        partition_defs = sorted(
            self.PARTITION_DEFINITIONS,
            key=lambda x: float('inf') if x[1] is None else x[1],
            reverse=True
        )

        for partition_name, days in partition_defs:
            if days is not None:
                boundary_time = int(
                    (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
                )
                boundaries[partition_name] = boundary_time
            else:
                boundaries[partition_name] = float('inf')

        return boundaries

    def needs_maintenance(self) -> bool:
        """
        Check if partition maintenance is needed by examining partition statistics
        and boundaries.

        Returns:
            bool: True if maintenance is needed, False otherwise
        """
        try:
            # Check partition existence and basic structure
            partition_stats = self.get_partition_stats()
            if not partition_stats:
                logger.warning("No partition statistics found")
                return True

            # Check if we have the expected number of partitions
            if len(partition_stats) != len(self.PARTITION_DEFINITIONS):
                logger.info("Unexpected number of partitions")
                return True

            # Check data distribution
            total_rows = sum(stat['rows'] for stat in partition_stats)
            if total_rows > 0:  # Only check distribution if we have data
                for stat in partition_stats:
                    partition_name = stat['partition_name']
                    rows = stat['rows']

                    # Check for uneven data distribution
                    # Alert if any partition (except current) has > 40% of total data
                    if partition_name != 'p_current' and rows > 0:
                        percentage = (rows / total_rows) * 100
                        if percentage > 40:
                            logger.info(f"Partition {partition_name} contains {percentage:.1f}% of data")
                            return True

            # Check partition boundaries
            return self.check_partition_boundaries()

        except Exception as e:
            logger.error(f"Error checking maintenance need: {e}")
            return True  # Conservative approach: trigger maintenance on error

    def reorganize_partitions(self) -> None:
        """
        Reorganize partitions with new boundaries, ensuring data integrity.
        """
        try:
            new_boundaries = self.calculate_new_boundaries()

            # Sort boundaries by timestamp (process oldest first)
            sorted_boundaries = sorted(
                [(name, ts) for name, ts in new_boundaries.items()],
                key=lambda x: float('inf') if x[1] == float('inf') else x[1]
            )

            # Get current data range for validation
            current_range = self.session.execute(text("""
                SELECT MIN(start_time) as min_time, MAX(start_time) as max_time
                FROM kline_data
            """)).first()

            min_time = current_range[0] if current_range else None
            max_time = current_range[1] if current_range else None

            if min_time is None or max_time is None:
                logger.info("No data found in table, proceeding with basic partition setup")
            else:
                logger.info(f"Current data range: {min_time} to {max_time}")

            # Build the partition definitions
            partitions_sql = []
            for name, boundary in sorted_boundaries:
                if boundary == float('inf'):
                    partitions_sql.append(f"PARTITION {name} VALUES LESS THAN MAXVALUE")
                else:
                    partitions_sql.append(f"PARTITION {name} VALUES LESS THAN ({boundary})")

            partitions_definition = ",\n".join(partitions_sql)

            # Reorganize partitions in one ALTER TABLE statement
            self.session.execute(text(f"""
                ALTER TABLE kline_data
                PARTITION BY RANGE (start_time) (
                    {partitions_definition}
                )
            """))

            self.session.commit()

            # Validate final state
            final_stats = self.get_partition_stats()
            logger.info("Partition reorganization completed. Final partition statistics:")
            for stat in final_stats:
                logger.info(f"Partition {stat['partition_name']}: {stat['rows']} rows")

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

    @with_db_retry(max_attempts=3)
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

    @with_db_retry(max_attempts=3)
    def get_partition_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for each partition."""
        try:
            result = self.session.execute(text("""
                SELECT
                    p.partition_name as partition_name,
                    COALESCE(p.table_rows, 0) as row_count,
                    COALESCE(p.data_length, 0) as data_size,
                    COALESCE(p.index_length, 0) as index_size
                FROM information_schema.partitions p
                WHERE p.table_schema = DATABASE()
                AND p.table_name = 'kline_data'
                AND p.partition_name IS NOT NULL
            """))

            stats = []
            for row in result:
                stats.append({
                    'partition_name': row.partition_name,
                    'rows': int(row.row_count),
                    'data_size': int(row.data_size),
                    'index_size': int(row.index_size)
                })

            if not stats:
                logger.warning("No partitions found for kline_data table")
                # Return default structure for new/empty table
                return [{
                    'partition_name': 'p_default',
                    'rows': 0,
                    'data_size': 0,
                    'index_size': 0
                }]

            return stats
        except Exception as e:
            logger.error(f"Error getting partition statistics: {e}")
            raise