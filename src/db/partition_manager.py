# src/db/partition_manager.py

from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.utils.db_retry import with_db_retry
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class PartitionBoundary:
    """
    Represents a database partition boundary with name and timestamp.
    Used for partition management and reorganization.
    """
    def __init__(self, partition_name: str, timestamp: int):
        self.partition_name = partition_name
        self.timestamp = timestamp

class PartitionManager:
    """
    Manages database partitioning for time-series data.

    Handles:
    - Initial partition setup
    - Partition boundary maintenance
    - Data distribution monitoring
    - Partition reorganization

    Partition structure:
    - p_historical: Data older than 90 days
    - p_recent: Data between 30-90 days
    - p_current: Recent data up to current time
    """

    PARTITION_DEFINITIONS: List[Tuple[str, int | None]] = [
        ('p_historical', 90),  # days
        ('p_recent', 30),      # days
        ('p_current', None)    # MAXVALUE
    ]

    def __init__(self, session: Session):
        """
        Initialize PartitionManager with database session.

        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
        logger.debug("PartitionManager initialized")

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if specified table exists in database.

        Args:
            table_name: Name of table to check

        Returns:
            bool: True if table exists, False otherwise

        Raises:
            Exception: If query fails
        """
        try:
            result = self.session.execute(text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = :table_name
            """), {'table_name': table_name})
            count = result.scalar()
            exists = bool(count and count > 0)
            logger.debug(f"Table '{table_name}' exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Failed to check if table '{table_name}' exists: {e}")
            raise

    def _create_backup_table(self) -> None:
        """
        Create backup table and copy data.

        Creates a temporary backup of the kline_data table to ensure
        data safety during partition reorganization.

        Raises:
            Exception: If backup creation fails
        """
        try:
            logger.info("Creating backup table for kline_data")
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

            logger.info("Copying data to backup table")
            self.session.execute(text("""
                INSERT INTO kline_data_backup
                SELECT * FROM kline_data
            """))

            self.session.commit()
            logger.info("Backup completed successfully")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create backup: {e}")
            raise

    def _calculate_partition_bounds(self) -> tuple[int, int]:
        """
        Calculate partition boundary timestamps based on current time.

        Returns:
            tuple[int, int]: Tuple of (historical_bound, recent_bound) timestamps
                            in milliseconds
        """
        now = datetime.now(timezone.utc)
        historical_bound = int((now - timedelta(days=90)).timestamp() * 1000)
        recent_bound = int((now - timedelta(days=30)).timestamp() * 1000)

        logger.debug(f"Calculated partition bounds - historical: {historical_bound}, recent: {recent_bound}")
        return historical_bound, recent_bound

    def setup_partitioning(self) -> None:
        """
        Set up partitioned table structure with data safety measures.

        Process:
        1. Backup existing data if table exists
        2. Create new partitioned table structure
        3. Restore data from backup
        4. Verify data integrity

        Raises:
            ValueError: If data verification fails
            Exception: If any step fails
        """
        try:
            historical_bound, recent_bound = self._calculate_partition_bounds()

            if self._check_table_exists('kline_data'):
                logger.info("Backing up existing kline_data table")

                # Create backup table
                self.session.execute(text("""
                    CREATE TABLE kline_data_backup LIKE kline_data
                """))

                # Copy data
                self.session.execute(text("""
                    INSERT INTO kline_data_backup
                    SELECT * FROM kline_data
                """))

                # Verify backup
                orig_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data
                """)).scalar() or 0

                backup_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data_backup
                """)).scalar() or 0

                if orig_count != backup_count:
                    err_msg = f"Backup verification failed: original {orig_count} != backup {backup_count}"
                    logger.error(err_msg)
                    raise ValueError(err_msg)

                logger.info(f"Successfully backed up {backup_count:,} records")

                logger.info("Dropping original table for reconstruction")
                self.session.execute(text("DROP TABLE kline_data"))

            # Create new partitioned table
            logger.info("Creating partitioned table structure")
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
            logger.info("Partitioned table structure created successfully")

            # Restore data if backup exists
            if self._check_table_exists('kline_data_backup'):
                logger.info("Restoring data from backup")
                self.session.execute(text("""
                    INSERT INTO kline_data
                    SELECT * FROM kline_data_backup
                """))

                # Verify restoration
                restored_count = self.session.execute(text("""
                    SELECT COUNT(*) FROM kline_data
                """)).scalar() or 0

                if restored_count != backup_count:
                    err_msg = f"Restoration verification failed: restored {restored_count:,} != backup {backup_count:,}"
                    logger.error(err_msg)
                    raise ValueError(err_msg)

                logger.info(f"Successfully restored {restored_count:,} records")

                logger.info("Cleaning up backup table")
                self.session.execute(text("DROP TABLE kline_data_backup"))

            self.session.commit()
            logger.info("Partition setup completed successfully")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Partition setup failed: {e}")
            raise

    def check_partition_boundaries(self) -> bool:
        """
        Check if partition boundaries need updating.

        Compares current boundaries with target boundaries based on current time,
        allowing for a 1-hour tolerance to prevent too frequent updates.

        Returns:
            bool: True if boundaries need updating, False otherwise
        """
        try:
            current_boundaries = self.get_current_partitions()
            if not current_boundaries:
                logger.warning("No partition boundaries found, maintenance required")
                return True

            target_boundaries = self.calculate_new_boundaries()

            current_boundaries = {
                name: ts if ts != float('inf') else None
                for name, ts in current_boundaries.items()
            }
            target_boundaries = {
                name: ts if ts != float('inf') else None
                for name, ts in target_boundaries.items()
            }

            for name in ['p_historical', 'p_recent']:
                if name not in current_boundaries:
                    logger.info(f"Missing partition: {name}")
                    return True

                current_ts = current_boundaries.get(name)
                target_ts = target_boundaries.get(name)

                # Skip if either timestamp is None (MAXVALUE)
                if current_ts is None or target_ts is None:
                    continue

                # Check if difference exceeds 1-hour tolerance
                if abs(current_ts - target_ts) > (3600 * 1000):
                    logger.info(
                        f"Partition {name} boundary needs update: "
                        f"current={datetime.fromtimestamp(current_ts/1000, tz=timezone.utc)}, "
                        f"target={datetime.fromtimestamp(target_ts/1000, tz=timezone.utc)}"
                    )
                    return True

            logger.debug("Partition boundaries are within acceptable range")
            return False

        except Exception as e:
            logger.error(f"Failed to check partition boundaries: {e}")
            return True  # Conservative approach: trigger maintenance on error

    @with_db_retry(max_attempts=3)
    def get_current_partitions(self) -> Dict[str, int]:
        """
        Get current partition boundary timestamps.

        Returns:
            Dict[str, int]: Mapping of partition names to boundary timestamps
                           (MAXVALUE represented as float('inf'))
        """
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

            logger.debug(f"Current partitions: {', '.join(partitions.keys())}")
            return partitions

        except Exception as e:
            logger.error(f"Failed to get partition information: {e}")
            raise

    def calculate_new_boundaries(self) -> Dict[str, int]:
        """
        Calculate new partition boundaries based on current time.

        Returns:
            Dict[str, int]: Mapping of partition names to target timestamps
                           (MAXVALUE represented as float('inf'))
        """
        boundaries = {}

        # Sort partitions by age (oldest first)
        partition_defs = sorted(
            self.PARTITION_DEFINITIONS,
            key=lambda x: float('inf') if x[1] is None else x[1],
            reverse=True
        )

        now = datetime.now(timezone.utc)
        for partition_name, days in partition_defs:
            if days is not None:
                boundary_time = int(
                    (now - timedelta(days=days)).timestamp() * 1000
                )
                boundaries[partition_name] = boundary_time
                logger.debug(
                    f"Calculated boundary for {partition_name}: "
                    f"{datetime.fromtimestamp(boundary_time/1000, tz=timezone.utc)}"
                )
            else:
                boundaries[partition_name] = float('inf')
                logger.debug(f"Set {partition_name} boundary to MAXVALUE")

        return boundaries

    def needs_maintenance(self) -> bool:
        """
        Check if partition maintenance is needed.

        Checks:
        1. Partition existence and structure
        2. Data distribution across partitions
        3. Partition boundary alignment

        Returns:
            bool: True if maintenance is needed, False otherwise
        """
        try:
            partition_stats = self.get_partition_stats()
            if not partition_stats:
                logger.warning("No partition statistics found")
                return True

            if len(partition_stats) != len(self.PARTITION_DEFINITIONS):
                logger.info(f"Incorrect partition count: found {len(partition_stats)}, "
                          f"expected {len(self.PARTITION_DEFINITIONS)}")
                return True

            # Check data distribution
            total_rows = sum(stat['rows'] for stat in partition_stats)
            if total_rows > 0:
                for stat in partition_stats:
                    partition_name = stat['partition_name']
                    rows = stat['rows']
                    if partition_name != 'p_current' and rows > 0:
                        percentage = (rows / total_rows) * 100
                        if percentage > 40:
                            logger.info(
                                f"Uneven data distribution: {partition_name} contains "
                                f"{percentage:.1f}% of total data ({rows:,} rows)"
                            )
                            return True

            # Check boundaries
            return self.check_partition_boundaries()

        except Exception as e:
            logger.error(f"Error checking maintenance need: {e}")
            return True  # Conservative approach: trigger maintenance on error

    def reorganize_partitions(self) -> None:
        """
        Reorganize partitions with new boundaries.

        Process:
        1. Calculate new boundary timestamps
        2. Verify current data range
        3. Build partition definitions
        4. Execute partition reorganization
        5. Validate final state

        Raises:
            Exception: If reorganization fails or validation fails
        """
        try:
            new_boundaries = self.calculate_new_boundaries()

            # Sort boundaries by timestamp (oldest first)
            sorted_boundaries = sorted(
                [(name, ts) for name, ts in new_boundaries.items()],
                key=lambda x: float('inf') if x[1] == float('inf') else x[1]
            )

            # Get and validate current data range
            current_range = self.session.execute(text("""
                SELECT
                    MIN(start_time) as min_time,
                    MAX(start_time) as max_time,
                    COUNT(*) as total_rows
                FROM kline_data
            """)).first()

            if current_range is None:
                logger.warning("Could not retrieve current data range")
                min_time = None
                max_time = None
                total_rows = 0
            else:
                min_time = current_range[0]
                max_time = current_range[1]
                total_rows = current_range[2]

            if total_rows == 0 or min_time is None or max_time is None:
                logger.info("No data in table, proceeding with basic partition setup")
            else:
                min_time_dt = datetime.fromtimestamp(min_time/1000, tz=timezone.utc)
                max_time_dt = datetime.fromtimestamp(max_time/1000, tz=timezone.utc)
                logger.info(
                    f"Current data range: {min_time_dt} to {max_time_dt} "
                    f"({total_rows:,} rows)"
                )

            # Build partition definitions
            partitions_sql = []
            for name, boundary in sorted_boundaries:
                if boundary == float('inf'):
                    partitions_sql.append(f"PARTITION {name} VALUES LESS THAN MAXVALUE")
                    logger.debug(f"Defined {name} with MAXVALUE boundary")
                else:
                    boundary_time = datetime.fromtimestamp(boundary/1000, tz=timezone.utc)
                    partitions_sql.append(f"PARTITION {name} VALUES LESS THAN ({boundary})")
                    logger.debug(f"Defined {name} with boundary {boundary_time}")

            partitions_definition = ",\n".join(partitions_sql)

            # Execute reorganization
            logger.info("Executing partition reorganization")
            self.session.execute(text(f"""
                ALTER TABLE kline_data
                PARTITION BY RANGE (start_time) (
                    {partitions_definition}
                )
            """))

            self.session.commit()

            # Validate final state
            final_stats = self.get_partition_stats()
            logger.info("Partition reorganization completed. Distribution:")
            for stat in final_stats:
                logger.info(
                    f"Partition {stat['partition_name']}: {stat['rows']:,} rows, "
                    f"Size: {stat['data_size']/1024/1024:.2f}MB, "
                    f"Index: {stat['index_size']/1024/1024:.2f}MB"
                )

        except Exception as e:
            self.session.rollback()
            logger.error(f"Partition reorganization failed: {e}")
            raise

    def perform_maintenance(self) -> None:
        """
        Perform partition maintenance if needed.

        Checks if maintenance is needed and performs reorganization
        while ensuring data integrity.
        """
        try:
            if self.needs_maintenance():
                logger.info("Starting partition maintenance")

                # Record state before maintenance
                size_before = self.get_table_size()
                logger.info(f"Current table size: {size_before:,} rows")

                # Perform reorganization
                self.reorganize_partitions()

                # Verify data integrity
                size_after = self.get_table_size()
                logger.info(f"New table size: {size_after:,} rows")

                if abs(size_before - size_after) > 1000:
                    err_msg = (
                        f"Data integrity check failed - row count mismatch: "
                        f"before={size_before:,}, after={size_after:,}"
                    )
                    logger.error(err_msg)
                    raise ValueError(err_msg)

                logger.info("Partition maintenance completed successfully")
            else:
                logger.debug("No partition maintenance needed")

        except Exception as e:
            logger.error(f"Partition maintenance failed: {e}")
            raise

    @with_db_retry(max_attempts=3)
    def get_table_size(self) -> int:
        """
        Get total number of rows in the table.

        Returns:
            int: Total row count

        Raises:
            Exception: If query fails
        """
        try:
            result = self.session.execute(text("""
                SELECT COUNT(*) as count FROM kline_data
            """))
            count = result.scalar()
            return int(count or 0)
        except Exception as e:
            logger.error(f"Failed to get table size: {e}")
            raise

    @with_db_retry(max_attempts=3)
    def get_partition_stats(self) -> List[Dict[str, Any]]:
        """
        Get detailed statistics for each partition.

        Returns:
            List[Dict[str, Any]]: List of partition statistics including
                                 row count and size information

        Raises:
            Exception: If query fails
        """
        try:
            # Query partition information with simplified query matching the working version
            result = self.session.execute(text("""
                SELECT
                    PARTITION_NAME as part_name,
                    TABLE_ROWS as row_count,
                    DATA_LENGTH as data_size,
                    INDEX_LENGTH as index_size
                FROM information_schema.partitions
                WHERE table_schema = DATABASE()
                AND table_name = 'kline_data'
                ORDER BY part_name
            """))

            stats = []
            for row in result:
                if row.part_name:  # Only include actual partitions
                    stats.append({
                        'partition_name': row.part_name,
                        'rows': int(row.row_count or 0),
                        'data_size': int(row.data_size or 0),
                        'index_size': int(row.index_size or 0)
                    })

            if not stats:
                logger.warning("No partition statistics found")
                return [{
                    'partition_name': 'p_default',
                    'rows': 0,
                    'data_size': 0,
                    'index_size': 0
                }]

            logger.info(f"Found {len(stats)} partitions:")
            for stat in stats:
                logger.info(
                    f"Partition {stat['partition_name']}: {stat['rows']:,} rows, "
                    f"Size: {stat['data_size']/1024/1024:.2f}MB, "
                    f"Index: {stat['index_size']/1024/1024:.2f}MB"
                )

            return stats

        except Exception as e:
            logger.error(f"Failed to get partition statistics: {e}")
            raise