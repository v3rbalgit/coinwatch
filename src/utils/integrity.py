# src/utils/integrity.py

from typing import Dict, Any, cast
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import CursorResult
from utils.exceptions import DataValidationError
from utils.db_retry import with_db_retry
import logging

logger = logging.getLogger(__name__)

class DataIntegrityManager:
    """
    Manages data integrity checking and repair operations.
    Handles both partition-level and data-level integrity.
    """

    def __init__(self, session: Session):
        self.session = session

    @with_db_retry(max_attempts=3)
    def verify_partition_integrity(self) -> Dict[str, Any]:
        """
        Verify the integrity of partitioned data.

        Returns:
            Dict containing:
                - orphaned_records: Count of records with no matching symbol
                - partition_gaps: Number of gaps between partitions
                - partitions_valid: Boolean indicating overall partition validity

        Raises:
            DataValidationError: If verification fails
        """
        try:
            # Check for orphaned records
            orphaned = self.session.execute(text("""
                SELECT COUNT(*) as count
                FROM kline_data k
                LEFT JOIN symbols s ON k.symbol_id = s.id
                WHERE s.id IS NULL
            """)).scalar() or 0

            # Check for partition gaps
            partition_gaps = self.session.execute(text("""
                SELECT
                    p1.partition_name as p1_name,
                    p1.partition_description as p1_end,
                    p2.partition_name as p2_name,
                    p2.partition_description as p2_start
                FROM information_schema.partitions p1
                JOIN information_schema.partitions p2
                    ON p1.table_name = p2.table_name
                    AND p1.partition_ordinal_position + 1 = p2.partition_ordinal_position
                WHERE p1.table_schema = DATABASE()
                    AND p1.table_name = 'kline_data'
                    AND p1.partition_description != p2.partition_description
            """)).fetchall()

            return {
                'orphaned_records': orphaned,
                'partition_gaps': len(partition_gaps),
                'partitions_valid': len(partition_gaps) == 0 and orphaned == 0
            }
        except Exception as e:
            raise DataValidationError(f"Failed to verify partition integrity: {str(e)}")

    @with_db_retry(max_attempts=3)
    def validate_kline_data(self, symbol_id: int, start_time: int, end_time: int) -> Dict[str, Any]:
        """
        Validate kline data for a specific time range.

        Args:
            symbol_id: Database ID of the symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            Dict containing validation results:
                - has_gaps: Boolean indicating if time gaps exist
                - gap_count: Number of gaps found
                - anomaly_count: Number of price anomalies found
                - time_range_valid: Boolean indicating if time range is valid
                - data_valid: Boolean indicating overall data validity
                - duplicates_found: Number of duplicate timestamps found
        """
        try:
            # Check for time gaps
            gaps = self.session.execute(text("""
                WITH time_series AS (
                    SELECT
                        start_time,
                        LEAD(start_time) OVER (ORDER BY start_time) as next_start
                    FROM kline_data USE INDEX (uix_symbol_id_start_time)
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time AND :end_time
                )
                SELECT COUNT(*) as gap_count
                FROM time_series
                WHERE next_start - start_time > 300000  -- More than 5 minutes
            """), {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time
            }).scalar() or 0

            # Check for price anomalies with comprehensive validation
            anomalies = self.session.execute(text("""
                WITH price_stats AS (
                    SELECT
                        AVG(high_price - low_price) as avg_range,
                        STDDEV(high_price - low_price) as stddev_range,
                        AVG(volume) as avg_volume,
                        STDDEV(volume) as stddev_volume
                    FROM kline_data USE INDEX (uix_symbol_id_start_time)
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time AND :end_time
                )
                SELECT COUNT(*) as anomaly_count
                FROM kline_data k, price_stats s
                WHERE k.symbol_id = :symbol_id
                AND start_time BETWEEN :start_time AND :end_time
                AND (
                    (high_price - low_price) > (avg_range + 3 * stddev_range)
                    OR volume > (avg_volume + 3 * stddev_volume)
                    OR high_price < low_price
                    OR open_price < low_price
                    OR open_price > high_price
                    OR close_price < low_price
                    OR close_price > high_price
                    OR volume < 0
                    OR turnover < 0
                )
            """), {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time
            }).scalar() or 0

            # Check for duplicate timestamps
            duplicates = self.session.execute(text("""
                SELECT COUNT(*)
                FROM (
                    SELECT start_time
                    FROM kline_data
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time AND :end_time
                    GROUP BY start_time
                    HAVING COUNT(*) > 1
                ) AS dupes
            """), {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time
            }).scalar() or 0

            return {
                'has_gaps': gaps > 0,
                'gap_count': gaps,
                'anomaly_count': anomalies,
                'duplicates_found': duplicates,
                'time_range_valid': end_time > start_time,
                'data_valid': gaps == 0 and anomalies == 0 and duplicates == 0
            }
        except Exception as e:
            raise DataValidationError(f"Failed to validate kline data: {str(e)}")

    def repair_data_issues(self, issues: Dict[str, Any]) -> Dict[str, Any]:
        try:
            repairs = {
                'orphaned_removed': 0,
                'duplicates_removed': 0,
                'anomalies_removed': 0,
                'success': False
            }

            if issues.get('orphaned_records', 0) > 0:
                result = cast(CursorResult, self.session.execute(text("""
                    DELETE FROM kline_data
                    WHERE symbol_id NOT IN (SELECT id FROM symbols)
                """)))
                repairs['orphaned_removed'] = result.rowcount
                logger.info(f"Removed {repairs['orphaned_removed']} orphaned records")

            # Remove duplicate timestamps keeping the most recent insert
            if issues.get('duplicates_found', 0) > 0:
                result = cast(CursorResult, self.session.execute(text("""
                    DELETE k1 FROM kline_data k1
                    INNER JOIN kline_data k2
                    WHERE k1.symbol_id = k2.symbol_id
                    AND k1.start_time = k2.start_time
                    AND k1.id < k2.id
                """)))
                repairs['duplicates_removed'] = result.rowcount
                logger.info(f"Removed {repairs['duplicates_removed']} duplicate records")

            # Remove obvious data anomalies
            result = cast(CursorResult, self.session.execute(text("""
                DELETE FROM kline_data
                WHERE high_price < low_price
                OR open_price < low_price
                OR open_price > high_price
                OR close_price < low_price
                OR close_price > high_price
                OR volume < 0
                OR turnover < 0
            """)))
            repairs['anomalies_removed'] = result.rowcount
            logger.info(f"Removed {repairs['anomalies_removed']} anomalous records")

            self.session.commit()
            repairs['success'] = True
            return repairs

        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to repair data issues: {str(e)}")
            return {**repairs, 'success': False, 'error': str(e)}