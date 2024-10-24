# src/utils/integrity.py

from typing import Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session
from utils.exceptions import DataValidationError
import logging

logger = logging.getLogger(__name__)

class DataIntegrityManager:
    def __init__(self, session: Session):
        self.session = session

    def verify_partition_integrity(self) -> Dict[str, Any]:
        """Verify the integrity of partitioned data."""
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

    def validate_kline_data(self, symbol_id: int, start_time: int, end_time: int) -> Dict[str, Any]:
        """Validate kline data for a specific time range."""
        try:
            # Check for gaps in data
            gaps = self.session.execute(text("""
                WITH time_series AS (
                    SELECT
                        start_time,
                        LEAD(start_time) OVER (ORDER BY start_time) as next_start
                    FROM kline_data
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

            # Check for price anomalies
            anomalies = self.session.execute(text("""
                WITH stats AS (
                    SELECT
                        AVG(high_price - low_price) as avg_range,
                        STDDEV(high_price - low_price) as stddev_range
                    FROM kline_data
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time AND :end_time
                )
                SELECT COUNT(*) as anomaly_count
                FROM kline_data, stats
                WHERE symbol_id = :symbol_id
                AND start_time BETWEEN :start_time AND :end_time
                AND (high_price - low_price) > (avg_range + 3 * stddev_range)
            """), {
                'symbol_id': symbol_id,
                'start_time': start_time,
                'end_time': end_time
            }).scalar() or 0

            return {
                'has_gaps': gaps > 0,
                'gap_count': gaps,
                'anomaly_count': anomalies,
                'time_range_valid': end_time > start_time,
                'data_valid': gaps == 0 and anomalies == 0
            }
        except Exception as e:
            raise DataValidationError(f"Failed to validate kline data: {str(e)}")

    def repair_data_issues(self, issues: Dict[str, Any]) -> bool:
        """Attempt to repair detected data issues."""
        try:
            if issues.get('orphaned_records', 0) > 0:
                self.session.execute(text("""
                    DELETE FROM kline_data
                    WHERE symbol_id NOT IN (SELECT id FROM symbols)
                """))
                logger.info("Cleaned up orphaned records")

            # Add more repair operations as needed
            return True
        except Exception as e:
            logger.error(f"Failed to repair data issues: {str(e)}")
            return False