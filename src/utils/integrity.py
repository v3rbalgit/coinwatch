# src/utils/integrity.py

from typing import Dict, Any, cast, Tuple
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from sqlalchemy.engine import CursorResult
from src.utils.exceptions import DataValidationError
from src.utils.db_retry import with_db_retry
from src.models.symbol import Symbol
from src.models.kline import Kline
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)

class DataIntegrityManager:
    """
    Manages data integrity checking and repair operations.
    Handles both partition-level and data-level integrity.
    """

    BYBIT_LAUNCH_DATE = int(datetime(2019, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)
    MAX_FUTURE_TOLERANCE = 60  # seconds
    MAX_DECIMAL_PLACES = 10

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
                WHERE NOT EXISTS (
                    SELECT 1 FROM symbols s
                    WHERE s.id = k.symbol_id
                )
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

        Performs comprehensive validation including:
        - Time range validity considering existing data
        - Detection of time gaps in 5-minute intervals
        - Price and volume anomaly detection
        - Duplicate timestamp detection

        Args:
            symbol_id: Database ID for the symbol
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

        Raises:
            DataValidationError: If validation query fails
        """
        try:
            # Get the latest timestamp for this symbol
            latest_ts = self.session.query(func.max(Kline.start_time))\
                .filter_by(symbol_id=symbol_id)\
                .scalar() or 0

            # Check for overlapping data
            overlap = self.session.query(Kline)\
                .filter(Kline.symbol_id == symbol_id,
                    Kline.start_time >= start_time,
                    Kline.start_time <= end_time)\
                .count()

            # More permissive time range validation
            time_range_valid = (
                end_time > start_time and  # Basic sanity check
                (start_time >= latest_ts or  # Either starting after last record
                overlap == 0)  # Or no overlapping records if starting earlier
            )

            # Check for time gaps (periods > 5 minutes)
            gaps = self.session.execute(text("""
                WITH time_series AS (
                    SELECT
                        start_time,
                        LEAD(start_time) OVER (ORDER BY start_time) as next_start
                    FROM kline_data USE INDEX (idx_symbol_time)
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

            # Check for price anomalies with dynamic thresholds
            anomalies = self.session.execute(text("""
                WITH price_stats AS (
                    SELECT
                        AVG(high_price - low_price) as avg_range,
                        STDDEV(high_price - low_price) as stddev_range,
                        AVG(volume) as avg_volume,
                        STDDEV(volume) as stddev_volume
                    FROM kline_data USE INDEX (idx_symbol_time)
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time - 86400000 AND :end_time  -- Include last 24h for better statistics
                )
                SELECT COUNT(*) as anomaly_count
                FROM kline_data k, price_stats s
                WHERE k.symbol_id = :symbol_id
                AND start_time BETWEEN :start_time AND :end_time
                AND (
                    -- Check for price anomalies
                    (high_price - low_price) > (avg_range + 3 * stddev_range)
                    OR volume > (avg_volume + 3 * stddev_volume)
                    -- Check for invalid price relationships
                    OR high_price < low_price
                    OR open_price < low_price
                    OR open_price > high_price
                    OR close_price < low_price
                    OR close_price > high_price
                    -- Check for invalid values
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

            # Log validation results for debugging
            symbol_name = self.session.query(Symbol.name)\
                .filter(Symbol.id == symbol_id)\
                .scalar() or str(symbol_id)

            logger.debug(
                f"Validation results for {symbol_name} "
                f"({datetime.fromtimestamp(start_time/1000, tz=timezone.utc)} to "
                f"{datetime.fromtimestamp(end_time/1000, tz=timezone.utc)}): "
                f"gaps={gaps}, anomalies={anomalies}, duplicates={duplicates}, "
                f"time_range_valid={time_range_valid}"
            )

            return {
                'has_gaps': gaps > 0,
                'gap_count': gaps,
                'anomaly_count': anomalies,
                'duplicates_found': duplicates,
                'time_range_valid': time_range_valid,
                'data_valid': gaps == 0 and anomalies == 0 and duplicates == 0,
                'overlap_count': overlap
            }

        except Exception as e:
            logger.error(f"Failed to validate kline data: {str(e)}")
            raise DataValidationError(f"Failed to validate kline data: {str(e)}")

    def validate_kline(self, kline_data: Tuple) -> bool:
        """
        Validate a single kline record with enhanced checks.
        """
        try:
            if len(kline_data) != 7:
                raise DataValidationError("Invalid kline data format")

            timestamp, open_price, high, low, close, volume, turnover = kline_data

            # Enhanced timestamp validation
            if not isinstance(timestamp, int):
                raise DataValidationError("Timestamp must be an integer")
            if len(str(abs(timestamp))) != 13:
                raise DataValidationError("Timestamp must be in milliseconds (13 digits)")
            if timestamp < self.BYBIT_LAUNCH_DATE:
                raise DataValidationError(f"Timestamp is before exchange launch ({self.BYBIT_LAUNCH_DATE})")

            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            max_allowed = datetime.now(timezone.utc) + timedelta(seconds=self.MAX_FUTURE_TOLERANCE)
            if dt > max_allowed:
                raise DataValidationError(f"Timestamp is too far in the future")

            # Price validation with decimal place check
            for price in (open_price, high, low, close):
                if not isinstance(price, (int, float)) or price <= 0:
                    raise DataValidationError(f"Invalid price value: {price}")
                decimal_str = str(float(price)).split('.')[-1]
                if len(decimal_str) > self.MAX_DECIMAL_PLACES:
                    raise DataValidationError(f"Price has too many decimal places: {price}")

            # Volume and turnover validation
            for value in (volume, turnover):
                if not isinstance(value, (int, float)) or value < 0:
                    raise DataValidationError(f"Invalid volume/turnover value: {value}")

            # Price relationship validation
            if not (low <= min(open_price, close) <= max(open_price, close) <= high):
                raise DataValidationError("Invalid price relationships in OHLC data")

            return True

        except DataValidationError:
            raise
        except Exception as e:
            raise DataValidationError(f"Validation failed: {str(e)}")

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