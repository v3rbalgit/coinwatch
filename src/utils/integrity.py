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
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class DataIntegrityManager:
    """
    Manages data integrity checking and repair operations.

    Provides comprehensive data validation and repair functionality:
    - Partition integrity verification
    - Time series data validation
    - Price and volume validation
    - Anomaly detection
    - Data repair operations

    Constants:
    - BYBIT_LAUNCH_DATE: Earliest valid timestamp
    - MAX_FUTURE_TOLERANCE: Maximum allowed future timestamp
    - MAX_DECIMAL_PLACES: Maximum price decimal places
    """

    BYBIT_LAUNCH_DATE = int(datetime(2019, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)
    MAX_FUTURE_TOLERANCE = 60  # seconds
    MAX_DECIMAL_PLACES = 10

    def __init__(self, session: Session):
        """
        Initialize integrity manager with database session.

        Args:
            session: Active SQLAlchemy database session
        """
        self.session = session
        logger.debug("Data integrity manager initialized")

    @with_db_retry(max_attempts=3)
    def verify_partition_integrity(self) -> Dict[str, Any]:
        """
        Verify database partition integrity.

        Checks:
        - Orphaned records (no matching symbol)
        - Partition gaps
        - Overall partition validity

        Returns:
            Dict containing:
                - orphaned_records: Count of records with no matching symbol
                - partition_gaps: Number of gaps between partitions
                - partitions_valid: Boolean indicating overall validity

        Raises:
            DataValidationError: If verification process fails
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

            if orphaned > 0:
                logger.warning(f"Found {orphaned:,} orphaned records")

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

            gap_count = len(partition_gaps)
            if gap_count > 0:
                logger.warning(f"Found {gap_count} partition gaps")

            result = {
                'orphaned_records': orphaned,
                'partition_gaps': gap_count,
                'partitions_valid': gap_count == 0 and orphaned == 0
            }

            logger.info(
                f"Partition integrity verification completed: "
                f"{'valid' if result['partitions_valid'] else 'invalid'}"
            )
            return result

        except Exception as e:
            logger.error("Partition integrity verification failed", exc_info=e)
            raise DataValidationError(f"Partition integrity verification failed: {str(e)}")

    @with_db_retry(max_attempts=3)
    def validate_kline_data(self, symbol_id: int, start_time: int, end_time: int) -> Dict[str, Any]:
        """
        Validate kline data for a specific time range.

        Performs comprehensive validation including:
        1. Time range and overlap checks
        2. Gap detection in 5-minute intervals
        3. Price and volume anomaly detection using statistical methods
        4. Duplicate timestamp detection
        5. Data relationship validation

        Args:
            symbol_id: Database ID for the symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            Dict containing:
                - has_gaps: True if time gaps exist
                - gap_count: Number of gaps found
                - anomaly_count: Number of price/volume anomalies
                - time_range_valid: True if time range is valid
                - data_valid: True if all checks pass
                - duplicates_found: Number of duplicate timestamps
                - overlap_count: Number of overlapping records

        Raises:
            DataValidationError: If validation query fails
        """
        try:
            # Get symbol info for logging context
            symbol_name = self.session.query(Symbol.name)\
                .filter(Symbol.id == symbol_id)\
                .scalar() or str(symbol_id)

            logger.debug(
                f"Validating data for {symbol_name} from "
                f"{datetime.fromtimestamp(start_time/1000, tz=timezone.utc)} to "
                f"{datetime.fromtimestamp(end_time/1000, tz=timezone.utc)}"
            )

            # Get the latest timestamp
            latest_ts = self.session.query(func.max(Kline.start_time))\
                .filter_by(symbol_id=symbol_id)\
                .scalar() or 0

            # Check for overlapping data
            overlap = self.session.query(Kline)\
                .filter(Kline.symbol_id == symbol_id,
                    Kline.start_time >= start_time,
                    Kline.start_time <= end_time)\
                .count()

            if overlap > 0:
                logger.debug(f"Found {overlap} overlapping records for {symbol_name}")

            # Time range validation
            time_range_valid = (
                end_time > start_time and  # Basic sanity check
                (start_time >= latest_ts or  # Either starting after last record
                overlap == 0)  # Or no overlapping records if starting earlier
            )

            # Check for time gaps
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

            if gaps > 0:
                logger.warning(f"Found {gaps} time gaps for {symbol_name}")

            # Check for price/volume anomalies
            anomalies = self.session.execute(text("""
                WITH price_stats AS (
                    SELECT
                        AVG(high_price - low_price) as avg_range,
                        STDDEV(high_price - low_price) as stddev_range,
                        AVG(volume) as avg_volume,
                        STDDEV(volume) as stddev_volume
                    FROM kline_data USE INDEX (idx_symbol_time)
                    WHERE symbol_id = :symbol_id
                    AND start_time BETWEEN :start_time - 86400000 AND :end_time
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

            if anomalies > 0:
                logger.warning(f"Found {anomalies} price/volume anomalies for {symbol_name}")

            # Check for duplicates
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

            if duplicates > 0:
                logger.warning(f"Found {duplicates} duplicate timestamps for {symbol_name}")

            validation_results = {
                'has_gaps': gaps > 0,
                'gap_count': gaps,
                'anomaly_count': anomalies,
                'duplicates_found': duplicates,
                'time_range_valid': time_range_valid,
                'data_valid': gaps == 0 and anomalies == 0 and duplicates == 0,
                'overlap_count': overlap
            }

            logger.debug(f"Validation results for {symbol_name}: {validation_results}")
            return validation_results

        except Exception as e:
            logger.error(
                f"Data validation failed for {symbol_name}: {e}",
                exc_info=True
            )
            raise DataValidationError(f"Data validation failed: {str(e)}")

    def validate_kline(self, kline_data: Tuple) -> bool:
        """
        Validate a single kline (candlestick) record.

        Performs comprehensive validation checks:
        1. Data format validation
        2. Timestamp validation (past/future bounds)
        3. Price value and decimal place validation
        4. Volume and turnover validation
        5. OHLC price relationship validation

        Args:
            kline_data: Tuple of (timestamp, open, high, low, close, volume, turnover)

        Returns:
            bool: True if record passes all validations

        Raises:
            DataValidationError: If any validation check fails

        Note:
            Timestamp must be:
            - Integer in milliseconds (13 digits)
            - After exchange launch date
            - Not too far in the future
        """
        try:
            if len(kline_data) != 7:
                raise DataValidationError(f"Invalid kline format: expected 7 values, got {len(kline_data)}")

            timestamp, open_price, high, low, close, volume, turnover = kline_data

            # Timestamp validation
            if not isinstance(timestamp, int):
                raise DataValidationError(f"Invalid timestamp type: {type(timestamp)}, must be int")

            if len(str(abs(timestamp))) != 13:
                raise DataValidationError(f"Invalid timestamp length: {len(str(abs(timestamp)))}, must be 13 digits")

            if timestamp < self.BYBIT_LAUNCH_DATE:
                launch_date = datetime.fromtimestamp(self.BYBIT_LAUNCH_DATE/1000, tz=timezone.utc)
                raise DataValidationError(f"Timestamp before exchange launch ({launch_date})")

            # Future timestamp check
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            max_allowed = datetime.now(timezone.utc) + timedelta(seconds=self.MAX_FUTURE_TOLERANCE)
            if dt > max_allowed:
                raise DataValidationError(
                    f"Future timestamp not allowed: {dt} > {max_allowed}"
                )

            # Price validation
            for name, price in [
                ('open', open_price),
                ('high', high),
                ('low', low),
                ('close', close)
            ]:
                if not isinstance(price, (int, float)):
                    raise DataValidationError(f"Invalid {name} price type: {type(price)}")

                if price <= 0:
                    raise DataValidationError(f"Non-positive {name} price: {price}")

                decimal_str = str(float(price)).split('.')[-1]
                if len(decimal_str) > self.MAX_DECIMAL_PLACES:
                    raise DataValidationError(
                        f"{name} price has too many decimal places: "
                        f"{len(decimal_str)} > {self.MAX_DECIMAL_PLACES}"
                    )

            # Volume and turnover validation
            for name, value in [('volume', volume), ('turnover', turnover)]:
                if not isinstance(value, (int, float)):
                    raise DataValidationError(f"Invalid {name} type: {type(value)}")

                if value < 0:
                    raise DataValidationError(f"Negative {name}: {value}")

            # Price relationship validation
            if not (low <= min(open_price, close) <= max(open_price, close) <= high):
                raise DataValidationError(
                    f"Invalid OHLC relationships: "
                    f"O={open_price}, H={high}, L={low}, C={close}"
                )

            logger.debug(
                f"Validated kline: {dt} - "
                f"O:{open_price}, H:{high}, L:{low}, C:{close}, V:{volume}"
            )
            return True

        except DataValidationError:
            raise
        except Exception as e:
            logger.error("Unexpected validation error", exc_info=e)
            raise DataValidationError(f"Validation failed: {str(e)}")

    def repair_data_issues(self, issues: Dict[str, Any]) -> Dict[str, Any]:
        """
        Repair detected data integrity issues.

        Performs the following repairs:
        1. Removes orphaned records
        2. Removes duplicate timestamps (keeps most recent)
        3. Removes records with invalid price relationships
        4. Removes records with invalid values

        Args:
            issues: Dictionary of detected issues to repair

        Returns:
            Dict containing:
                - orphaned_removed: Count of orphaned records removed
                - duplicates_removed: Count of duplicate records removed
                - anomalies_removed: Count of anomalous records removed
                - success: Whether repair operations succeeded
                - error: Error message if repair failed
        """
        try:
            repairs = {
                'orphaned_removed': 0,
                'duplicates_removed': 0,
                'anomalies_removed': 0,
                'success': False
            }

            # Remove orphaned records
            if issues.get('orphaned_records', 0) > 0:
                result = cast(CursorResult, self.session.execute(text("""
                    DELETE FROM kline_data
                    WHERE symbol_id NOT IN (SELECT id FROM symbols)
                """)))
                repairs['orphaned_removed'] = result.rowcount
                logger.info(f"Removed {repairs['orphaned_removed']:,} orphaned records")

            # Remove duplicate timestamps
            if issues.get('duplicates_found', 0) > 0:
                result = cast(CursorResult, self.session.execute(text("""
                    DELETE k1 FROM kline_data k1
                    INNER JOIN kline_data k2
                    WHERE k1.symbol_id = k2.symbol_id
                    AND k1.start_time = k2.start_time
                    AND k1.id < k2.id
                """)))
                repairs['duplicates_removed'] = result.rowcount
                logger.info(f"Removed {repairs['duplicates_removed']:,} duplicate records")

            # Remove anomalous records
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
            logger.info(f"Removed {repairs['anomalies_removed']:,} anomalous records")

            self.session.commit()
            repairs['success'] = True

            total_removed = sum(
                repairs[k] for k in ['orphaned_removed', 'duplicates_removed', 'anomalies_removed']
            )
            logger.info(f"Data repair completed: {total_removed:,} total records removed")
            return repairs

        except Exception as e:
            self.session.rollback()
            logger.error("Data repair failed", exc_info=e)
            return {**repairs, 'success': False, 'error': str(e)}