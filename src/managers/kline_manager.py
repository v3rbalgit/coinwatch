# src/managers/kline_manager.py

import time
import random
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, text, select
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, timezone, timedelta
from src.models.symbol import Symbol
from src.models.kline import Kline
from src.services.monitoring.collectors.insert_metrics_collector import InsertMetricsCollector
from src.utils.exceptions import DataValidationError, DatabaseError
from src.utils.db_retry import with_db_retry
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class KlineManager:
    """
    Manages all kline (candlestick) data operations.

    Handles:
    - Symbol management (creation/retrieval)
    - Kline data validation
    - Data insertion and updates
    - Data retrieval
    - Symbol removal

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
        Initialize KlineManager with database session.

        Args:
            session: Active SQLAlchemy database session
        """
        self.session = session
        self.INITIAL_BATCH_SIZE = 10
        self.MAX_BATCH_SIZE = 50
        self._current_batch_size = self.INITIAL_BATCH_SIZE
        self._success_count = 0
        self._failure_count = 0
        self.metrics_collector = InsertMetricsCollector(session)
        logger.debug("KlineManager initialized with metrics collection")

    @with_db_retry(max_attempts=3)
    def get_symbol_id(self, symbol_name: str) -> int:
        """
        Get or create a database ID for a trading symbol.

        Args:
            symbol_name: Trading pair symbol name (e.g., 'BTCUSDT')

        Returns:
            int: Database ID of the symbol

        Raises:
            DatabaseError: If symbol creation fails
        """
        try:
            # First try a simple select without locking
            stmt = select(Symbol).where(Symbol.name == symbol_name)
            symbol = self.session.execute(stmt).scalar_one_or_none()

            if symbol:
                return symbol.id

            # If no symbol exists, we need to create one with proper locking
            try:
                # Start a nested transaction
                with self.session.begin_nested():
                    # Now get with lock
                    stmt = select(Symbol).where(Symbol.name == symbol_name).with_for_update()
                    symbol = self.session.execute(stmt).scalar_one_or_none()

                    if symbol:  # Double-check after acquiring lock
                        return symbol.id

                    # Create new symbol
                    symbol = Symbol(name=symbol_name)
                    self.session.add(symbol)
                    # Flush to get the ID but keep in transaction
                    self.session.flush()
                    return symbol.id

            except IntegrityError:
                # Handle race condition - another transaction may have created the symbol
                self.session.rollback()
                # Try one more time to get the symbol
                stmt = select(Symbol).where(Symbol.name == symbol_name)
                symbol = self.session.execute(stmt).scalar_one_or_none()
                if symbol:
                    return symbol.id
                raise DatabaseError(f"Failed to get/create symbol after concurrent insert: {symbol_name}")

        except Exception as e:
            logger.error(f"Error processing symbol {symbol_name}: {str(e)}", exc_info=True)
            raise DatabaseError(f"Symbol processing error: {symbol_name}: {str(e)}")

    def get_latest_timestamp(self, symbol_id: int) -> Optional[int]:
        """
        Get the most recent kline timestamp for a symbol.

        Args:
            symbol_id: Database ID of the symbol

        Returns:
            Optional[int]: Most recent timestamp in milliseconds, or
                          30 minutes ago if no data exists
        """
        try:
            stmt = select(func.max(Kline.start_time)).where(Kline.symbol_id == symbol_id)
            result = self.session.execute(stmt).scalar_one_or_none()

            if result is None:
                # Get symbol name for logging
                symbol_stmt = select(Symbol.name).where(Symbol.id == symbol_id)
                symbol_name = self.session.execute(symbol_stmt).scalar_one_or_none()

                start_time = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
                logger.info(
                    f"No data for {symbol_name or symbol_id}, starting from: "
                    f"{datetime.fromtimestamp(start_time/1000, tz=timezone.utc)}"
                )
                return start_time

            return result

        except Exception as e:
            logger.error(f"Failed to get latest timestamp for symbol {symbol_id}: {e}")
            raise DatabaseError(f"Timestamp query failed for symbol {symbol_id}: {str(e)}")

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
            kline_data: Tuple of (timestamp, open, high, close, low, volume, turnover)

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

    def insert_kline_data(self, symbol_id: int, kline_data: List[Tuple], batch_size: Optional[int] = None) -> None:
        """
        Insert or update kline data with batching, error handling, and performance metrics.

        Args:
            symbol_id: Database ID for the symbol
            kline_data: List of kline data tuples
            batch_size: Optional override for batch size

        Raises:
            DatabaseError: If insertion fails after retries
        """
        if not kline_data:
            return

        batch_size = batch_size or self._current_batch_size
        start_time = time.time()
        total_records = len(kline_data)
        total_batches = (total_records + batch_size - 1) // batch_size
        successful_batches = 0

        try:
            # Sort data by timestamp
            kline_data.sort(key=lambda x: x[0])

            for i in range(0, len(kline_data), batch_size):
                batch = kline_data[i:i + batch_size]
                success = False
                retries = 0
                batch_start_time = time.time()

                while not success and retries < 3:
                    try:
                        with self.session.begin_nested():
                            # Prepare the values for insertion
                            values = [
                                {
                                    'symbol_id': symbol_id,
                                    'start_time': item[0],
                                    'open_price': item[1],
                                    'high_price': item[2],
                                    'low_price': item[3],
                                    'close_price': item[4],
                                    'volume': item[5],
                                    'turnover': item[6]
                                }
                                for item in batch
                            ]

                            # Execute the UPSERT
                            stmt = text("""
                                INSERT INTO kline_data (
                                    symbol_id, start_time, open_price, high_price,
                                    low_price, close_price, volume, turnover
                                ) VALUES (
                                    :symbol_id, :start_time, :open_price, :high_price,
                                    :low_price, :close_price, :volume, :turnover
                                ) AS new_values
                                ON DUPLICATE KEY UPDATE
                                    open_price = new_values.open_price,
                                    high_price = new_values.high_price,
                                    low_price = new_values.low_price,
                                    close_price = new_values.close_price,
                                    volume = new_values.volume,
                                    turnover = new_values.turnover
                            """)

                            for value in values:
                                self.session.execute(stmt, value)

                            success = True
                            successful_batches += 1
                            self._success_count += 1

                            # Record metrics for successful batch
                            batch_duration = time.time() - batch_start_time
                            self.metrics_collector.record_insert_operation(
                                success=True,
                                batch_size=len(batch),
                                retries=retries,
                                latency_ms=batch_duration * 1000  # Convert to milliseconds
                            )

                            # Log batch completion with timing info
                            logger.debug(
                                f"Inserted batch {successful_batches}/{total_batches} "
                                f"({len(batch)} records) in {batch_duration:.2f}s"
                            )

                    except Exception as e:
                        retries += 1
                        self._failure_count += 1

                        # Record failed attempt metrics
                        failed_duration = time.time() - batch_start_time
                        self.metrics_collector.record_insert_operation(
                            success=False,
                            batch_size=len(batch),
                            retries=retries,
                            latency_ms=failed_duration * 1000
                        )

                        if retries >= 3:
                            raise DatabaseError(
                                f"Failed to insert batch after {retries} attempts: {str(e)}"
                            )

                        # Exponential backoff with logging
                        sleep_time = 0.5 * (2 ** (retries - 1))  # 0.5s, 1s, 2s
                        logger.warning(
                            f"Batch insertion failed (attempt {retries}/3), "
                            f"retrying in {sleep_time:.1f}s: {str(e)}"
                        )
                        time.sleep(sleep_time)

            # Commit the transaction
            self.session.commit()

            # Calculate and log overall performance metrics
            total_duration = time.time() - start_time
            average_batch_duration = total_duration / total_batches if total_batches > 0 else 0
            records_per_second = total_records / total_duration if total_duration > 0 else 0

            logger.info(
                f"Completed insertion of {total_records} records in {total_duration:.2f}s "
                f"({records_per_second:.1f} records/s). "
                f"Successful batches: {successful_batches}/{total_batches}, "
                f"Average batch duration: {average_batch_duration:.3f}s"
            )

            # Adjust batch size based on performance
            if successful_batches == total_batches:  # All batches successful
                self._adjust_batch_size()

        except Exception as e:
            self.session.rollback()
            total_duration = time.time() - start_time
            logger.error(
                f"Failed to insert kline data after {total_duration:.2f}s: {str(e)}"
            )
            raise DatabaseError(f"Failed to insert kline data: {str(e)}")

    def _adjust_batch_size(self) -> None:
        """
        Dynamically adjust batch size based on success/failure ratio and current performance.
        """
        if self._success_count >= 5:
            # Gradually increase batch size after consistent success
            new_batch_size = min(
                self._current_batch_size + 5,
                self.MAX_BATCH_SIZE
            )
            if new_batch_size != self._current_batch_size:
                logger.debug(
                    f"Increasing batch size from {self._current_batch_size} to {new_batch_size}"
                )
                self._current_batch_size = new_batch_size
            self._success_count = 0
        elif self._failure_count > 0:
            # Immediately reduce batch size on failure
            new_batch_size = max(
                self.INITIAL_BATCH_SIZE,
                self._current_batch_size // 2
            )
            logger.debug(
                f"Reducing batch size from {self._current_batch_size} to {new_batch_size}"
            )
            self._current_batch_size = new_batch_size
            self._failure_count = 0

    def get_all_symbols(self) -> List[str]:
        """
        Get all symbols currently in database.

        Returns:
            List[str]: List of symbol names
        """
        try:
            result = self.session.execute(
                select(Symbol.name)
            ).scalars().all()

            return list(result)
        except Exception as e:
            logger.error(f"Failed to get symbols: {e}")
            raise DatabaseError(f"Failed to get symbols: {str(e)}")

    def add_symbol(self, symbol_name: str) -> int:
        """
        Add a new symbol to database.

        Args:
            symbol_name: Symbol to add

        Returns:
            int: Symbol ID
        """
        try:
            symbol = Symbol(name=symbol_name)
            self.session.add(symbol)
            self.session.flush()  # Get ID without committing
            return symbol.id
        except IntegrityError:
            self.session.rollback()
            # Symbol might have been added by another process
            return self.get_symbol_id(symbol_name)
        except Exception as e:
            logger.error(f"Failed to add symbol {symbol_name}: {e}")
            raise DatabaseError(f"Failed to add symbol: {str(e)}")

    def remove_symbol(self, symbol_name: str) -> None:
        """
        Remove a symbol and its data from database.

        Args:
            symbol_name: Symbol to remove
        """
        try:
            symbol = self.session.query(Symbol).filter_by(name=symbol_name).first()
            if symbol:
                # Delete associated klines first
                self.session.query(Kline).filter_by(symbol_id=symbol.id).delete()
                self.session.delete(symbol)
                logger.info(f"Removed symbol {symbol_name} and its data")
        except Exception as e:
            logger.error(f"Failed to remove symbol {symbol_name}: {e}")
            raise DatabaseError(f"Failed to remove symbol: {str(e)}")

    def get_symbol_data(self, symbol_name: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
        """
        Retrieve kline data for a symbol within a time range.

        Args:
            symbol_name: Trading pair symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            List of kline data dictionaries
        """
        try:
            symbol_id = self.get_symbol_id(symbol_name)
            klines = self.session.query(Kline)\
                .filter(
                    Kline.symbol_id == symbol_id,
                    Kline.start_time >= start_time,
                    Kline.start_time <= end_time
                )\
                .order_by(Kline.start_time)\
                .all()

            return [{
                'timestamp': kline.start_time,
                'open': float(kline.open_price),
                'high': float(kline.high_price),
                'low': float(kline.low_price),
                'close': float(kline.close_price),
                'volume': float(kline.volume),
                'turnover': float(kline.turnover)
            } for kline in klines]

        except Exception as e:
            logger.error(f"Failed to retrieve data for {symbol_name}: {e}")
            raise DatabaseError(f"Data retrieval failed for {symbol_name}: {str(e)}")