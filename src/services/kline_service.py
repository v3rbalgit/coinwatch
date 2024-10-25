# src/services/kline_service.py

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import func
from typing import Optional, List, Tuple, cast
from datetime import datetime, timezone, timedelta
from src.models.symbol import Symbol
from src.models.kline import Kline
from src.utils.integrity import DataIntegrityManager
from src.utils.exceptions import DataValidationError, DatabaseError
from src.utils.db_retry import with_db_retry
from src.utils.timestamp import from_timestamp
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

KlineDataTuple = Tuple[int, float, float, float, float, float, float]

@with_db_retry(max_attempts=3)
def get_symbol_id(session: Session, symbol_name: str) -> int:
    """
    Get or create a database ID for a trading symbol.

    Implements an upsert pattern with race condition handling:
    1. Attempts to find existing symbol
    2. Creates new symbol if not found
    3. Handles potential race conditions during creation

    Args:
        session: Active database session
        symbol_name: Trading pair symbol name (e.g., 'BTCUSDT')

    Returns:
        int: Database ID of the symbol

    Raises:
        DatabaseError: If symbol creation fails or database error occurs
    """
    try:
        logger.debug(f"Looking up symbol: {symbol_name}")
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()

        if not symbol:
            symbol = Symbol(name=symbol_name)
            session.add(symbol)
            session.flush()  # Get ID without committing
            logger.info(f"Created new symbol: {symbol_name} (ID: {symbol.id})")
        else:
            logger.debug(f"Found existing symbol: {symbol_name} (ID: {symbol.id})")

        return cast(int, symbol.id)

    except IntegrityError:
        session.rollback()
        # Handle potential race condition
        logger.warning(f"Race condition detected for symbol: {symbol_name}, retrying lookup")
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            return cast(int, symbol.id)
        raise DatabaseError(f"Failed to get/create symbol: {symbol_name}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error processing symbol {symbol_name}: {e}", exc_info=True)
        raise DatabaseError(f"Symbol processing error: {symbol_name}: {str(e)}")

def get_latest_timestamp(session: Session, symbol_id: int) -> Optional[int]:
    """
    Get the most recent kline timestamp for a symbol.

    If no data exists, returns a timestamp from 30 minutes ago
    to ensure we don't miss recent data.

    Args:
        session: Active database session
        symbol_id: Database ID of the symbol

    Returns:
        Optional[int]: Most recent timestamp in milliseconds, or
                      30 minutes ago if no data exists

    Raises:
        DatabaseError: If database query fails
    """
    try:
        result = session.query(func.max(Kline.start_time)).filter_by(symbol_id=symbol_id).scalar()

        if result is None:
            symbol = session.query(Symbol.name).filter_by(id=symbol_id).scalar()
            start_time = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
            logger.info(
                f"No data for {symbol or symbol_id}, starting from: "
                f"{datetime.fromtimestamp(start_time/1000, tz=timezone.utc)}"
            )
            return start_time

        return result

    except Exception as e:
        logger.error(f"Failed to get latest timestamp for symbol {symbol_id}: {e}")
        raise DatabaseError(f"Timestamp query failed for symbol {symbol_id}: {str(e)}")

@with_db_retry(max_attempts=3)
def insert_kline_data(session: Session, symbol_id: int,
                     kline_data: List[KlineDataTuple], batch_size: int = 1000) -> None:
    """
    Insert kline (candlestick) data with comprehensive validation.

    Process:
    1. Verifies symbol existence
    2. Filters out existing records
    3. Validates data integrity
    4. Performs batch insertion
    5. Handles duplicate records with updates

    Args:
        session: Active database session
        symbol_id: Database ID of the symbol
        kline_data: List of kline tuples (timestamp, open, high, low, close, volume, turnover)
        batch_size: Number of records per batch insert (default: 1000)

    Raises:
        DataValidationError: If data fails validation
        DatabaseError: If database operation fails
    """
    try:
        # Verify symbol exists
        symbol_exists = session.query(
            session.query(Symbol).filter_by(id=symbol_id).exists()
        ).scalar()

        if not symbol_exists:
            raise DataValidationError(f"Symbol ID {symbol_id} not found")

        if not kline_data:
            logger.debug(f"No data to insert for symbol {symbol_id}")
            return

        # Filter out existing records
        latest_ts = session.query(func.max(Kline.start_time))\
            .filter_by(symbol_id=symbol_id)\
            .scalar() or 0

        filtered_data = sorted(
            [data for data in kline_data if data[0] > latest_ts],
            key=lambda x: x[0]
        )

        if not filtered_data:
            logger.debug(f"No new data to insert for symbol {symbol_id}")
            return

        # Log processing details
        logger.debug(
            f"Processing {len(filtered_data)} records for symbol {symbol_id} "
            f"(filtered from {len(kline_data)} total)"
        )

        if filtered_data:
            logger.debug(
                f"Time range: {from_timestamp(filtered_data[0][0])} to "
                f"{from_timestamp(filtered_data[-1][0])}"
            )

        # Validate data
        integrity_manager = DataIntegrityManager(session)
        validation_errors = []

        for data in filtered_data:
            try:
                if not integrity_manager.validate_kline(data):
                    validation_errors.append(
                        f"Validation failed for timestamp "
                        f"{datetime.fromtimestamp(data[0]/1000, tz=timezone.utc)}"
                    )
            except Exception as e:
                validation_errors.append(str(e))

        if validation_errors:
            error_msg = f"Validation errors for symbol {symbol_id}: {'; '.join(validation_errors)}"
            logger.error(error_msg)
            raise DataValidationError(error_msg)

        # Insert in batches
        for i in range(0, len(filtered_data), batch_size):
            batch = filtered_data[i:i + batch_size]
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

            # Upsert statement
            stmt = insert(Kline).values(values)
            stmt = stmt.on_duplicate_key_update({
                'open_price': stmt.inserted.open_price,
                'high_price': stmt.inserted.high_price,
                'low_price': stmt.inserted.low_price,
                'close_price': stmt.inserted.close_price,
                'volume': stmt.inserted.volume,
                'turnover': stmt.inserted.turnover
            })

            session.execute(stmt)
            session.flush()
            logger.debug(f"Inserted batch of {len(batch)} klines for symbol {symbol_id}")

    except DataValidationError:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert kline data for symbol {symbol_id}: {e}", exc_info=True)
        raise DatabaseError(f"Kline insertion failed for symbol {symbol_id}: {str(e)}")

def remove_symbol(session: Session, symbol_name: str) -> None:
    """
    Remove a symbol and all its associated kline data.

    Performs a cascading delete that:
    1. Verifies symbol existence
    2. Deletes associated kline records
    3. Removes symbol entry
    4. Maintains referential integrity

    Args:
        session: Active database session
        symbol_name: Name of the symbol to remove

    Raises:
        DatabaseError: If removal process fails

    Note:
        This operation is irreversible and should be used with caution.
        Includes logging of removed record counts for audit purposes.
    """
    try:
        # Query symbol first to avoid unnecessary database operations
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()

        if symbol:
            # Log current record count for validation
            kline_count = session.query(Kline).filter_by(symbol_id=symbol.id).count()

            logger.info(
                f"Removing symbol '{symbol_name}' "
                f"(ID: {symbol.id}) with {kline_count:,} kline records"
            )

            # Delete klines first to maintain referential integrity
            deleted_klines = session.query(Kline)\
                .filter_by(symbol_id=symbol.id)\
                .delete()

            # Delete the symbol
            session.delete(symbol)

            # Verify deletion counts match
            if deleted_klines != kline_count:
                logger.warning(
                    f"Kline deletion count mismatch for {symbol_name}: "
                    f"expected {kline_count}, deleted {deleted_klines}"
                )

            logger.info(
                f"Successfully removed symbol '{symbol_name}' and "
                f"{deleted_klines:,} associated klines"
            )
        else:
            logger.warning(f"Symbol not found for removal: {symbol_name}")

    except Exception as e:
        error_msg = f"Failed to remove symbol '{symbol_name}'"
        logger.error(f"{error_msg}: {e}", exc_info=True)
        raise DatabaseError(f"{error_msg}: {str(e)}")