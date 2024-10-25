# src/services/kline_service.py

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import func
import logging
import traceback
from typing import Optional, List, Tuple, cast
from datetime import datetime, timezone, timedelta
from src.models.symbol import Symbol
from src.models.kline import Kline
from src.utils.integrity import DataIntegrityManager
from src.utils.exceptions import DataValidationError, DatabaseError
from src.utils.db_retry import with_db_retry
from src.utils.timestamp import from_timestamp

logger = logging.getLogger(__name__)

@with_db_retry(max_attempts=3)
def get_symbol_id(session: Session, symbol_name: str) -> int:
    """
    Get or create the database ID for a given symbol.

    Args:
        session: Database session
        symbol_name: Name of the symbol

    Returns:
        int: Database ID of the symbol

    Raises:
        DatabaseError: If unable to get or create symbol
    """
    try:
        logger.info(f"Attempting to get or create symbol: {symbol_name}")
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if not symbol:
            symbol = Symbol(name=symbol_name)
            session.add(symbol)
            session.flush()  # Use flush to get the ID without committing
            logger.info(f"Added new symbol: {symbol_name} with ID {symbol.id}")
        else:
            logger.info(f"Symbol found: {symbol_name} with ID {symbol.id}")
        return cast(int, symbol.id)
    except IntegrityError:
        session.rollback()
        # Handle race condition by querying again
        logger.warning(f"IntegrityError occurred while adding symbol: {symbol_name}. Retrying.")
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            return cast(int, symbol.id)
        raise DatabaseError(f"Failed to get or create symbol: {symbol_name}")
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing symbol: {symbol_name}: {str(e)}")
        raise DatabaseError(f"Error processing symbol: {symbol_name}: {str(e)}")

def get_latest_timestamp(session: Session, symbol_id: int) -> Optional[int]:
    """
    Get the most recent timestamp for a given symbol.

    Args:
        session: Database session
        symbol_id: Symbol ID

    Returns:
        Optional[int]: Latest timestamp or timestamp from 30 minutes ago if no data exists

    Raises:
        DatabaseError: If query fails
    """
    try:
        result = session.query(func.max(Kline.start_time))\
            .filter_by(symbol_id=symbol_id)\
            .scalar()

        if result is None:
            symbol = session.query(Symbol.name).filter_by(id=symbol_id).scalar()
            logger.info(f"No data found for {symbol or symbol_id}, starting from 30 minutes ago")
            return int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)

        # Return exact timestamp - we'll handle overlap in validation
        return result

    except Exception as e:
        raise DatabaseError(f"Failed to get latest timestamp for symbol_id {symbol_id}: {str(e)}")

@with_db_retry(max_attempts=3)
def insert_kline_data(session: Session, symbol_id: int, kline_data: List[Tuple], batch_size: int = 1000) -> None:
    """Insert kline data with integrity checks."""
    try:
        # Verify symbol exists first
        symbol_exists = session.query(
            session.query(Symbol).filter_by(id=symbol_id).exists()
        ).scalar()

        if not symbol_exists:
            raise DataValidationError(f"Symbol ID {symbol_id} does not exist")

        if not kline_data:
            logger.debug(f"No data to insert for symbol_id {symbol_id}")
            return

        # Get the latest timestamp from existing data
        latest_ts = session.query(func.max(Kline.start_time))\
            .filter_by(symbol_id=symbol_id)\
            .scalar() or 0

        # Filter out any records we already have and sort by timestamp
        filtered_data = sorted(
            [data for data in kline_data if data[0] > latest_ts],
            key=lambda x: x[0]
        )

        if not filtered_data:
            logger.debug(f"No new data to insert for symbol_id {symbol_id}")
            return

        # Log the timestamps we're working with
        logger.debug(f"Symbol {symbol_id} - Processing {len(filtered_data)} records:")
        if filtered_data:
            logger.debug(f"First timestamp: {from_timestamp(filtered_data[0][0])}")
            logger.debug(f"Last timestamp: {from_timestamp(filtered_data[-1][0])}")

        # Then validate individual records
        validation_errors = []
        for data in filtered_data:
            try:
                integrity_manager = DataIntegrityManager(session)
                if not integrity_manager.validate_kline(data):
                    validation_errors.append(f"Data validation failed for timestamp {data[0]}")
            except Exception as e:
                validation_errors.append(str(e))

        if validation_errors:
            logger.error(f"Validation errors for symbol {symbol_id}: {'; '.join(validation_errors)}")
            raise DataValidationError(f"Validation errors: {'; '.join(validation_errors)}")

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
            logger.debug(f"Inserted batch of {len(batch)} klines for symbol_id {symbol_id}")

    except DataValidationError as ve:
        logger.error(f"Data validation error: {ve}")
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert kline data for symbol_id {symbol_id}: {str(e)}")
        logger.debug(traceback.format_exc())
        raise DatabaseError(f"Failed to insert kline data for symbol_id {symbol_id}: {str(e)}")

def remove_symbol(session: Session, symbol_name: str) -> None:
    """
    Remove a symbol and its associated kline data.

    Args:
        session: Database session
        symbol_name: Symbol name to remove

    Raises:
        DatabaseError: If removal fails
    """
    try:
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            deleted = session.query(Kline).filter_by(symbol_id=symbol.id).delete()
            session.delete(symbol)
            logger.info(f"Removed symbol '{symbol_name}' and its {deleted} associated klines.")
        else:
            logger.warning(f"Symbol not found for removal: {symbol_name}")
    except Exception as e:
        logger.error(f"Failed to remove symbol '{symbol_name}': {e}")
        raise DatabaseError(f"Failed to remove symbol '{symbol_name}': {str(e)}")
