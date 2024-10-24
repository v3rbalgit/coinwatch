# src/services/kline_service.py

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert
from models.symbol import Symbol
from models.kline import Kline
import logging
from typing import Optional, List, Tuple, cast
from datetime import datetime
from utils.integrity import DataIntegrityManager
from utils.exceptions import DataValidationError, DatabaseError
from utils.validation import KlineValidator, ValidationError

logger = logging.getLogger(__name__)

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
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if not symbol:
            symbol = Symbol(name=symbol_name)
            session.add(symbol)
            session.flush()  # Use flush to get the ID without committing
            logger.debug(f"Added new symbol: {symbol_name} with ID {symbol.id}")
        return cast(int, symbol.id)
    except IntegrityError:
        session.rollback()
        # Handle race condition by querying again
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            return cast(int, symbol.id)
        raise DatabaseError(f"Failed to get or create symbol: {symbol_name}")
    except Exception as e:
        session.rollback()
        raise DatabaseError(f"Error processing symbol: {symbol_name}: {str(e)}")

def get_latest_timestamp(session: Session, symbol_id: int) -> Optional[int]:
    """
    Get the most recent timestamp for a given symbol.

    Args:
        session: Database session
        symbol_id: Symbol ID

    Returns:
        Optional[int]: Latest timestamp or None if no data exists

    Raises:
        DatabaseError: If query fails
    """
    try:
        result = session.query(Kline.start_time)\
            .filter_by(symbol_id=symbol_id)\
            .order_by(Kline.start_time.desc())\
            .first()
        return result[0] if result else None
    except Exception as e:
        raise DatabaseError(f"Failed to get latest timestamp for symbol_id {symbol_id}: {str(e)}")

def validate_kline_data(data: Tuple) -> bool:
    """
    Validate a single kline data tuple.

    Args:
        data: Tuple of (timestamp, open, high, low, close, volume, turnover)

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        timestamp, open_price, high, low, close, volume, turnover = data
        current_time = int(datetime.now().timestamp() * 1000)

        return all([
            isinstance(timestamp, int),
            timestamp > 0,
            timestamp <= current_time,
            all(isinstance(x, (int, float)) for x in [open_price, high, low, close, volume, turnover]),
            low <= high,
            low <= open_price <= high,
            low <= close <= high,
            volume >= 0,
            turnover >= 0
        ])
    except Exception:
        return False

def insert_kline_data(session: Session, symbol_id: int, kline_data: List[Tuple], batch_size: int = 1000) -> None:
    """
    Insert kline data with integrity checks.

    Args:
        session: Database session
        symbol_id: Symbol ID
        kline_data: List of kline data tuples
        batch_size: Size of each insert batch

    Raises:
        DataValidationError: If data validation fails
        DatabaseError: If insert fails
    """
    try:
        # Basic validation first
        validation_errors = KlineValidator.validate_klines(kline_data)
        if validation_errors:
            error_indices = [i for i, _ in validation_errors]
            error_messages = [msg for _, msg in validation_errors]
            raise ValidationError(
                f"Invalid kline data found at indices: {error_indices}\n"
                f"Errors: {error_messages}"
            )

        # Integrity checks
        if kline_data:
            integrity_manager = DataIntegrityManager(session)
            start_time = min(data[0] for data in kline_data)
            end_time = max(data[0] for data in kline_data)

            validation_result = integrity_manager.validate_kline_data(
                symbol_id, start_time, end_time
            )

            if not validation_result.get('time_range_valid', False):
                raise DataValidationError("Invalid time range in kline data")

        # Process in batches
        for i in range(0, len(kline_data), batch_size):
            batch = kline_data[i:i + batch_size]
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
            logger.debug(f"Inserted batch of {len(batch)} klines for symbol_id {symbol_id}")

    except ValidationError as ve:
        logger.error(f"Data validation error: {ve}")
        raise
    except DataValidationError as dve:
        logger.error(f"Data validation error: {dve}")
        raise
    except Exception as e:
        logger.error(f"Failed to insert kline data for symbol_id {symbol_id}: {str(e)}")
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
