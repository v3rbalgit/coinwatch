# src/services/kline_service.py
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.mysql import insert
from src.models.symbol import Symbol
from src.models.kline import Kline
import logging
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class KlineServiceError(Exception):
    """Base exception for kline service errors"""
    pass

def get_symbol_id(session: Session, symbol_name: str) -> int:
    """
    Get the database ID for a given symbol, creating a new entry if it doesn't exist.

    Args:
        session (Session): The database session
        symbol_name (str): The name of the symbol

    Returns:
        int: The database ID of the symbol

    Raises:
        KlineServiceError: If unable to get or create the symbol
    """
    try:
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if not symbol:
            symbol = Symbol(name=symbol_name)
            session.add(symbol)
            session.commit()
            session.refresh(symbol)
        return int(symbol.id)
    except IntegrityError:
        session.rollback()
        # Handle race condition
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            return int(symbol.id)
        raise KlineServiceError(f"Failed to get or create symbol: {symbol_name}")
    except Exception as e:
        session.rollback()
        logger.error(f"Error in get_symbol_id for {symbol_name}: {str(e)}")
        raise KlineServiceError(f"Error processing symbol: {symbol_name}")

def get_latest_timestamp(session: Session, symbol_id: int) -> Optional[int]:
    """
    Get the most recent timestamp for a given symbol.

    Args:
        session (Session): The database session
        symbol_id (int): The symbol ID

    Returns:
        Optional[int]: The latest timestamp or None if no data exists
    """
    try:
        result = session.query(Kline.start_time)\
            .filter_by(symbol_id=symbol_id)\
            .order_by(Kline.start_time.desc())\
            .first()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting latest timestamp for symbol_id {symbol_id}: {str(e)}")
        raise KlineServiceError(f"Failed to get latest timestamp for symbol_id: {symbol_id}")

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
    Insert kline data in batches with validation.

    Args:
        session (Session): The database session
        symbol_id (int): The symbol ID
        kline_data (List[Tuple]): List of kline data tuples
        batch_size (int): Size of each batch for insertion

    Raises:
        KlineServiceError: If data insertion fails
    """
    try:
        # Validate all data first
        invalid_data = [(i, data) for i, data in enumerate(kline_data) if not validate_kline_data(data)]
        if invalid_data:
            invalid_indices = [i for i, _ in invalid_data]
            logger.error(f"Invalid kline data found at indices: {invalid_indices}")
            raise ValueError(f"Invalid kline data found at indices: {invalid_indices}")

        # Process in batches
        for i in range(0, len(kline_data), batch_size):
            batch = kline_data[i:i + batch_size]

            # Prepare batch data
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

            # Use MySQL's INSERT ... ON DUPLICATE KEY UPDATE
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
            session.commit()

            logger.debug(f"Inserted batch of {len(batch)} klines for symbol_id {symbol_id}")

    except ValueError as e:
        session.rollback()
        raise KlineServiceError(str(e))
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting kline data for symbol_id {symbol_id}: {str(e)}")
        raise KlineServiceError(f"Failed to insert kline data for symbol_id: {symbol_id}")

def remove_symbol(session: Session, symbol_name: str) -> None:
    """
    Remove a symbol and its associated kline data.

    Args:
        session (Session): The database session
        symbol_name (str): The symbol name to remove

    Raises:
        KlineServiceError: If symbol removal fails
    """
    try:
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            # Delete associated kline data first
            session.query(Kline).filter_by(symbol_id=symbol.id).delete()
            session.delete(symbol)
            session.commit()
            logger.info(f"Removed symbol and its klines: {symbol_name}")
        else:
            logger.warning(f"Symbol not found for removal: {symbol_name}")
    except Exception as e:
        session.rollback()
        logger.error(f"Error removing symbol {symbol_name}: {str(e)}")
        raise KlineServiceError(f"Failed to remove symbol: {symbol_name}")