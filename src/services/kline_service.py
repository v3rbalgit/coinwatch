from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.symbol import Symbol
from models.kline import Kline
import logging
from typing import Optional, List, Tuple, cast

logger = logging.getLogger(__name__)

def get_symbol_id(session: Session, symbol_name: str) -> int:
    """
    Get the database ID for a given symbol, creating a new entry if it doesn't exist.

    Args:
        session (Session): The database session.
        symbol_name (str): The name of the symbol.

    Returns:
        int: The database ID of the symbol.

    Raises:
        ValueError: If unable to get or create the symbol after a retry.
        RuntimeError: If a database error occurs or for any unexpected errors.
    """
    try:
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if not symbol:
            symbol = Symbol(name=symbol_name)
            session.add(symbol)
            session.commit()
            session.refresh(symbol)
        return cast(int, symbol.id)
    except IntegrityError:
        session.rollback()
        # In case of a race condition, try to get the symbol again
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            return cast(int, symbol.id)
        else:
            raise ValueError(f"Failed to get or create symbol: {symbol_name}")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Database error in get_symbol_id for {symbol_name}: {str(e)}")
        raise RuntimeError(f"Database error occurred while getting/creating symbol: {symbol_name}")
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error in get_symbol_id for {symbol_name}: {str(e)}")
        raise RuntimeError(f"Unexpected error occurred while getting/creating symbol: {symbol_name}")

def get_latest_timestamp(session: Session, symbol_id: int) -> Optional[int]:
    """
    Get the most recent timestamp for a given symbol from the database.

    Args:
        session (Session): The database session.
        symbol_id (int): The database ID of the symbol.

    Returns:
        Optional[int]: The latest timestamp for the symbol, or None if no data exists.

    Raises:
        RuntimeError: If a database error occurs or for any unexpected errors.
    """
    try:
        result = session.query(Kline.start_time).filter_by(symbol_id=symbol_id).order_by(Kline.start_time.desc()).first()
        return cast(int, result[0]) if result else None
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_latest_timestamp for symbol_id {symbol_id}: {str(e)}")
        raise RuntimeError(f"Database error occurred while getting latest timestamp for symbol_id: {symbol_id}")
    except Exception as e:
        logger.error(f"Unexpected error in get_latest_timestamp for symbol_id {symbol_id}: {str(e)}")
        raise RuntimeError(f"Unexpected error occurred while getting latest timestamp for symbol_id: {symbol_id}")

def insert_kline_data(session: Session, symbol_id: int, kline_data: List[Tuple]):
    """
    Insert kline data for a symbol into the database.

    Args:
        session (Session): The database session.
        symbol_id (int): The database ID of the symbol.
        kline_data (List[Tuple]): A list of tuples containing kline data.
                                  Each tuple should contain:
                                  (timestamp, open, high, low, close, volume, turnover)

    Raises:
        ValueError: If there's a data integrity error (e.g., duplicate data).
        RuntimeError: If a database error occurs or for any unexpected errors.
    """
    try:
        for data in kline_data:
            kline_entry = Kline(
                symbol_id=symbol_id,
                start_time=data[0],
                open_price=data[1],
                high_price=data[2],
                low_price=data[3],
                close_price=data[4],
                volume=data[5],
                turnover=data[6]
            )
            session.merge(kline_entry)  # Use merge to handle duplicates
        session.commit()
        logger.info(f"Successfully inserted {len(kline_data)} klines for symbol_id {symbol_id}")
    except IntegrityError as e:
        session.rollback()
        logger.error(f"IntegrityError while inserting kline data for symbol_id {symbol_id}: {str(e)}")
        raise ValueError(f"Data integrity error for symbol_id {symbol_id}. Possible duplicate or constraint violation.")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"SQLAlchemyError while inserting kline data for symbol_id {symbol_id}: {str(e)}")
        raise RuntimeError(f"Database error occurred while inserting data for symbol_id {symbol_id}")
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error inserting kline data for symbol_id {symbol_id}: {str(e)}")
        raise RuntimeError(f"Unexpected error occurred while inserting data for symbol_id {symbol_id}")

def remove_symbol(session: Session, symbol_name: str):
    """
    Remove a symbol and its associated kline data from the database.

    Args:
        session (Session): The database session.
        symbol_name (str): The name of the symbol to remove.

    Raises:
        RuntimeError: If a database error occurs or for any unexpected errors.
    """
    try:
        symbol = session.query(Symbol).filter_by(name=symbol_name).first()
        if symbol:
            session.query(Kline).filter_by(symbol_id=symbol.id).delete()
            session.delete(symbol)
            session.commit()
            logger.info(f"Removed symbol and its klines: {symbol_name}")
        else:
            logger.warning(f"Symbol not found for removal: {symbol_name}")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Database error in remove_symbol for {symbol_name}: {str(e)}")
        raise RuntimeError(f"Database error occurred while removing symbol: {symbol_name}")
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error in remove_symbol for {symbol_name}: {str(e)}")
        raise RuntimeError(f"Unexpected error occurred while removing symbol: {symbol_name}")