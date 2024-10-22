# src/main.py
import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from db.init_db import init_db, db_manager
from api.bybit_adapter import bybit
from services.kline_service import get_symbol_id, get_latest_timestamp, insert_kline_data, remove_symbol
from config import SYNCHRONIZATION_DAYS
from utils.timestamp import get_current_timestamp, get_past_timestamp, calculate_hours_between, from_timestamp
from models.symbol import Symbol
from models.kline import Kline
import threading
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Thread-local storage for database sessions
thread_local = threading.local()

def get_thread_session():
    if not hasattr(thread_local, "session"):
        if not db_manager.SessionLocal:
            raise RuntimeError("Database session factory not initialized")
        thread_local.session = db_manager.SessionLocal()
    return thread_local.session

def process_symbol(symbol: str, end_time: int) -> None:
    """Process a single symbol's data synchronization."""
    session = get_thread_session()
    try:
        # Add small random delay to spread out connections
        sleep(random.uniform(0.1, 0.3))  # Random delay between 100-300ms
        symbol_id = get_symbol_id(session, symbol)
        sync_symbol_data(session, symbol, symbol_id, end_time)
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
    finally:
        session.close()

def sync_symbol_data(session, symbol: str, symbol_id: int, end_time: int, batch_size: int = 200) -> None:
    """
    Synchronize data for a single symbol with batch processing.
    """
    try:
        latest_timestamp = get_latest_timestamp(session, symbol_id)
        start_time = latest_timestamp if latest_timestamp else get_past_timestamp(SYNCHRONIZATION_DAYS)

        if end_time > get_current_timestamp():
            end_time = get_current_timestamp()

        logger.info(f"Syncing {symbol} from {from_timestamp(start_time)} to {from_timestamp(end_time)}")

        current_start = start_time
        while current_start < end_time:
            current_end = min(current_start + (batch_size * 5 * 60 * 1000), end_time)  # 5 minutes * batch_size

            kline_data = bybit.get_kline(
                symbol=symbol,
                interval='5',  # 5 minute intervals
                start_time=current_start,
                end_time=current_end,
                limit=batch_size
            )

            if kline_data["retCode"] == 0 and kline_data["result"]["list"]:
                kline_list = kline_data["result"]["list"]
                formatted_data = [
                    (int(item[0]), float(item[1]), float(item[2]), float(item[3]),
                     float(item[4]), float(item[5]), float(item[6]))
                    for item in kline_list
                ]

                insert_kline_data(session, symbol_id, formatted_data)
                logger.debug(f"Inserted batch of {len(formatted_data)} klines for {symbol}")

                current_start = current_end + 1
            else:
                logger.warning(f"No data returned for {symbol} in time range")
                break

            # Small delay between batches to prevent rate limiting
            sleep(0.1)

    except Exception as e:
        logger.error(f"Error syncing data for {symbol}: {str(e)}")
        raise

def cleanup_database(session) -> None:
    """Perform database cleanup operations."""
    try:
        # Get current active symbols from Bybit
        current_symbols = set(bybit.get_active_instruments())

        # Get symbols from database
        db_symbols = session.query(Symbol.name).all()
        db_symbols = set(symbol[0] for symbol in db_symbols)

        # Remove symbols that are no longer active
        symbols_to_remove = db_symbols - current_symbols
        for symbol_name in symbols_to_remove:
            remove_symbol(session, symbol_name)

        # Remove old data
        cutoff_timestamp = get_past_timestamp(SYNCHRONIZATION_DAYS)
        session.query(Kline).filter(Kline.start_time < cutoff_timestamp).delete()

        session.commit()
        logger.info("Database cleanup completed successfully")

    except Exception as e:
        logger.error(f"Error during database cleanup: {str(e)}")
        session.rollback()
        raise

def main() -> None:
    try:
        init_db()
        logger.info("Database initialized successfully")

        while True:
            try:
                with db_manager.get_session() as session:
                    # Cleanup and integrity check
                    cleanup_database(session)

                    # Get current active symbols
                    current_symbols = bybit.get_active_instruments()
                    if not current_symbols:
                        logger.warning("No symbols fetched. Retrying in 5 minutes...")
                        sleep(300)
                        continue

                    end_time = get_current_timestamp()

                    # Process symbols in parallel
                    max_workers = min(10, len(current_symbols))  # Limit concurrent threads
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [
                            executor.submit(process_symbol, symbol, end_time)
                            for symbol in current_symbols
                        ]

                        # Wait for all futures to complete
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Thread execution failed: {str(e)}")

                    logger.info("Synchronization cycle completed")
                    logger.info("Sleeping for 5 minutes before next cycle")
                    sleep(300)

            except SQLAlchemyError as e:
                logger.error(f"Database error in main loop: {str(e)}")
                sleep(300)
            except Exception as e:
                logger.critical(f"Critical error in main loop: {str(e)}")
                sleep(600)

    except Exception as e:
        logger.critical(f"Fatal error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()