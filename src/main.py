import logging
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from db.init_db import init_db, engine
from api.bybit_adapter import get_kline, get_instruments
from services.kline_service import get_symbol_id, get_latest_timestamp, insert_kline_data, remove_symbol
from config import DATABASE_URL, SYNCHRONIZATION_DAYS
from utils.timestamp import get_current_timestamp, get_past_timestamp, calculate_hours_between, from_timestamp
from models.symbol import Symbol
from models.kline import Kline

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def cleanup_database(session):
    """
    Perform database cleanup operations.

    This function removes symbols no longer present in the current list,
    deletes duplicate klines, and removes data older than SYNCHRONIZATION_DAYS.

    Args:
        session (Session): The database session to use for operations.

    Raises:
        Exception: If any error occurs during the cleanup process.
    """
    try:
        # 1. Remove symbols not present in the current list
        current_symbols = set(get_instruments())
        db_symbols = set(symbol.name for symbol in session.query(Symbol).all())
        symbols_to_remove = db_symbols - current_symbols

        for symbol_name in symbols_to_remove:
            remove_symbol(session, symbol_name)

        # 2. Remove duplicate klines
        session.execute(text("""
            DELETE k1 FROM kline_data k1
            INNER JOIN kline_data k2
            WHERE k1.id > k2.id
            AND k1.symbol_id = k2.symbol_id
            AND k1.start_time = k2.start_time
        """))
        logger.info("Removed duplicate klines")

        # 3. Remove data older than SYNCHRONIZATION_DAYS
        cutoff_timestamp = get_past_timestamp(SYNCHRONIZATION_DAYS)
        deleted_rows = session.query(Kline).filter(Kline.start_time < cutoff_timestamp).delete()
        logger.info(f"Removed {deleted_rows} kline entries older than {SYNCHRONIZATION_DAYS} days")

        session.commit()
        logger.info("Database cleanup completed successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error during database cleanup: {str(e)}")

def fill_data_gaps(session, symbol, symbol_id, start_time, end_time):
    """
    Fill gaps in kline data for a specific symbol.

    This function identifies missing data points between start_time and end_time,
    and attempts to fetch and insert the missing data.

    Args:
        session (Session): The database session to use for operations.
        symbol (str): The trading pair symbol.
        symbol_id (int): The database ID of the symbol.
        start_time (int): The start timestamp for gap filling.
        end_time (int): The end timestamp for gap filling.

    Raises:
        Exception: If any error occurs during the gap filling process.
    """
    try:
        current_time = get_current_timestamp()
        if end_time > current_time:
            logger.warning(f"End time {end_time} is in the future. Adjusting to current time.")
            end_time = current_time

        logger.info(f"Filling data gaps for {symbol} from {from_timestamp(start_time)} to {from_timestamp(end_time)}")

        # Fetch existing data points
        existing_data = session.query(Kline.start_time).filter(
            Kline.symbol_id == symbol_id,
            Kline.start_time >= start_time,
            Kline.start_time <= end_time
        ).all()
        existing_timestamps = set(data[0] for data in existing_data)

        # Calculate missing timestamps
        all_timestamps = set(range(start_time, end_time + 3600000, 3600000))  # Hourly intervals
        missing_timestamps = all_timestamps - existing_timestamps

        if missing_timestamps:
            logger.info(f"Found {len(missing_timestamps)} data gaps for {symbol}")
            for timestamp in missing_timestamps:
                if timestamp > current_time:
                    logger.warning(f"Skipping future timestamp {timestamp} for {symbol}")
                    continue
                kline_data = get_kline(symbol, limit=1, start_time=timestamp, end_time=timestamp + 3600000)
                if kline_data["retCode"] == 0 and kline_data["result"]["list"]:
                    item = kline_data["result"]["list"][0]
                    formatted_data = [(int(item[0]), float(item[1]), float(item[2]), float(item[3]), float(item[4]), float(item[5]), float(item[6]))]
                    insert_kline_data(session, symbol_id, formatted_data)
                else:
                    logger.warning(f"Failed to fetch data for {symbol} at timestamp {timestamp} ({from_timestamp(timestamp)})")

        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error filling data gaps for {symbol}: {str(e)}")

def sync_symbol_data(session, symbol, symbol_id, end_time):
    """
    Synchronize kline data for a specific symbol.

    This function fills any gaps in existing data and fetches new data up to the specified end_time.

    Args:
        session (Session): The database session to use for operations.
        symbol (str): The trading pair symbol.
        symbol_id (int): The database ID of the symbol.
        end_time (int): The end timestamp for data synchronization.

    Raises:
        Exception: If any error occurs during the synchronization process.
    """
    try:
        current_time = get_current_timestamp()
        if end_time > current_time:
            logger.warning(f"End time {end_time} is in the future. Adjusting to current time.")
            end_time = current_time

        latest_timestamp = get_latest_timestamp(session, symbol_id)
        start_time = latest_timestamp if latest_timestamp else get_past_timestamp(SYNCHRONIZATION_DAYS)

        logger.info(f"Syncing data for {symbol} from {from_timestamp(start_time)} to {from_timestamp(end_time)}")

        # Fill any gaps in existing data
        fill_data_gaps(session, symbol, symbol_id, start_time, end_time)

        # Fetch new data
        hours_between = calculate_hours_between(start_time, end_time)
        if hours_between > 0:
            kline_data = get_kline(symbol, limit=hours_between)
            if kline_data["retCode"] == 0:
                kline_list = kline_data["result"]["list"]
                formatted_data = [
                    (int(item[0]), float(item[1]), float(item[2]), float(item[3]), float(item[4]), float(item[5]), float(item[6]))
                    for item in kline_list if int(item[0]) <= current_time
                ]
                insert_kline_data(session, symbol_id, formatted_data)
                logger.info(f"Inserted {len(formatted_data)} new klines for {symbol}")
            else:
                logger.error(f"Error fetching kline data for {symbol}: {kline_data['retMsg']}")
    except Exception as e:
        logger.error(f"Error syncing data for {symbol}: {str(e)}")

def get_current_symbols(max_retries=3, retry_delay=60):
    """
    Fetch the current list of trading pair symbols from the exchange.

    This function attempts to fetch the list of instruments multiple times in case of failure.

    Args:
        max_retries (int): The maximum number of retry attempts. Defaults to 3.
        retry_delay (int): The delay in seconds between retry attempts. Defaults to 60.

    Returns:
        List[str]: A list of current trading pair symbols.

    Raises:
        RuntimeError: If unable to fetch the instrument list after all retry attempts.
    """
    for attempt in range(max_retries):
        try:
            return get_instruments()
        except RuntimeError as e:
            logger.error(f"Error fetching instruments (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
            else:
                logger.critical("Failed to fetch instruments after maximum retries.")
                return []

def main() -> None:
    """
    Main function to run the data synchronization process.

    This function initializes the database, performs cleanup, and continuously
    synchronizes data for all current trading pairs.

    Raises:
        Exception: If a fatal error occurs during execution.
    """
    try:
        init_db()
        logger.info("Database initialized successfully.")

        while True:
            try:
                with Session() as session:
                    # Cleanup and integrity check
                    cleanup_database(session)

                    current_symbols = get_current_symbols()
                    if not current_symbols:
                        logger.warning("No symbols fetched. Skipping this synchronization cycle.")
                        sleep(300)  # Sleep for 5 minutes before retrying
                        continue

                    end_time = get_current_timestamp()

                    for symbol in current_symbols:
                        try:
                            symbol_id = get_symbol_id(session, symbol)
                            sync_symbol_data(session, symbol, symbol_id, end_time)
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {str(e)}")

                    logger.info("Synchronization cycle completed.")
                    logger.info("Sleeping for 5 minutes before next data fetch.")
                    sleep(300)  # Sleep for 5 minutes

            except SQLAlchemyError as e:
                logger.error(f"Database error in main loop: {str(e)}")
                logger.info("Retrying in 5 minutes...")
                sleep(300)
            except Exception as e:
                logger.critical(f"Critical error in main loop: {str(e)}")
                logger.info("Retrying in 10 minutes...")
                sleep(600)

    except Exception as e:
        logger.critical(f"Fatal error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()