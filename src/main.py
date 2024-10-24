# src/main.py

import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from db.init_db import init_db, db_manager, session_scope
from api.bybit_adapter import BybitAdapter
from services.kline_service import get_symbol_id, get_latest_timestamp, insert_kline_data, remove_symbol
from services.historical_service import HistoricalDataManager
from services.partition_maintenance_service import PartitionMaintenanceService
from services.database_monitor_service import DatabaseMonitorService
from db.partition_manager import PartitionManager
from utils.timestamp import get_current_timestamp, from_timestamp
from utils.db_resource_manager import DatabaseResourceManager
from utils.db_retry import with_db_retry
from models.symbol import Symbol
import threading
import random

# Set up logging with thread name
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_symbol(symbol: str, end_time: int) -> None:
    """
    Process a single symbol's data synchronization.

    Args:
        symbol: The trading pair symbol (e.g., 'BTCUSDT')
        end_time: Target end time in milliseconds for data synchronization

    Uses thread-local session management to ensure thread safety.
    Adds random delays to prevent API rate limiting.
    """
    bybit = BybitAdapter()  # Create a new instance per thread
    try:
        with session_scope() as session:
            # Add small random delay to spread out connections
            sleep(random.uniform(0.1, 0.3))  # Random delay between 100-300ms

            # Get symbol ID with retry
            try:
                symbol_id = get_symbol_id(session, symbol)
            except Exception as e:
                logger.error(f"Failed to get symbol ID for {symbol}: {e}")
                raise

            # Sync data with retry handled internally
            sync_symbol_data(session, symbol, symbol_id, end_time, bybit)
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")

@with_db_retry(max_attempts=3)
def sync_symbol_data(session: Session, symbol: str, symbol_id: int, end_time: int, bybit: BybitAdapter, batch_size: int = 200) -> None:
    """
    Synchronize data for a single symbol with batch processing.

    Args:
        session: Database session
        symbol: Trading pair symbol
        symbol_id: Database ID for the symbol
        end_time: Target end time in milliseconds
        batch_size: Number of klines to fetch per request (max 200)

    Fetches 5-minute kline data in batches, starting from the most recent stored
    timestamp up to the specified end time. Implements rate limiting and error handling.
    """
    try:
        latest_timestamp = get_latest_timestamp(session, symbol_id)
        if latest_timestamp is None:
            logger.warning(f"No historical data found for {symbol}, skipping synchronization")
            return

        if end_time > get_current_timestamp():
            end_time = get_current_timestamp()

        logger.info(f"Syncing {symbol} from {from_timestamp(latest_timestamp)} to {from_timestamp(end_time)}")

        current_start = latest_timestamp + 1  # Ensure we don't fetch the last timestamp again
        while current_start < end_time:
            current_end = min(current_start + (batch_size * 5 * 60 * 1000), end_time)

            kline_data = bybit.get_kline(
                symbol=symbol,
                interval='5',
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

                # Update current_start based on the last timestamp in the data
                current_start = max(item[0] for item in kline_list) + 1
            else:
                logger.warning(f"No data returned for {symbol} in time range")
                break

            # Small delay between batches to prevent rate limiting
            sleep(0.05)

    except Exception as e:
        logger.error(f"Error syncing data for {symbol}: {str(e)}")
        raise

def cleanup_database(session: Session) -> None:
    """
    Perform database cleanup operations.

    Args:
        session: Database session

    Removes symbols that are no longer active on Bybit.
    Ensures database consistency with current trading pairs.
    """
    bybit = BybitAdapter()  # Create a new instance
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

        session.commit()
        logger.info("Database cleanup completed successfully")

    except Exception as e:
        logger.error(f"Error during database cleanup: {str(e)}")
        session.rollback()
        raise

def initialize_historical_data(symbols: List[str], bybit_adapter: BybitAdapter) -> None:
    """
    Initialize historical data for all provided symbols.

    Args:
        symbols: List of trading pair symbols

    Fetches complete price history for each symbol from their listing date.
    Uses checkpointing to allow resume of interrupted downloads.
    Must be run before starting regular synchronization.
    """
    try:
        historical_manager = HistoricalDataManager(bybit_adapter)

        for symbol in symbols:
            logger.info(f"Fetching complete history for {symbol}")
            if historical_manager.fetch_complete_history(symbol):
                logger.info(f"Successfully fetched complete history for {symbol}")
            else:
                logger.error(f"Failed to fetch complete history for {symbol}")

    except Exception as e:
        logger.error(f"Error in historical data initialization: {e}")
        raise

def start_services() -> List[threading.Thread]:
    """
    Start all background services needed for system operation.

    Returns:
        List of started service threads

    Starts the following services:
    - Partition maintenance: Manages database partitions
    - Database monitoring: Tracks database size and performance
    - Resource monitoring: Monitors database connection pool

    All services are started as daemon threads and include error recovery.
    """
    threads = []

    try:
        # Start partition maintenance
        maintenance_service = PartitionMaintenanceService()
        maintenance_thread = threading.Thread(
            target=maintenance_service.run_maintenance_loop,
            daemon=True,
            name="PartitionMaintenance"
        )
        maintenance_thread.start()
        threads.append(maintenance_thread)
        logger.info("Partition maintenance service started")

        # Start database monitoring
        monitor_service = DatabaseMonitorService()
        monitor_thread = threading.Thread(
            target=monitor_service.run_monitoring_loop,
            daemon=True,
            name="DatabaseMonitor"
        )
        monitor_thread.start()
        threads.append(monitor_thread)
        logger.info("Database monitoring service started")

        # Start resource monitoring
        if db_manager.engine:
            db_resource_manager = DatabaseResourceManager(db_manager.engine)
            resource_thread = threading.Thread(
                target=db_resource_manager.start_monitoring,
                daemon=True,
                name="ResourceMonitor"
            )
            resource_thread.start()
            threads.append(resource_thread)
            logger.info("Resource monitoring started")

    except Exception as e:
        logger.error(f"Failed to start services: {e}")
        # Try to stop any services that did start
        for thread in threads:
            if hasattr(thread, "stop"):
                thread.stop()

    return threads

def main() -> None:
    """
    Main application entry point.

    Application flow:
    1. Initialize database and set up partitioning
    2. Start background services (monitoring, maintenance)
    3. Initialize historical data for all symbols
    4. Enter main loop for continuous data synchronization:
       - Cleanup inactive symbols
       - Fetch new data for active symbols
       - Process symbols in parallel with rate limiting

    Includes comprehensive error handling and recovery mechanisms.
    Uses connection pooling and thread-safe session management.
    """
    try:
        # Initialize database and setup partitioning
        init_db()
        logger.info("Database initialized successfully")

        with session_scope() as session:
            partition_manager = PartitionManager(session)
            partition_manager.setup_partitioning()
            logger.info("Partition setup completed")

        # Start all background services
        background_threads = start_services()
        logger.info("All background services started")

        # Create a BybitAdapter instance
        bybit = BybitAdapter()

        # Get current active symbols
        current_symbols = bybit.get_active_instruments()
        if not current_symbols:
            logger.error("No symbols fetched from Bybit")
            return

        # Initialize historical data
        initialize_historical_data(current_symbols, bybit)
        logger.info("Historical data initialization completed")

        # Main synchronization loop
        while True:
            try:
                with session_scope() as session:
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
                    max_workers = min(10, len(current_symbols))
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [
                            executor.submit(process_symbol, symbol, end_time)
                            for symbol in current_symbols
                        ]

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