# src/main.py

import traceback
import random
import threading
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

# Database imports
from src.db.init_db import init_db, db_manager, session_scope
from src.db.partition_manager import PartitionManager

# API and services
from src.api.bybit_adapter import BybitAdapter
from src.services.kline_service import (
    get_symbol_id,
    get_latest_timestamp,
    insert_kline_data,
    remove_symbol
)
from src.services.historical_service import HistoricalDataManager
from src.services.partition_maintenance_service import PartitionMaintenanceService
from src.services.database_monitor_service import DatabaseMonitorService

# Utilities
from src.utils.timestamp import get_current_timestamp, from_timestamp
from src.utils.db_resource_manager import DatabaseResourceManager
from src.utils.db_retry import with_db_retry
from src.utils.logger import LoggerSetup

# Models
from src.models.symbol import Symbol

# Configure logging
logger = LoggerSetup.setup(__name__)

def process_symbol(symbol: str, end_time: int) -> None:
    """
    Process a single symbol's data synchronization.

    Args:
        symbol: The trading pair symbol (e.g., 'BTCUSDT')
        end_time: Target end time in milliseconds for data synchronization
    """
    bybit = BybitAdapter()
    try:
        with session_scope() as session:
            # Add small random delay to spread out connections
            sleep(random.uniform(0.1, 0.3))

            try:
                symbol_id = get_symbol_id(session, symbol)
            except Exception as e:
                logger.error(f"Failed to get symbol ID for {symbol}: {e}")
                raise

            sync_symbol_data(session, symbol, symbol_id, end_time, bybit)
    except Exception as e:
        # Critical errors should go to console
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
        bybit: BybitAdapter instance
        batch_size: Number of klines to fetch per request
    """
    try:
        latest_timestamp = get_latest_timestamp(session, symbol_id)
        if latest_timestamp is None:
            logger.warning(f"No historical data found for {symbol}, skipping synchronization")
            return

        if end_time > get_current_timestamp():
            end_time = get_current_timestamp()

        # Console log for sync start
        logger.info(f"Syncing {symbol} from {from_timestamp(latest_timestamp)} to {from_timestamp(end_time)}")

        current_start = latest_timestamp
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

                logger.debug(f"Got {len(kline_list)} klines for {symbol} from API")

                kline_list.sort(key=lambda x: int(x[0]))

                first_ts = int(kline_list[0][0])
                last_ts = int(kline_list[-1][0])
                logger.debug(f"{symbol} - First API timestamp: {from_timestamp(first_ts)}")
                logger.debug(f"{symbol} - Last API timestamp: {from_timestamp(last_ts)}")

                formatted_data = [
                    (int(item[0]), float(item[1]), float(item[2]), float(item[3]),
                     float(item[4]), float(item[5]), float(item[6]))
                    for item in kline_list
                ]

                try:
                    insert_kline_data(session, symbol_id, formatted_data)
                    logger.debug(f"Inserted batch of {len(formatted_data)} klines for {symbol}")

                    if formatted_data:
                        current_start = formatted_data[-1][0] + (5 * 60 * 1000)
                    else:
                        current_start += (5 * 60 * 1000)
                except Exception as e:
                    logger.error(f"Error inserting data for {symbol}: {e}")
                    raise

            else:
                logger.warning(
                    f"No data returned for {symbol} between "
                    f"{from_timestamp(current_start)} and {from_timestamp(current_end)}, "
                    "moving to next time window"
                )
                current_start += (5 * 60 * 1000)

            # Small delay between batches
            sleep(0.05)

    except Exception as e:
        logger.error(f"Error syncing data for {symbol}: {str(e)}")
        logger.debug(traceback.format_exc())
        raise

def cleanup_database(session: Session) -> None:
    """
    Perform database cleanup operations.
    Removes symbols that are no longer active on Bybit.
    """
    bybit = BybitAdapter()
    try:
        # Get current active symbols from Bybit
        current_symbols = set(bybit.get_active_instruments())
        db_symbols = session.query(Symbol.name).all()
        db_symbols = set(symbol[0] for symbol in db_symbols)

        # Remove symbols that are no longer active
        symbols_to_remove = db_symbols - current_symbols
        for symbol_name in symbols_to_remove:
            remove_symbol(session, symbol_name)

        session.commit()
        logger.info("Database cleanup completed")

    except Exception as e:
        logger.error(f"Database cleanup failed: {str(e)}")
        session.rollback()
        raise

def initialize_historical_data(symbols: List[str], bybit_adapter: BybitAdapter) -> None:
    """
    Initialize historical data for provided symbols.
    Fetches complete price history with checkpointing.
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
        logger.error(f"Historical data initialization failed: {e}")
        raise

def start_background_services() -> None:
    """
    Start background services for system maintenance and monitoring.

    Services:
    - Partition maintenance
    - Database monitoring
    - Resource monitoring
    """
    try:
        # Start partition maintenance
        maintenance_service = PartitionMaintenanceService()
        maintenance_thread = threading.Thread(
            target=maintenance_service.run_maintenance_loop,
            daemon=True,
            name="PartitionMaintenance"
        )
        maintenance_thread.start()
        logger.info("Partition maintenance service started")

        # Start database monitoring
        monitor_service = DatabaseMonitorService()
        monitor_thread = threading.Thread(
            target=monitor_service.run_monitoring_loop,
            daemon=True,
            name="DatabaseMonitor"
        )
        monitor_thread.start()
        logger.info("Database monitoring service started")

        # Start resource monitoring if engine is available
        if db_manager.engine:
            db_resource_manager = DatabaseResourceManager(db_manager.engine)
            resource_thread = threading.Thread(
                target=db_resource_manager.start_monitoring,
                daemon=True,
                name="ResourceMonitor"
            )
            resource_thread.start()
            logger.info("Resource monitoring started")

    except Exception as e:
        logger.error(f"Failed to start background services: {e}")
        raise

def main() -> None:
    """
    Main application entry point.

    Flow:
    1. Initialize database and set up partitioning
    2. Start background services
    3. Initialize historical data
    4. Enter main synchronization loop
    """
    try:
        # Initialize database and setup partitioning
        init_db()
        logger.info("Database initialized successfully")

        with session_scope() as session:
            partition_manager = PartitionManager(session)
            partition_manager.setup_partitioning()
            logger.info("Partition setup completed")

        # Start background services
        start_background_services()
        logger.info("All background services started")

        # Create BybitAdapter instance
        bybit = BybitAdapter()

        # Get initial symbols and initialize historical data
        current_symbols = bybit.get_active_instruments(limit=5)
        if not current_symbols:
            logger.error("No symbols fetched from Bybit")
            return

        initialize_historical_data(current_symbols, bybit)
        logger.info("Historical data initialization completed")

        # Create thread pool for symbol processing
        with ThreadPoolExecutor(max_workers=min(10, len(current_symbols))) as executor:
            # Main synchronization loop
            while True:
                try:
                    with session_scope() as session:
                        cleanup_database(session)
                        current_symbols = bybit.get_active_instruments(limit=5)

                        if not current_symbols:
                            logger.warning("No symbols fetched, retrying in 5 minutes")
                            sleep(300)
                            continue

                        end_time = get_current_timestamp()

                        # Process symbols in parallel
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