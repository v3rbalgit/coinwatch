from datetime import datetime, timezone
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.api.bybit_adapter import BybitAdapter
from src.services.kline_service import get_symbol_id, insert_kline_data
from src.utils.timestamp import to_timestamp
from src.utils.db_retry import with_db_retry
from src.utils.exceptions import APIError
from src.models.checkpoint import Checkpoint
from src.models.symbol import Symbol
from src.models.kline import Kline
from src.db.init_db import session_scope
from src.utils.logger import LoggerSetup
import time

logger = LoggerSetup.setup(__name__)

class HistoricalDataManager:
    """
    Manages the collection and storage of historical price data from Bybit.

    This class provides functionality for:
    - Fetching complete historical data for trading pairs
    - Processing data in optimized batches
    - Storing data with data integrity checks
    - Managing checkpoints for resume capability
    - Handling rate limits and retries

    Implementation details:
    - Uses BybitAdapter for API interactions
    - Implements exponential backoff for retries
    - Maintains checkpoints for interrupted downloads
    - Handles data validation and integrity
    - Provides transaction safety with nested sessions
    """

    def __init__(self, bybit_adapter: BybitAdapter):
        """
        Initialize the HistoricalDataManager.

        Args:
            bybit_adapter: Configured BybitAdapter instance for API interactions

        Note:
            batch_size is set to 1000 (maximum allowed by Bybit API)
            max_retries and backoff_factor control retry behavior
        """
        self.bybit = bybit_adapter
        self.batch_size = 1000  # Maximum allowed by Bybit API
        self.max_retries = 5
        self.backoff_factor = 2
        logger.debug("Historical data manager initialized")

    def _make_api_call(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Make an API call with retry logic and exponential backoff.

        Implements retry mechanism for handling transient API failures:
        - Retries up to max_retries times
        - Uses exponential backoff between retries
        - Validates API response codes

        Args:
            *args: Positional arguments for get_kline API call
            **kwargs: Keyword arguments for get_kline API call

        Returns:
            Dict[str, Any]: Validated API response

        Raises:
            APIError: If API returns error response
            Exception: If all retry attempts fail
        """
        retries = 0
        while retries < self.max_retries:
            try:
                response = self.bybit.get_kline(*args, **kwargs)

                if response.get("retCode") == 0:
                    return response

                raise APIError(f"API Error: {response.get('retCode')} - {response.get('retMsg')}")

            except Exception as e:
                retries += 1
                if retries < self.max_retries:
                    sleep_time = self.backoff_factor ** retries
                    logger.warning(
                        f"API call failed (attempt {retries}/{self.max_retries}). "
                        f"Retrying in {sleep_time}s. Error: {str(e)}"
                    )
                    time.sleep(sleep_time)
                    continue
                raise

        raise Exception(f"API call failed after {self.max_retries} attempts")

    @with_db_retry(max_attempts=3)
    def _get_symbol_first_trade(self, symbol: str) -> Optional[int]:
        """
        Get the timestamp of symbol's first trade from Bybit.

        Makes API request to get instrument information and extracts
        the launch time. Includes validation of the timestamp.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')

        Returns:
            Optional[int]: Launch timestamp in milliseconds, or None if:
                        - No launch time found
                        - Invalid launch time format
                        - Launch time outside valid range
        """
        try:
            raw_response = self.bybit.session.get_instruments_info(
                category='linear',
                symbol=symbol
            )

            if isinstance(raw_response, dict) and raw_response.get('retCode') == 0:
                instrument_list = raw_response.get('result', {}).get('list', [])
                for item in instrument_list:
                    if item.get('symbol') == symbol and 'launchTime' in item:
                        launch_time = item['launchTime']
                        if isinstance(launch_time, (int, str)) and str(launch_time).isdigit():
                            ts = int(float(launch_time))
                            # API returns milliseconds, only convert to seconds for validation
                            ts_seconds = ts // 1000
                            if 1500000000 < ts_seconds < int(time.time()):
                                logger.debug(f"Found valid launch time for {symbol}")
                                return ts
                            else:
                                logger.warning(f"Invalid launch time for {symbol}: {launch_time}")
                                return None

            logger.warning(f"No valid launch time found for {symbol}")
            return None

        except Exception as e:
            logger.error(f"Error getting first trade date for {symbol}: {e}")
            return None

    def _process_time_chunk(self, session: Session, symbol: str, symbol_id: int,
                          start_time: int, end_time: int) -> bool:
        """
        Process a chunk of historical data for a symbol.

        Handles the complete workflow for a time chunk:
        1. Fetches data from Bybit API
        2. Validates and sorts the data
        3. Formats data for database insertion
        4. Inserts data and updates checkpoint

        Args:
            session: Active database session
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            symbol_id: Database ID for the symbol
            start_time: Chunk start time in milliseconds
            end_time: Chunk end time in milliseconds

        Returns:
            bool: True if chunk was processed successfully, False if:
                 - No data available for time range
                 - API error occurred
                 - Data validation failed

        Raises:
            Exception: If data processing fails
        """
        try:
            logger.debug(
                f"Processing chunk for {symbol} from "
                f"{datetime.fromtimestamp(start_time/1000, tz=timezone.utc)} to "
                f"{datetime.fromtimestamp(end_time/1000, tz=timezone.utc)}"
            )

            raw_data = self._make_api_call(
                symbol=symbol,
                interval='5',
                category='linear',
                start_time=start_time,
                end_time=end_time,
                limit=self.batch_size
            )

            if raw_data.get("retCode") == 0:
                kline_list = raw_data.get("result", {}).get("list", [])
                if kline_list:
                    # Sort by timestamp ascending before processing
                    kline_list.sort(key=lambda x: int(x[0]))
                    logger.info(f"Processing {len(kline_list)} klines for {symbol}")

                    formatted_data = [
                        (int(item[0]), float(item[1]), float(item[2]), float(item[3]),
                         float(item[4]), float(item[5]), float(item[6]))
                        for item in kline_list
                    ]

                    insert_kline_data(session, symbol_id, formatted_data)
                    last_timestamp = formatted_data[-1][0]
                    self._update_checkpoint(session, symbol, last_timestamp)
                    return True

                logger.debug(f"No data available for {symbol} in requested time range")
                return False

            logger.warning(f"API error for {symbol}: {raw_data.get('retMsg')}")
            return False

        except Exception as e:
            logger.error(f"Failed to process chunk for {symbol}: {e}", exc_info=True)
            raise

    @with_db_retry(max_attempts=3)
    def _update_checkpoint(self, session: Session, symbol: str, last_timestamp: int) -> None:
        """
        Update or create a checkpoint for symbol's data collection progress.

        Handles both update and insert scenarios with proper error handling
        and transaction management. Uses upsert pattern to handle race conditions.

        Args:
            session: Active database session
            symbol: Trading pair symbol
            last_timestamp: Last successfully processed timestamp in milliseconds

        Raises:
            Exception: If checkpoint update fails after retries
        """
        try:
            rows_updated = session.query(Checkpoint)\
                .filter_by(symbol=symbol)\
                .update({'last_timestamp': last_timestamp})

            if rows_updated == 0:
                checkpoint = Checkpoint(symbol=symbol, last_timestamp=last_timestamp)
                try:
                    session.add(checkpoint)
                    session.flush()
                except IntegrityError:
                    session.rollback()
                    session.query(Checkpoint)\
                        .filter_by(symbol=symbol)\
                        .update({'last_timestamp': last_timestamp})

            session.commit()
            logger.debug(
                f"Updated checkpoint for {symbol} to "
                f"{datetime.fromtimestamp(last_timestamp/1000, tz=timezone.utc)}"
            )

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update checkpoint for {symbol}: {e}", exc_info=True)
            raise

    def fetch_complete_history(self, symbol: str) -> bool:
        """
        Fetch complete historical data for a trading pair.

        Performs the complete historical data collection process:
        1. Validates symbol existence
        2. Determines symbol's first trade time
        3. Fetches data in chunks from first trade to current time
        4. Handles data storage and checkpointing
        5. Provides progress tracking and error recovery

        The process is chunked to:
        - Manage memory usage
        - Enable partial progress tracking
        - Allow for recovery from failures
        - Stay within API rate limits

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')

        Returns:
            bool: True if complete history was fetched successfully,
                 False if:
                 - Symbol doesn't exist in database
                 - Cannot determine first trade time
                 - Critical error occurs during fetching

        Note:
            Uses nested transactions for chunk processing to ensure
            data consistency while allowing for partial progress
        """
        try:
            with session_scope() as session:
                symbol_id = get_symbol_id(session, symbol)

                if not session.query(Symbol).filter_by(id=symbol_id).first():
                    logger.error(f"Symbol {symbol} not found in database")
                    return False

                start_time = self._get_symbol_first_trade(symbol)
                if start_time is None:
                    logger.error(f"Could not determine first trade time for {symbol}")
                    return False

                end_time = to_timestamp(datetime.now(timezone.utc))
                current_start = start_time

                logger.info(
                    f"Starting historical data collection for {symbol} from "
                    f"{datetime.fromtimestamp(start_time/1000, tz=timezone.utc)}"
                )

                while current_start < end_time:
                    try:
                        with session.begin_nested():
                            current_end = min(
                                current_start + (self.batch_size * 5 * 60 * 1000),
                                end_time
                            )
                            success = self._process_time_chunk(
                                session, symbol, symbol_id, current_start, current_end
                            )

                            if not success:
                                break

                            current_start = current_end + 1

                    except Exception as e:
                        logger.error(f"Error processing chunk for {symbol}: {e}")
                        if isinstance(e, IntegrityError):
                            session.rollback()
                            continue
                        raise

                records = session.query(Kline).filter_by(symbol_id=symbol_id).count()
                logger.info(
                    f"Historical data collection completed for {symbol}. "
                    f"Total records: {records:,}"
                )
                return True

        except Exception as e:
            logger.error(f"Historical data collection failed for {symbol}: {e}", exc_info=True)
            return False