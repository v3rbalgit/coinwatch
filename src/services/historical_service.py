# src/services/historical_service.py

import logging
from datetime import datetime, timezone
from typing import Optional, List, TypedDict
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError
from api.bybit_adapter import BybitAdapter
from services.kline_service import get_symbol_id, insert_kline_data
from utils.timestamp import to_timestamp, from_timestamp
from utils.db_retry import with_db_retry
from models.checkpoint import Checkpoint
from db.init_db import session_scope
import time
import threading

logger = logging.getLogger(__name__)

class InstrumentInfo(TypedDict):
    symbol: str
    launchTime: str

class InstrumentResult(TypedDict):
    list: List[InstrumentInfo]

class InstrumentResponse(TypedDict):
    retCode: int
    result: InstrumentResult

class RateLimiter:
    """
    A simple rate limiter to ensure that API calls do not exceed
    a specified rate limit.
    """
    def __init__(self, max_calls: int, period: float):
        """
        Initialize the RateLimiter.

        Args:
            max_calls (int): Maximum number of calls allowed within the period.
            period (float): Time period in seconds.
        """
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self.calls = []

    def acquire(self):
        """
        Acquire permission to make an API call. If the rate limit is reached,
        this method will block until a call slot becomes available.
        """
        with self.lock:
            current = time.time()
            # Remove timestamps older than the current period
            self.calls = [call for call in self.calls if call > current - self.period]
            if len(self.calls) >= self.max_calls:
                # Calculate sleep time until the oldest call exits the period
                sleep_time = self.period - (current - self.calls[0])
                logger.debug(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
                current = time.time()
                self.calls = [call for call in self.calls if call > current - self.period]
            # Record the current call
            self.calls.append(time.time())

# Define a global rate limiter instance
global_rate_limiter = RateLimiter(max_calls=110, period=1.0)  # Max 120 reqs/sec. Using 110 here for safety

class HistoricalDataManager:
    def __init__(self, bybit_adapter: BybitAdapter, rate_limiter: RateLimiter = global_rate_limiter):
        """
        Initialize the HistoricalDataManager.

        Args:
            bybit_adapter (BybitAdapter): Adapter to interact with Bybit API.
            rate_limiter (RateLimiter, optional): Shared rate limiter instance.
        """
        self.bybit = bybit_adapter
        self.batch_size = 200  # Bybit's max limit
        self.rate_limit_pause = 0.05  # 50ms between requests
        self.rate_limiter = rate_limiter
        self.max_retries = 5
        self.backoff_factor = 2

    def _make_api_call(self, *args, **kwargs):
        """
        Make an API call with rate limiting and retry logic.

        Args:
            *args: Positional arguments for the API call.
            **kwargs: Keyword arguments for the API call.

        Returns:
            dict: The API response.

        Raises:
            Exception: If all retry attempts fail.
        """
        retries = 0
        while retries < self.max_retries:
            try:
                self.rate_limiter.acquire()
                response = self.bybit.get_kline(*args, **kwargs)
                return response
            except Exception as e:
                retries += 1
                sleep_time = self.backoff_factor ** retries
                logger.warning(f"API call failed on attempt {retries}/{self.max_retries}: {e}. Retrying in {sleep_time} seconds.")
                time.sleep(sleep_time)
        logger.error(f"API call failed after {self.max_retries} attempts.")
        raise Exception(f"Failed to make API call after {self.max_retries} retries.")

    @with_db_retry(max_attempts=3)
    def _get_symbol_first_trade(self, symbol: str) -> Optional[int]:
        """
        Get the timestamp of the first trade for a symbol.

        Args:
            session (Session): Database session.
            symbol (str): Symbol name.

        Returns:
            Optional[int]: Timestamp in milliseconds or None if not found.
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
                        return int(item['launchTime']) * 1000

            logger.warning(f"No launch time found for symbol {symbol}")
            return None

        except Exception as e:
            logger.error(f"Error getting first trade date for {symbol}: {e}")
            return None

    @with_db_retry(max_attempts=3)
    def _update_checkpoint(self, session: Session, symbol: str, last_timestamp: int) -> None:
        """
        Update the checkpoint for a given symbol with proper concurrency handling.

        Args:
            session (Session): Database session
            symbol (str): Symbol name
            last_timestamp (int): Last timestamp processed
        """
        try:
            # Try to update existing checkpoint
            rows_updated = session.query(Checkpoint)\
                .filter_by(symbol=symbol)\
                .update({'last_timestamp': last_timestamp})

            if rows_updated == 0:
                # If no existing checkpoint, try to create one
                checkpoint = Checkpoint(symbol=symbol, last_timestamp=last_timestamp)
                try:
                    session.add(checkpoint)
                    session.flush()  # Try to insert immediately
                except IntegrityError:
                    # If another thread created the checkpoint, update it
                    session.rollback()
                    session.query(Checkpoint)\
                        .filter_by(symbol=symbol)\
                        .update({'last_timestamp': last_timestamp})

            session.commit()
            logger.debug(f"Updated checkpoint for symbol '{symbol}' to {last_timestamp}")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update checkpoint for symbol '{symbol}': {e}")
            raise

    def _process_time_chunk(self, session: Session, symbol: str, symbol_id: int,
                          start_time: int, end_time: int) -> bool:
        """Process a single time chunk with proper error handling."""
        try:
            raw_data = self._make_api_call(
                symbol=symbol,
                interval='5',
                start_time=start_time,
                end_time=end_time,
                limit=self.batch_size
            )

            if raw_data["retCode"] == 0 and raw_data["result"]["list"]:
                kline_list = raw_data["result"]["list"]
                formatted_data = [
                    (int(item[0]), float(item[1]), float(item[2]), float(item[3]),
                     float(item[4]), float(item[5]), float(item[6]))
                    for item in kline_list
                ]

                insert_kline_data(session, symbol_id, formatted_data)
                last_timestamp = max(item[0] for item in formatted_data)
                self._update_checkpoint(session, symbol, last_timestamp)

                return True
            else:
                logger.warning(f"No data returned for {symbol} in time range")
                return False

        except Exception as e:
            logger.error(f"Error processing time chunk: {e}")
            raise


    def fetch_complete_history(self, symbol: str) -> bool:
        """
        Fetch complete historical kline data for a given symbol.

        Args:
            symbol (str): Symbol name.

        Returns:
            bool: True if fetching is successful, False otherwise.
        """
        try:
            with session_scope() as session:
                symbol_id = get_symbol_id(session, symbol)

                # Use SELECT FOR UPDATE with a short timeout
                checkpoint = None
                try:
                    checkpoint = (
                        session.query(Checkpoint)
                        .filter_by(symbol=symbol)
                        .with_for_update(skip_locked=True)  # Allow concurrent processing of different symbols
                        .one_or_none()
                    )
                except OperationalError as e:
                    logger.error(f"Failed to acquire lock for {symbol}: {e}")
                    return False

                start_time: Optional[int] = checkpoint.last_timestamp if checkpoint else None

                if start_time is None:
                    start_time = self._get_symbol_first_trade(symbol)
                    if start_time is None:
                        logger.error(f"Could not determine start time for {symbol}")
                        return False

                # Process data in smaller chunks with savepoints
                chunk_size = 200  # Number of klines per chunk
                end_time = to_timestamp(datetime.now(timezone.utc))
                current_start = start_time

                while current_start < end_time:
                    try:
                        with session.begin_nested():  # Create savepoint
                            # Process chunk
                            current_end = min(current_start + (chunk_size * 5 * 60 * 1000), end_time)
                            success = self._process_time_chunk(session, symbol, symbol_id, current_start, current_end)

                            if not success:
                                break

                            current_start = current_end + 1

                    except Exception as e:
                        logger.error(f"Error processing chunk for {symbol}: {e}")
                        if isinstance(e, IntegrityError):
                            session.rollback()  # Rollback to last savepoint
                            continue
                        raise

                return True

        except Exception as e:
            logger.error(f"Error in historical data collection for {symbol}: {e}")
            return False
