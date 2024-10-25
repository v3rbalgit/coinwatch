# src/services/historical_service.py

import logging
from datetime import datetime, timezone
from typing import Optional
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
import time

logger = logging.getLogger(__name__)

class HistoricalDataManager:
    """
    Manages the collection and storage of historical price data from Bybit.

    This class handles fetching complete historical data for trading pairs,
    processes it in batches, and stores it in the database with proper
    checkpointing for resume capability.

    Rate limiting is handled by the BybitAdapter class.
    """

    def __init__(self, bybit_adapter: BybitAdapter):
        """
        Initialize the HistoricalDataManager.

        Args:
            bybit_adapter: Instance of BybitAdapter for API interactions
        """
        self.bybit = bybit_adapter
        self.batch_size = 1000  # Maximum allowed by Bybit API
        self.max_retries = 5
        self.backoff_factor = 2

    def _make_api_call(self, *args, **kwargs):
        """
        Make an API call with retry logic.

        Args:
            *args: Positional arguments for the API call
            **kwargs: Keyword arguments for the API call

        Returns:
            dict: The API response

        Raises:
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
                    logger.warning(f"API call failed (attempt {retries}/{self.max_retries}). Retrying in {sleep_time}s")
                    time.sleep(sleep_time)
                    continue
                raise

        raise Exception(f"Failed to make API call after {self.max_retries} retries")

    @with_db_retry(max_attempts=3)
    def _get_symbol_first_trade(self, symbol: str) -> Optional[int]:
        """
        Get the timestamp of the first trade for a symbol.

        Args:
            symbol: Trading pair symbol name

        Returns:
            Optional[int]: Launch timestamp in milliseconds or None if not found
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

        Args:
            session: Database session
            symbol: Trading pair symbol
            symbol_id: Database ID for the symbol
            start_time: Start time in milliseconds
            end_time: End time in milliseconds

        Returns:
            bool: True if data was processed successfully, False otherwise
        """
        try:
            logger.debug(f"Processing chunk for {symbol}")
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
                    last_timestamp = formatted_data[-1][0]  # Latest time after sorting
                    self._update_checkpoint(session, symbol, last_timestamp)
                    return True

                logger.debug(f"No data available for {symbol} in requested time range")
                return False

            logger.warning(f"API error for {symbol}")
            return False

        except Exception as e:
            logger.error(f"Error processing chunk for {symbol}: {e}")
            raise

    @with_db_retry(max_attempts=3)
    def _update_checkpoint(self, session: Session, symbol: str, last_timestamp: int) -> None:
        """
        Update or create a checkpoint for a symbol's data collection progress.

        Args:
            session: Database session
            symbol: Trading pair symbol
            last_timestamp: Last processed timestamp in milliseconds
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
            logger.debug(f"Updated checkpoint for {symbol}")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update checkpoint for {symbol}: {e}")
            raise

    def fetch_complete_history(self, symbol: str) -> bool:
        """
        Fetch complete historical data for a symbol.

        Args:
            symbol: Trading pair symbol name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with session_scope() as session:
                symbol_id = get_symbol_id(session, symbol)

                if not session.query(Symbol).filter_by(id=symbol_id).first():
                    logger.error(f"Symbol {symbol} does not exist in database")
                    return False

                start_time = self._get_symbol_first_trade(symbol)
                if start_time is None:
                    logger.error(f"Could not determine first trade time for {symbol}")
                    return False

                end_time = to_timestamp(datetime.now(timezone.utc))
                current_start = start_time

                while current_start < end_time:
                    try:
                        with session.begin_nested():
                            current_end = min(current_start + (self.batch_size * 5 * 60 * 1000), end_time)
                            success = self._process_time_chunk(session, symbol, symbol_id, current_start, current_end)

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
                logger.info(f"Historical data fetch complete for {symbol}. Total records: {records}")
                return True

        except Exception as e:
            logger.error(f"Error in historical data collection for {symbol}: {e}")
            return False