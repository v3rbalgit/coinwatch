# src/api/bybit_adapter.py

import threading
import time
import logging
from typing import List, Dict, Any, Optional
from pybit.unified_trading import HTTP
from requests.exceptions import RequestException
from src.utils.exceptions import APIError

logger = logging.getLogger(__name__)

class BybitAdapter:
    # Class-level variables shared among all instances (and thus all threads)
    _lock = threading.Lock()
    _tokens = 600  # Bybit allows 600 requests per 5 seconds
    _last_refill = time.time()

    RATE_LIMIT = 600  # Max requests per window
    WINDOW_SIZE = 5   # Window size in seconds

    def __init__(self):
        # Each thread can have its own session
        self.session = HTTP()
        self._local_lock = threading.Lock()  # Instance-level lock for better granularity

    @classmethod
    def _acquire_token(cls):
        """Acquire a token for making an API request."""
        with cls._lock:
            current_time = time.time()
            elapsed = current_time - cls._last_refill

            # Refill tokens if window has passed
            if elapsed >= cls.WINDOW_SIZE:
                cls._tokens = cls.RATE_LIMIT
                cls._last_refill = current_time
                logger.debug("Token bucket refilled")

            if cls._tokens > 0:
                cls._tokens -= 1
                return True
            else:
                return False

    def _handle_rate_limit(self):
        """Wait for token availability before making an API request."""
        while not self._acquire_token():
            sleep_time = self.WINDOW_SIZE - (time.time() - self._last_refill)
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)

    def _validate_response(self, response: Any) -> Dict[str, Any]:
        """Validate API response and return the response dictionary."""
        # Check if response is a tuple
        # PyBit sometimes returns a tuple (response, request_time, headers)
        if isinstance(response, tuple):
            response = response[0]  # Extract the response dictionary

        if not isinstance(response, dict):
            raise ValueError(f"Invalid response type: {type(response)}")

        ret_code = response.get('retCode')
        if ret_code is None:
            raise ValueError("Response missing retCode")

        if ret_code != 0:
            error_msg = f"API Error: {ret_code} - {response.get('retMsg')}"
            logger.error(error_msg)
            raise APIError(error_msg)

        return response

    def get_kline(self,
                  symbol: str,
                  interval: str = '5',
                  category: str = 'linear',
                  limit: int = 200,
                  start_time: Optional[int] = None,
                  end_time: Optional[int] = None,
                  retry_count: int = 3) -> Dict[str, Any]:
        """
        Fetch kline data from Bybit API.

        Args:
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            Other args remain unchanged
        """
        for attempt in range(retry_count):
            try:
                self._handle_rate_limit()

                params = {
                    "category": category,
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit or 200
                }

                if start_time is not None:
                    params["start"] = start_time
                if end_time is not None:
                    params["end"] = end_time

                logger.debug(f"Making request for {symbol} with params: {params}")
                raw_response = self.session.get_kline(**params)

                if isinstance(raw_response, tuple):
                    response = raw_response[0]
                else:
                    response = raw_response

                return self._validate_response(response)

            except APIError as e:
                if "Too many visits" in str(e) or "Rate limit exceeded" in str(e):
                    logger.warning(f"Rate limit error: {e}")
                    sleep_time = self.WINDOW_SIZE
                    logger.debug(f"Sleeping for {sleep_time} seconds before retrying")
                    time.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"APIError on attempt {attempt + 1}: {e}")
                    raise

            except (RequestException, ConnectionError) as e:
                logger.error(f"RequestException on attempt {attempt + 1}: {e}")
                if attempt < retry_count - 1:
                    sleep_time = 2 ** attempt
                    logger.debug(f"Retrying in {sleep_time} seconds")
                    time.sleep(sleep_time)
                    continue
                raise

        raise APIError(f"Failed to get kline data for {symbol} after {retry_count} attempts")


    def get_active_instruments(self, limit: Optional[int] = None) -> List[str]:
        """Fetch only actively trading USDT perpetual pairs."""
        try:
            self._handle_rate_limit()
            response = self.session.get_instruments_info(category='linear')
            response = self._validate_response(response)

            result = response.get('result', {})
            instrument_list = result.get('list', [])

            symbols = [
                item['symbol']
                for item in instrument_list
                if isinstance(item, dict) and
                item.get('status') == 'Trading' and
                'symbol' in item and
                isinstance(item['symbol'], str) and
                'USDT' in item['symbol']
            ]

            if limit is not None:
                symbols.sort()
                symbols = symbols[:limit]

            logger.info(f"Successfully fetched {len(symbols)} active trading pairs")
            return symbols

        except Exception as e:
            logger.exception(f"Failed to fetch active instruments: {e}")
            raise