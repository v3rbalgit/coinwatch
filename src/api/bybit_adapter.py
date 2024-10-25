# src/api/bybit_adapter.py

import threading
import time
from typing import List, Dict, Any, Optional
from pybit.unified_trading import HTTP
from requests.exceptions import RequestException
from src.utils.exceptions import APIError
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class BybitAdapter:
    """
    Thread-safe adapter for Bybit API with rate limiting.

    Manages API requests with token bucket rate limiting system.
    Each instance maintains its own HTTP session while sharing
    rate limit tokens across all instances.
    """

    # Class-level rate limiting
    _lock = threading.Lock()
    _tokens = 600  # Bybit allows 600 requests per 5 seconds
    _last_refill = time.time()

    RATE_LIMIT = 600  # Max requests per window
    WINDOW_SIZE = 5   # Window size in seconds

    def __init__(self):
        """Initialize adapter with a thread-local session."""
        self.session = HTTP()
        self._local_lock = threading.Lock()

    @classmethod
    def _acquire_token(cls) -> bool:
        """
        Acquire a token for making an API request.

        Uses token bucket algorithm for rate limiting.

        Returns:
            bool: True if token acquired, False if no tokens available
        """
        with cls._lock:
            current_time = time.time()
            elapsed = current_time - cls._last_refill

            if elapsed >= cls.WINDOW_SIZE:
                cls._tokens = cls.RATE_LIMIT
                cls._last_refill = current_time
                logger.debug("Rate limit token bucket refilled")

            if cls._tokens > 0:
                cls._tokens -= 1
                return True
            return False

    def _handle_rate_limit(self) -> None:
        """
        Wait for token availability before making an API request.
        Implements blocking wait when rate limit is reached.
        """
        while not self._acquire_token():
            sleep_time = self.WINDOW_SIZE - (time.time() - self._last_refill)
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, waiting {sleep_time:.2f}s")
                time.sleep(sleep_time)

    def _validate_response(self, response: Any) -> Dict[str, Any]:
        """
        Validate API response and return the response dictionary.

        Args:
            response: Raw API response

        Returns:
            Dict[str, Any]: Validated response dictionary

        Raises:
            ValueError: If response format is invalid
            APIError: If API returns error code
        """
        if isinstance(response, tuple):
            response = response[0]

        if not isinstance(response, dict):
            error_msg = f"Invalid response type: {type(response)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        ret_code = response.get('retCode')
        if ret_code is None:
            error_msg = "Response missing retCode"
            logger.error(error_msg)
            raise ValueError(error_msg)

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
        Fetch kline (candlestick) data from Bybit API.

        Args:
            symbol: Trading pair symbol
            interval: Kline interval in minutes (default: '5')
            category: Market category (default: 'linear')
            limit: Number of klines to fetch (default: 200)
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            retry_count: Number of retry attempts (default: 3)

        Returns:
            Dict[str, Any]: Kline data response

        Raises:
            APIError: If API request fails after all retries
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

                logger.debug(f"Requesting klines for {symbol}: {params}")
                raw_response = self.session.get_kline(**params)

                if isinstance(raw_response, tuple):
                    response = raw_response[0]
                else:
                    response = raw_response

                return self._validate_response(response)

            except APIError as e:
                if "Too many visits" in str(e) or "Rate limit exceeded" in str(e):
                    logger.warning(f"Rate limit hit for {symbol}: {e}")
                    sleep_time = self.WINDOW_SIZE
                    logger.debug(f"Backing off for {sleep_time}s")
                    time.sleep(sleep_time)
                    continue

                logger.error(f"API error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt == retry_count - 1:
                    raise

            except (RequestException, ConnectionError) as e:
                logger.error(f"Request error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < retry_count - 1:
                    sleep_time = 2 ** attempt
                    logger.debug(f"Retrying in {sleep_time}s")
                    time.sleep(sleep_time)
                    continue
                raise

        raise APIError(f"Failed to fetch klines for {symbol} after {retry_count} attempts")

    def get_active_instruments(self, limit: Optional[int] = None) -> List[str]:
        """
        Fetch actively trading USDT perpetual pairs.

        Args:
            limit: Optional limit on number of pairs to return

        Returns:
            List[str]: List of active trading pair symbols

        Raises:
            Exception: If API request fails
        """
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

            logger.info(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch active instruments: {e}")
            raise