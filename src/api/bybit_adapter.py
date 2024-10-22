# src/api/bybit_adapter.py

from typing import List, Dict, Any, Optional, TypedDict, Union
from pybit.unified_trading import HTTP
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class BybitResponse(TypedDict):
    retCode: int
    retMsg: str
    result: Dict[str, Any]
    retExtInfo: Dict[str, Any]
    time: int

class BybitAdapter:
    def __init__(self):
        self.session = HTTP()
        self.rate_limit_remaining = 100
        self.last_request_time = 0
        self.min_request_interval = 0.05  # 50ms between requests

    def _handle_rate_limit(self) -> None:
        """Handle rate limiting for API requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _validate_response(self, response: Union[Dict[str, Any], Any]) -> None:
        """Validate API response."""
        if not isinstance(response, dict):
            raise ValueError(f"Invalid response type: {type(response)}")

        ret_code = response.get('retCode')
        if ret_code is None:
            raise ValueError("Response missing retCode")

        if ret_code != 0:
            error_msg = f"API Error: {ret_code} - {response.get('retMsg')}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def get_kline(self,
                  symbol: str,
                  interval: str = '5',
                  limit: int = 200,
                  start_time: Optional[int] = None,
                  end_time: Optional[int] = None,
                  retry_count: int = 3) -> Dict[str, Any]:
        """
        Fetch kline/candlestick data from Bybit.

        Args:
            symbol: Trading pair symbol
            interval: Kline interval in minutes ('5' for 5m, '60' for 1h, etc.)
            limit: Number of klines to fetch (max 200)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            retry_count: Number of retry attempts

        Returns:
            Dict[str, Any]: Processed API response

        Raises:
            ValueError: If API response is invalid
            Exception: For other errors during API call
        """
        for attempt in range(retry_count):
            try:
                self._handle_rate_limit()

                params = {
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "limit": min(limit, 200)
                }

                if start_time:
                    params["start"] = start_time
                if end_time:
                    params["end"] = end_time

                response = self.session.get_kline(**params)
                self._validate_response(response)

                return response

            except ValueError as e:
                if "Too many visits" in str(e):
                    sleep_time = 2 ** attempt
                    logger.warning(f"Rate limit hit, sleeping for {sleep_time} seconds")
                    time.sleep(sleep_time)
                    continue
                raise

            except Exception as e:
                if attempt < retry_count - 1:
                    sleep_time = 2 ** attempt
                    logger.warning(f"Request failed, attempt {attempt + 1}/{retry_count}. "
                                 f"Retrying in {sleep_time} seconds. Error: {str(e)}")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch kline data after {retry_count} attempts: {str(e)}")
                    raise

        raise Exception(f"Failed to get kline data after {retry_count} attempts")

    def get_instruments(self) -> List[str]:
        """
        Fetch all available USDT perpetual trading pairs.

        Returns:
            List[str]: List of trading pair symbols

        Raises:
            ValueError: If API response is invalid
            Exception: For other errors during API call
        """
        try:
            self._handle_rate_limit()
            response = self.session.get_instruments_info(category='linear')
            self._validate_response(response)

            if not isinstance(response, dict) or 'result' not in response:
                raise ValueError("Invalid response format")

            result = response['result']
            if not isinstance(result, dict) or 'list' not in result:
                raise ValueError("Invalid response structure")

            symbols = [
                item['symbol']
                for item in result['list']
                if isinstance(item, dict) and
                'symbol' in item and
                isinstance(item['symbol'], str) and
                'USDT' in item['symbol']
            ]

            logger.info(f"Successfully fetched {len(symbols)} trading pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch instruments: {str(e)}")
            raise

    def get_active_instruments(self) -> List[str]:
        """
        Fetch only actively trading USDT perpetual pairs.

        Returns:
            List[str]: List of active trading pair symbols
        """
        try:
            self._handle_rate_limit()
            response = self.session.get_instruments_info(category='linear')
            self._validate_response(response)

            if not isinstance(response, dict) or 'result' not in response:
                raise ValueError("Invalid response format")

            result = response['result']
            if not isinstance(result, dict) or 'list' not in result:
                raise ValueError("Invalid response structure")

            symbols = [
                item['symbol']
                for item in result['list']
                if isinstance(item, dict) and
                'symbol' in item and
                isinstance(item['symbol'], str) and
                'USDT' in item['symbol'] and
                item.get('status') == 'Trading'  # Only get actively trading pairs
            ]

            logger.info(f"Successfully fetched {len(symbols)} active trading pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch instruments: {str(e)}")
            raise

# Create global instance
bybit = BybitAdapter()