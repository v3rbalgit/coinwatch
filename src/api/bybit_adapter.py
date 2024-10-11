from typing import List, Optional, Any, Union, Tuple
from pybit.unified_trading import HTTP
from requests.structures import CaseInsensitiveDict
from datetime import timedelta
import logging
from pybit.unified_trading import HTTP

logger = logging.getLogger(__name__)

def get_kline(symbol: str, interval: str = '60', limit: int = 1000, start_time: Optional[int] = None, end_time: Optional[int] = None):
    """
    Fetch kline (candlestick) data from Bybit API.

    Args:
    symbol (str): The trading pair symbol (e.g., 'BTCUSDT')
    interval (str): Kline interval. Default is '60' (1 hour)
    limit (int): Number of candles to fetch. Default is 1000 (maximum allowed by Bybit)
    start_time (Optional[int]): Start timestamp in milliseconds. Optional.
    end_time (Optional[int]): End timestamp in milliseconds. Optional.

    Returns:
    dict: Response from Bybit API containing kline data
    """
    session = HTTP()

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    if start_time is not None:
        params["start"] = start_time
    if end_time is not None:
        params["end"] = end_time

    return session.get_kline(**params)

def get_instruments() -> List[str]:
    """
    Fetch the list of USDT-paired instruments from Bybit API.

    Returns:
    List[str]: A list of symbol names for USDT-paired instruments.

    Raises:
    RuntimeError: If there's an error fetching or processing the instrument data.
    """
    session = HTTP()
    symbols: List[str] = []

    try:
        response: Union[Tuple[Any, timedelta, CaseInsensitiveDict[str]], Tuple[Any, timedelta], Any] = session.get_instruments_info(category='linear')

        # Extract the actual response data
        if isinstance(response, tuple):
            res = response[0]  # The first element contains the API response data
        else:
            res = response

        if not isinstance(res, dict):
            raise TypeError(f"Unexpected response type from Bybit API: {type(res)}")

        if res.get('retMsg') != 'OK':
            raise ValueError(f"API error: {res.get('retCode')} - {res.get('retMsg')}")

        result = res.get('result', {})
        if not isinstance(result, dict):
            raise TypeError(f"Unexpected 'result' type in API response: {type(result)}")

        symbol_list = result.get('list', [])
        if not isinstance(symbol_list, list):
            raise TypeError(f"Unexpected 'list' type in API response: {type(symbol_list)}")

        symbols = [
            symbol['symbol'] for symbol in symbol_list
            if isinstance(symbol, dict) and 'symbol' in symbol and 'USDT' in symbol['symbol']
        ]

        if not symbols:
            logger.warning("No USDT-paired symbols found")

        return symbols

    except (ValueError, TypeError) as e:
        logger.error(f"Error processing Bybit API response: {str(e)}")
        raise RuntimeError(f"Failed to fetch instruments: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_instruments: {str(e)}")
        raise RuntimeError(f"Unexpected error while fetching instruments: {str(e)}")