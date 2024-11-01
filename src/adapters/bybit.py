import asyncio
from asyncio import Lock
import time
from typing import List, Optional, Dict, Any, Callable, TypeVar, Protocol, cast, Union, overload
from decimal import Decimal
import uuid
from pybit.unified_trading import HTTP
from functools import partial
from requests.structures import CaseInsensitiveDict
from datetime import timedelta

from src.config import BybitConfig
from src.core.exceptions import ValidationError
from src.utils.time import from_timestamp, get_current_timestamp

from ..core.models import KlineData, SymbolInfo
from ..core.protocols import ExchangeAdapter
from ..utils.domain_types import Timeframe, SymbolName, Timestamp
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

# Type definitions
T = TypeVar('T')
BybitResponse = Union[
    tuple[Any, timedelta, CaseInsensitiveDict[str]],
    tuple[Any, timedelta],
    Dict[str, Any]
]

class BybitMethod(Protocol):
    """Protocol for Bybit API methods"""
    def __call__(self, *args: Any, **kwargs: Any) -> BybitResponse: ...

class BybitAdapter(ExchangeAdapter):
    """
    Bybit exchange adapter with async rate limiting.

    Handles API communication with Bybit exchange, including:
    - Rate limiting with token bucket algorithm
    - Data validation and error handling
    - Conversion of exchange data formats to internal models
    - Thread-safe API operations

    Note: Bybit API returns kline data in descending order (newest first).
    """

    def __init__(self, config: Optional[BybitConfig] = None):
        """
        Initialize adapter with optional configuration.

        Args:
            config: Optional configuration for API access and rate limiting
        """
        self._config = config or BybitConfig()
        self._initialized = False

        # Set rate limits from config or use defaults
        self.RATE_LIMIT = self._config.rate_limit if config else 600
        self.WINDOW_SIZE = self._config.rate_limit_window if config else 300

        # Store credentials for future use
        self._api_key = self._config.api_key
        self._api_secret = self._config.api_secret
        self._testnet=self._config.testnet

        # Initialize rate limiting
        self._lock = Lock()
        self._tokens = self.RATE_LIMIT
        self._last_refill = time.time()

    def _create_session(self) -> HTTP:
        """Create a new HTTP session for thread-safe operations"""
        return HTTP(
            testnet=self._testnet,
            api_key=self._api_key,
            api_secret=self._api_secret
        )

    @overload
    async def _execute_request(
        self,
        func: Callable[[HTTP], BybitResponse],
    ) -> Dict[str, Any]: ...

    @overload
    async def _execute_request(
        self,
        func: Callable[[HTTP], BybitResponse],
        **kwargs: Any
    ) -> Dict[str, Any]: ...

    @overload
    async def _execute_request(
        self,
        func: Callable[[HTTP, str], BybitResponse],
        symbol: str
    ) -> Dict[str, Any]: ...

    async def _execute_request(
        self,
        func: Callable[[HTTP], BybitResponse] | Callable[[HTTP, str], BybitResponse],
        *args: Any,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Execute a request with a dedicated session.

        Args:
            func: The API function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Dict[str, Any]: The validated API response

        Raises:
            Exception: If the request fails
        """
        await self._handle_rate_limit()

        # Create a new session for this specific request
        session = self._create_session()

        try:
            # Create a partial function with the session
            bound_func = partial(func, session, *args, **kwargs)

            # Execute in thread pool
            response = await asyncio.to_thread(bound_func)
            return self._validate_response(response)
        finally:
            # Clean up underlying requests session
            session_obj = getattr(session, '_session', None)
            if session_obj is not None:
                session_obj.close()

    async def _acquire_token(self) -> bool:
        """
        Attempt to acquire a rate limit token.

        Returns:
            bool: True if token acquired, False if no tokens available
        """
        async with self._lock:
            current_time = time.time()
            elapsed = current_time - self._last_refill

            if elapsed >= self.WINDOW_SIZE:
                self._tokens = self.RATE_LIMIT
                self._last_refill = current_time
                logger.debug("Rate limit token bucket refilled")

            if self._tokens > 0:
                self._tokens -= 1
                return True
            return False

    async def _handle_rate_limit(self) -> None:
        """Handle rate limiting by waiting when necessary."""
        while not await self._acquire_token():
            sleep_time = self.WINDOW_SIZE - (time.time() - self._last_refill)
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

    def _validate_response(self, response: BybitResponse) -> Dict[str, Any]:
        """
        Validate API response format and error codes.

        Args:
            response: Raw API response

        Returns:
            Dict[str, Any]: Validated response data

        Raises:
            ValueError: If response format is invalid or contains error
        """
        if isinstance(response, tuple):
            response = response[0]

        response_dict = cast(Dict[str, Any], response)
        if not isinstance(response_dict, dict):
            raise ValueError(f"Invalid response type: {type(response_dict)}")

        ret_code = response_dict.get('retCode')
        if ret_code is None:
            raise ValueError("Response missing retCode")

        if ret_code != 0:
            raise ValueError(f"API Error: {ret_code} - {response_dict.get('retMsg')}")

        return response_dict

    @staticmethod
    def _get_klines(session: HTTP, **kwargs: Any) -> BybitResponse:
        """Get klines using provided session"""
        return session.get_kline(**kwargs)

    @staticmethod
    def _get_symbols(session: HTTP) -> BybitResponse:
        """Get symbols using provided session"""
        return session.get_instruments_info(category='linear')

    @staticmethod
    def _get_ticker(session: HTTP, symbol: str) -> BybitResponse:
        """Get ticker using provided session"""
        return session.get_tickers(category="linear", symbol=symbol)

    async def initialize(self) -> None:
        """
        Initialize the adapter by testing connection.

        Raises:
            Exception: If initialization fails
        """
        if not self._initialized:
            try:
                await self.get_latest_price(SymbolName("BTCUSDT"))
                self._initialized = True
                logger.info("BybitAdapter initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize BybitAdapter: {e}")
                raise

    async def get_symbols(self) -> List[SymbolInfo]:
        """
        Get available trading pairs from Bybit.

        Returns:
            List[SymbolInfo]: List of available trading symbols

        Raises:
            Exception: If fetching symbols fails
        """
        try:
            response = await self._execute_request(self._get_symbols)

            symbols: List[SymbolInfo] = []
            for item in response.get('result', {}).get('list', []):
                if (isinstance(item, dict) and
                    item.get('status') == 'Trading' and
                    'USDT' in item.get('symbol', '')):

                    symbols.append(SymbolInfo(
                        name=item['symbol'],
                        base_asset=item['baseCoin'],
                        quote_asset=item['quoteCoin'],
                        price_precision=str(item['priceFilter']['tickSize']).count('0'),
                        qty_precision=str(item['lotSizeFilter']['qtyStep']).count('0'),
                        min_order_qty=Decimal(str(item['lotSizeFilter']['minOrderQty'])),
                        launch_time=Timestamp(int(item.get('launchTime', '0')))
                    ))

            logger.info(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {e}")
            raise

    async def get_klines(self,
                        symbol: SymbolName,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        limit: int = 5) -> List[KlineData]:
        """
        Fetch kline data from Bybit.
        Note: Results are returned in ascending order (oldest first).

        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            start_time: Optional start timestamp (milliseconds)
            limit: Maximum number of candles to fetch

        Returns:
            List[KlineData]: Kline data in ascending order

        Raises:
            ValidationError: If request parameters are invalid
            Exception: If API request fails
        """
        try:
            request_id = uuid.uuid4()
            logger.debug(f"[{request_id}] Starting klines request for {symbol}")
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": timeframe.value,
                "limit": limit
            }

            if start_time is not None:
                current_time = get_current_timestamp()
                if start_time > current_time:
                    raise ValidationError(
                        f"Cannot request future data: {from_timestamp(start_time)}"
                    )
                params["start"] = start_time

            logger.debug(f"[{request_id}] Executing request with params: {params}")
            response = await self._execute_request(self._get_klines, **params)
            logger.debug(f"[{request_id}] Received response: {response}")

            klines: List[KlineData] = []
            # Process items in reverse order to convert from descending to ascending
            for item in reversed(response.get('result', {}).get('list', [])):
                timestamp = int(item[0])

                # Skip future timestamps
                if timestamp > get_current_timestamp():
                    logger.warning(
                        f"Received future timestamp from API: {from_timestamp(timestamp)}"
                    )
                    continue

                logger.debug(f"Processing kline item: {item}")
                klines.append(KlineData(
                    timestamp=timestamp,
                    open_price=Decimal(str(item[1])),
                    high_price=Decimal(str(item[2])),
                    low_price=Decimal(str(item[3])),
                    close_price=Decimal(str(item[4])),
                    volume=Decimal(str(item[5])),
                    turnover=Decimal(str(item[6])),
                    symbol=symbol,
                    timeframe=timeframe.value
                ))

            logger.debug(f"[{request_id}] Finished processing klines for {symbol}")
            return klines

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {e}")
            raise

    async def get_latest_price(self, symbol: SymbolName) -> Decimal:
        """
        Get latest price for symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Decimal: Latest price

        Raises:
            Exception: If fetching price fails
        """
        try:
            response = await self._execute_request(self._get_ticker, symbol)
            ticker = response.get('result', {}).get('list', [{}])[0]
            return Decimal(str(ticker.get('lastPrice', '0')))

        except Exception as e:
            logger.error(f"Failed to fetch latest price for {symbol}: {e}")
            raise

    async def close(self) -> None:
        """Cleanup adapter resources."""
        self._initialized = False
        logger.info("BybitAdapter closed")