# src/adapters/bybit.py

from decimal import Decimal
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, List, Optional
import aiohttp
import asyncio
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..registry import ExchangeAdapter
from shared.core.config import BybitConfig
from shared.core.enums import Timeframe
from shared.core.exceptions import AdapterError
from shared.core.models import KlineData, SymbolInfo
from shared.utils.logger import LoggerSetup
from shared.utils.rate_limit import RateLimiter
from .bybit_ws import BybitWebsocket

logger = LoggerSetup.setup(__name__)


class BybitAdapter(ExchangeAdapter):
    """
    Async Bybit API adapter using aiohttp.

    Handles:
    - Rate limiting with circuit breaker
    - Connection management
    - Retry logic
    - Websocket streaming via BybitWebsocket client
    """

    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"

    # Class-level variables for circuit breaker
    _rate_limit_reset: Optional[int] = None  # Shared timestamp when rate limit resets (in milliseconds)
    _circuit_breaker_lock = asyncio.Lock()   # Lock for thread-safe access

    def __init__(self, config: BybitConfig):
        self._config = config
        self._base_url = self.TESTNET_URL if self._config.testnet else self.BASE_URL

        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            calls_per_window=self._config.rate_limit,
            window_size=self._config.rate_limit_window
        )

        # Websocket client for streaming
        self._ws_client = BybitWebsocket(config)

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with Bybit configuration"""
        return aiohttp.ClientSession(
            base_url=self._base_url,
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'Content-Type': 'application/json'}
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(aiohttp.ClientError),
        reraise=True
    )
    async def _request(self,
                      method: str,
                      endpoint: str,
                      **kwargs: Any) -> Any:
        """
        Make API request with retry logic and rate limiting

        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
        """
        session = await self._get_session()

        # Handle rate limiting
        await self._rate_limiter.acquire()

        # Check circuit breaker
        async with self.__class__._circuit_breaker_lock:
            current_time = int(time.time() * 1000)
            if self.__class__._rate_limit_reset and current_time < self.__class__._rate_limit_reset:
                sleep_time = (self.__class__._rate_limit_reset - current_time) / 1000
                logger.warning(f"Rate limit active! Sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

        async with session.request(method, endpoint, **kwargs) as response:
            if response.status == 429:  # Rate limit exceeded
                reset_timestamp = int(response.headers.get('X-Bapi-Limit-Reset-Timestamp', 0))
                async with self.__class__._circuit_breaker_lock:
                    self.__class__._rate_limit_reset = reset_timestamp
                    wait_time = (reset_timestamp - current_time) / 1000
                    logger.warning(
                        f"Rate limit hit! Next retry in {wait_time:.2f}s. "
                        f"Endpoint: {endpoint}"
                    )
                    await asyncio.sleep(wait_time)
                    raise aiohttp.ClientError("Bybit API Rate limit exceeded")

            response.raise_for_status()
            data = await response.json()

            # Handle Bybit-specific error responses
            if data.get('retCode') != 0:
                raise AdapterError(f"API error: {data.get('retMsg')}")

            # Clear rate limit reset after successful request
            async with self.__class__._circuit_breaker_lock:
                self.__class__._rate_limit_reset = None

            return data.get('result', {})

    async def get_symbols(self, symbol: Optional[str] = None) -> List[SymbolInfo]:
        """
        Get available trading pairs

        Args:
            symbol (Optional[str]): Name of a symbol to fetch from Bybit.

        Returns:
            List[SymbolInfo]: List of SymbolInfo objects.
        """
        try:
            params = {'category': 'linear'}

            if symbol:
                params['symbol'] = symbol

            data = await self._request(
                'GET',
                '/v5/market/instruments-info',
                params=params
            )

            symbols: List[SymbolInfo] = []
            for item in data.get('list', []):
                if (item.get('status') == 'Trading' and
                    'USDT' in item.get('symbol', '')):

                    symbols.append(SymbolInfo(
                        name=item['symbol'],
                        base_asset=item['baseCoin'],
                        quote_asset=item['quoteCoin'],
                        price_precision=str(item['priceFilter']['tickSize']).count('0'),
                        qty_precision=str(item['lotSizeFilter']['qtyStep']).count('0'),
                        min_order_qty=Decimal(str(item['lotSizeFilter']['minOrderQty'])),
                        launch_time=int(item.get('launchTime', '0'))
                    ))

            logger.debug(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {str(e)}")
            raise

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: int,
                        end_time: int,
                        limit: Optional[int] = None) -> AsyncGenerator[List[KlineData], None]:
        """
        Get kline (candlestick) data

        Args:
            symbol (SymbolInfo): Symbol to get the klines for.
            timeframe (Timeframe): Timeframe to use.
            start_time (int): Start timestamp of the klines.
            end_time (int): End timestamp of the klines.
            limit (Optional[int]): Number of klines to fetch (will default to config value).

        Yields:
            List[KlineData]: List of KlineData objects.
        """
        try:
            params = {
                "category": "linear",
                "symbol": symbol.name,
                "interval": timeframe.value,
                "limit": limit or self._config.kline_limit
            }

            current_start = start_time
            chunk_size_ms = params['limit'] * timeframe.to_milliseconds()

            while current_start < end_time:
                current_end = min(current_start + chunk_size_ms, end_time)

                params['start'] = current_start
                params['end'] = current_end

                data = await self._request(
                    'GET',
                    '/v5/market/kline',
                    params=params
                )

                # Process klines in ascending order
                klines = [KlineData(
                    timestamp=int(item[0]),
                    open_price=Decimal(item[1]),
                    high_price=Decimal(item[2]),
                    low_price=Decimal(item[3]),
                    close_price=Decimal(item[4]),
                    volume=Decimal(item[5]),
                    turnover=Decimal(item[6]),
                    symbol=symbol,
                    timeframe=timeframe
                ) for item in reversed(data.get('list', []))]

                logger.debug(f"Fetched {len(klines)} klines for {symbol.name} on {symbol.exchange}")
                yield klines

                current_start = current_end

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {str(e)}")
            raise

    async def subscribe_klines(self,
                             symbol: SymbolInfo,
                             timeframe: Timeframe,
                             handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Subscribe to real-time kline updates via websocket client"""
        await self._ws_client.subscribe_klines(symbol, timeframe, handler)

    async def unsubscribe_klines(self,
                                symbol: SymbolInfo,
                                timeframe: Timeframe) -> None:
        """Unsubscribe from kline updates via websocket client"""
        await self._ws_client.unsubscribe_klines(symbol, timeframe)

    async def cleanup(self) -> None:
        """Cleanup resources"""
        # Use APIAdapter's session cleanup
        await super().cleanup()
        # Cleanup websocket client
        await self._ws_client.cleanup()
