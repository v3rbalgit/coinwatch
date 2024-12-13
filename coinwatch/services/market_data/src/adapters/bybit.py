# src/adapters/bybit.py

from decimal import Decimal
from typing import Any, List, Optional
import aiohttp
import asyncio
import time

from ..adapters.registry import ExchangeAdapter
from shared.core.config import BybitConfig
from shared.core.exceptions import AdapterError
from shared.core.models import KlineData, SymbolInfo
from shared.utils.domain_types import Timeframe
from shared.utils.logger import LoggerSetup
from shared.utils.rate_limit import TokenBucket, RateLimitConfig
from shared.utils.time import TimeUtils
from shared.utils.retry import RetryConfig, RetryStrategy

logger = LoggerSetup.setup(__name__)

class BybitAdapter(ExchangeAdapter):
    """
    Async Bybit API adapter using aiohttp.

    Handles:
    - Rate limiting with circuit breaker
    - Connection management
    - Retry logic
    """

    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"

    # Class-level variables for circuit breaker
    _rate_limit_reset: Optional[int] = None  # Shared timestamp when rate limit resets (in milliseconds)
    _circuit_breaker_lock = asyncio.Lock()   # Lock for thread-safe access

    def __init__(self, config: BybitConfig):
        super().__init__(config)

        # Base URL based on testnet setting
        self._base_url = self.TESTNET_URL if config.testnet else self.BASE_URL

        # Initialize rate limiter
        self._rate_limiter = TokenBucket(RateLimitConfig(
            calls_per_window=config.rate_limit,
            window_size=config.rate_limit_window
        ))

        # Configure retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=1.0,
            max_delay=30.0,
            max_retries=3,
            jitter_factor=0.1
        ))

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with Bybit configuration"""
        return aiohttp.ClientSession(
            base_url=self._base_url,
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'Content-Type': 'application/json'}
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
        attempt = 0
        session = await self._get_session()

        while True:
            try:
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
                            continue

                    response.raise_for_status()
                    data = await response.json()

                    # Handle Bybit-specific error responses
                    if data.get('retCode') != 0:
                        raise AdapterError(
                            f"API error {data.get('retCode')}: {data.get('retMsg')}"
                        )

                    # Clear rate limit reset after successful request
                    async with self.__class__._circuit_breaker_lock:
                        self.__class__._rate_limit_reset = None

                    return data.get('result', {})

            except aiohttp.ClientError as e:
                should_retry, reason = self._retry_strategy.should_retry(attempt, e)
                if should_retry:
                    attempt += 1
                    delay = self._retry_strategy.get_delay(attempt)
                    logger.warning(
                        f"API request failed ({reason}), "
                        f"retry {attempt} after {delay:.2f}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                    continue

                raise AdapterError(f"API request failed: {str(e)}")

    async def get_symbols(self) -> List[SymbolInfo]:
        """Get available trading pairs"""
        try:
            data = await self._request(
                'GET',
                '/v5/market/instruments-info',
                params={'category': 'linear'}
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

            logger.info(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {e}")
            raise

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
                        limit: Optional[int] = None) -> List[KlineData]:
        """Get kline (candlestick) data"""
        try:
            params = {
                "category": "linear",
                "symbol": symbol.name,
                "interval": timeframe.value,
                "limit": limit or self._config.kline_limit
            }

            if start_time is not None:
                params['start'] = start_time
            if end_time is not None:
                params['end'] = end_time

            data = await self._request(
                'GET',
                '/v5/market/kline',
                params=params
            )

            klines: List[KlineData] = []
            current_time = TimeUtils.get_current_timestamp()

            # Process klines in ascending order
            for item in reversed(data.get('list', [])):
                timestamp = int(item[0])

                if timestamp > current_time:
                    continue

                if end_time and timestamp > end_time:
                    continue
                if start_time and timestamp < start_time:
                    continue

                klines.append(KlineData(
                    timestamp=timestamp,
                    open_price=Decimal(str(item[1])),
                    high_price=Decimal(str(item[2])),
                    low_price=Decimal(str(item[3])),
                    close_price=Decimal(str(item[4])),
                    volume=Decimal(str(item[5])),
                    turnover=Decimal(str(item[6])),
                    symbol=symbol,
                    timeframe=timeframe
                ))

            return klines

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {e}")
            raise
