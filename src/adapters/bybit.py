# src/adapters/bybit.py

from decimal import Decimal
from typing import Dict, Any, List, Optional
import aiohttp
import asyncio
import hmac
import hashlib
import time

from ..adapters.registry import ExchangeAdapter
from ..config import BybitConfig
from ..core.exceptions import AdapterError
from ..core.models import KlineData, SymbolInfo
from ..utils.domain_types import Timeframe, Timestamp
from ..utils.logger import LoggerSetup
from ..utils.rate_limit import TokenBucket, RateLimitConfig
from ..utils.time import TimeUtils
from ..utils.retry import RetryConfig, RetryStrategy

logger = LoggerSetup.setup(__name__)

class BybitAdapter(ExchangeAdapter):
    """
    Async Bybit API adapter using aiohttp.

    Handles:
    - API authentication
    - Request signing
    - Rate limiting
    - Connection management
    """

    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"

    def __init__(self, config: BybitConfig):
        super().__init__(config)

        # Base URL based on testnet setting
        self._base_url = self.TESTNET_URL if config.testnet else self.BASE_URL

        # Authentication
        self._api_key = config.api_key
        self._api_secret = config.api_secret

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

    def _generate_signature(self, params: Dict[str, Any], timestamp: int) -> str:
        """Generate signature for authenticated requests"""
        if not self._api_key or not self._api_secret:
            raise AdapterError("API credentials not configured")

        # Create parameter string in correct order
        param_str = str(timestamp) + self._api_key + str(self._config.recv_window) + str(params)

        # Generate HMAC SHA256 signature
        return hmac.new(
            self._api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

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
                      auth: bool = False,
                      **kwargs: Any) -> Any:
        """
        Make API request with retry logic and rate limiting

        Args:
            method: HTTP method
            endpoint: API endpoint
            auth: Whether request needs authentication
            **kwargs: Additional request parameters
        """
        attempt = 0
        session = await self._get_session()

        while True:
            try:
                # Handle rate limiting
                await self._rate_limiter.acquire()

                # Add authentication if required
                if auth:
                    timestamp = int(time.time() * 1000)
                    params = kwargs.get('params', {})

                    # Add authentication parameters
                    auth_params = {
                        'api_key': self._api_key,
                        'timestamp': timestamp,
                        'recv_window': self._config.recv_window,
                        'sign': self._generate_signature(params, timestamp)
                    }

                    if method == 'GET':
                        # For GET requests, add auth params to query string
                        kwargs['params'] = {**params, **auth_params}
                    else:
                        # For POST requests, add auth params to body
                        kwargs['json'] = {**params, **auth_params}

                async with session.request(method, endpoint, **kwargs) as response:
                    if response.status == 429:  # Rate limit exceeded
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limit exceeded, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    data = await response.json()

                    # Handle Bybit-specific error responses
                    if data.get('ret_code') != 0:
                        raise AdapterError(
                            f"API error {data.get('ret_code')}: {data.get('ret_msg')}"
                        )

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
                        launch_time=Timestamp(int(item.get('launchTime', '0')))
                    ))

            logger.info(f"Fetched {len(symbols)} active USDT pairs")
            return symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols: {e}")
            raise

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[Timestamp] = None,
                        end_time: Optional[Timestamp] = None,
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
                    timestamp=Timestamp(timestamp),
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