# src/adapters/coingecko.py

from typing import Dict, Any, Optional
import aiohttp
import asyncio

from ..config import CoingeckoConfig
from ..adapters.base import APIAdapter
from ..utils.logger import LoggerSetup
from ..core.exceptions import AdapterError
from ..utils.retry import RetryConfig, RetryStrategy
from ..utils.rate_limit import TokenBucket, RateLimitConfig

logger = LoggerSetup.setup(__name__)

class CoinGeckoAdapter(APIAdapter):
    """
    Async CoinGecko API adapter using aiohttp.

    Advantages:
    - True async implementation
    - Better resource utilization
    - More control over HTTP sessions
    - Better error handling
    - Proper connection pooling
    """

    BASE_URL = "https://api.coingecko.com/api/v3"
    PRO_URL = "https://pro-api.coingecko.com/api/v3"

    def __init__(self, config: CoingeckoConfig):
        super().__init__(config)

        # Initialize rate limiter
        self._rate_limiter = TokenBucket(RateLimitConfig(
            calls_per_window=config.rate_limit,
            window_size=config.rate_limit_window,
            max_monthly_calls=config.monthly_limit
        ))

        self._api_key = config.api_key

        # Base URL based on testnet setting
        self._base_url = self.PRO_URL if config.pro_account else self.BASE_URL

        # Configure retry strategy
        self._retry_strategy = RetryStrategy(RetryConfig(
            base_delay=2.0,
            max_delay=60.0,
            max_retries=3,
            jitter_factor=0.1
        ))

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with CoinGecko configuration"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        if self._api_key:
            key_header = 'x-cg-pro-api-key' if self._config.pro_account else 'x-cg-api-key'
            headers[key_header] = self._api_key

        return aiohttp.ClientSession(
            base_url=self._base_url,
            timeout=aiohttp.ClientTimeout(total=30),
            headers=headers
        )

    async def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
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

                async with session.request(method, endpoint, **kwargs) as response:
                    if response.status == 429:  # Rate limit exceeded
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limit exceeded, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    return await response.json()

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

    async def get_coin_id(self, symbol: str) -> Optional[str]:
        """Get CoinGecko coin ID for a symbol"""
        try:
            # Use coins/list endpoint for efficient lookup
            coins = await self._request(
                'GET',
                '/coins/list',
                params={'include_platform': 'false'}
            )

            # Find matching symbol (case-insensitive)
            for coin in coins:
                if coin['symbol'].lower() == symbol.lower():
                    return coin['id']

            return None

        except Exception as e:
            logger.error(f"Error getting coin ID for {symbol}: {e}")
            return None

    async def get_coin_info(self, coin_id: str) -> Dict[str, Any]:
        """Get detailed coin information"""
        try:
            return await self._request(
                'GET',
                f'/coins/{coin_id}',
                params={
                    'localization': 'false',
                    'tickers': 'false',
                    'market_data': 'false',
                    'community_data': 'false',
                    'developer_data': 'false'
                }
            )

        except Exception as e:
            logger.error(f"Error getting coin info for {coin_id}: {e}")
            raise AdapterError(f"Failed to get coin info: {str(e)}")