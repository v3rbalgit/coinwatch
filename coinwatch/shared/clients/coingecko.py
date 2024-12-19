import aiohttp
import asyncio

from shared.core.exceptions import AdapterError
from shared.core.config import CoingeckoConfig
from shared.core.protocols import APIAdapter
from shared.utils.logger import LoggerSetup
from shared.utils.cache import redis_cached, RedisCache
from shared.utils.rate_limit import RateLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


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
        super().__init__()

        self._config = config or CoingeckoConfig()

        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            calls_per_window=config.rate_limit,
            window_size=config.rate_limit_window,
            max_monthly_calls=config.monthly_limit
        )

        self._api_key = config.api_key

        # Base URL based on testnet setting
        self._base_url = self.PRO_URL if config.pro_account else self.BASE_URL

        # Initialize Redis cache with namespace
        self.cache = RedisCache(
            redis_url=config.redis_url,
            namespace="coingecko"
        )

        self.logger = LoggerSetup.setup(__class__.__name__)

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(aiohttp.ClientError),
        reraise=True
    )
    async def _request(self, method: str, endpoint: str, **kwargs):
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

        async with session.request(method, endpoint, **kwargs) as response:
            if response.status == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"Rate limit exceeded, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientError("Coingecko API Rate limit exceeded")

            response.raise_for_status()
            return await response.json()

    @redis_cached[str | None](ttl=86400)  # Cache for 24 hours
    async def get_coin_id(self, symbol: str) -> str | None:
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
            self.logger.error(f"Error getting coin ID for {symbol}: {e}")
            return None

    async def get_coin_info(self, coin_id: str) -> dict:
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
            self.logger.error(f"Error getting coin info for {coin_id}: {e}")
            raise AdapterError(f"Failed to get coin info: {str(e)}")

    async def get_market_data(self, coin_ids: list[str]) -> list[dict]:
        """
        Get market data for multiple coins handling pagination.

        Args:
            coin_ids: List of CoinGecko coin IDs

        Returns:
            List of market data dictionaries for all requested coins
        """
        try:
            all_results = []
            page = 1
            per_page = 250  # Maximum allowed by CoinGecko
            ids_chunks = [coin_ids[i:i + 250] for i in range(0, len(coin_ids), 250)]

            # Process each chunk of IDs
            for chunk in ids_chunks:
                while True:
                    params = {
                        'ids': ','.join(chunk),
                        'vs_currency': 'usd',
                        'order': 'market_cap_desc',
                        'per_page': per_page,
                        'page': page,
                        'sparkline': False
                    }

                    results = await self._request(
                        'GET',
                        '/coins/markets',
                        params=params
                    )

                    if not results:
                        break

                    all_results.extend(results)

                    # If we got less than per_page results, we've hit the last page
                    if len(results) < per_page:
                        break

                    page += 1

            # Ensure we got data for all requested coins
            received_ids = {result['id'] for result in all_results}
            missing_ids = set(coin_ids) - received_ids
            if missing_ids:
                self.logger.warning(f"Missing market data for coins: {missing_ids}")

            return all_results

        except Exception as e:
            self.logger.error(f"Error getting market data for {len(coin_ids)} coins: {e}")
            raise