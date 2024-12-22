from typing import AsyncGenerator
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

    BASE_URL = "https://api.coingecko.com/api/v3/"
    PRO_URL = "https://pro-api.coingecko.com/api/v3/"

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
            try:
                response.raise_for_status()
                return await response.json()
            except aiohttp.ClientResponseError as e:
                if e.status == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('retry-after', 60))
                    self.logger.warning(f"CoinGecko API Rate limit exceeded, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    # Retry the request after waiting
                    return await self._request(method, endpoint, **kwargs)
                raise


    @redis_cached[list[dict]](ttl=86400)  # Cache for 24 hours
    async def get_coin_ids(self) -> list[dict]:
        """Get a list of CoinGecko coin IDs"""
        try:
            # Use coins/list endpoint for efficient lookup
            coins = await self._request(
                'GET',
                'coins/list',
                params={'include_platform': 'false'}
            )
            return coins

        except Exception as e:
            self.logger.error(f"Error getting coin IDs: {e}")
            return []


    def _find_canonical_token(self, coins: list[dict], symbol: str) -> dict | None:
        """
        Find the canonical/official token from a list of coins with potentially matching symbols.

        Args:
            coins: List of coin dictionaries from CoinGecko API
            symbol: Token symbol to match (e.g. 'btc', 'eth')

        Returns:
            The best matching coin dictionary or None if no matches found
        """
        # Find all coins matching the symbol
        matching_coins = [
            coin for coin in coins
            if coin['symbol'].lower() == symbol.lower()
        ]

        if not matching_coins:
            return None

        if len(matching_coins) == 1:
            return matching_coins[0]

        # Multiple matches - score each token based on characteristics of official tokens
        def score_token(coin: dict) -> tuple[int, int, int]:
            # 1. ID complexity score (fewer special chars = higher score)
            special_chars = sum(1 for c in coin['id'] if not c.isalnum())
            id_score = 100 - special_chars - len(coin['id'])

            # 2. Name match score (name matching symbol = higher score)
            # e.g. for BTC, prefer "Bitcoin" over "Bitcoin Wrapped"
            name_words = coin['name'].lower().split()
            symbol_in_name = symbol.lower() in name_words
            name_score = 100 if symbol_in_name else 50
            name_score -= len(name_words)  # Prefer shorter names

            # 3. ID match score (id matching symbol = higher score)
            # e.g. for BTC, prefer "bitcoin" over "wrapped-bitcoin"
            id_words = coin['id'].lower().split('-')
            symbol_in_id = symbol.lower() in id_words
            id_match_score = 100 if symbol_in_id else 50
            id_match_score -= len(id_words)  # Prefer shorter IDs

            return (id_match_score, name_score, id_score)

        # Return the highest scoring coin
        return max(matching_coins, key=score_token)


    async def get_coin_id(self, token: str) -> str | None:
        """
        Get CoinGecko coin ID for a token, handling cases where multiple tokens
        share the same symbol by identifying the canonical/official token.

        Args:
            token: Token symbol to look up (e.g. 'btc', 'eth')

        Returns:
            The canonical CoinGecko coin ID for the token, or None if not found
        """
        try:
            coins = await self.get_coin_ids()

            # Try to find canonical token with original symbol
            best_match = self._find_canonical_token(coins, token)
            if best_match:
                return best_match['id']

            # If not found and token starts with numbers, try without numeric prefix
            if token[0].isdigit():
                stripped_token = token.lstrip('0123456789')
                best_match = self._find_canonical_token(coins, stripped_token)
                if best_match:
                    self.logger.info(f"Found coin ID for {token} by stripping leading numbers -> {stripped_token}")
                    return best_match['id']

            return None

        except Exception as e:
            self.logger.error(f"Error getting coin ID for {token}: {e}")
            return None


    async def get_metadata(self, coin_id: str) -> dict:
        """Get detailed coin metadata"""
        try:
            return await self._request(
                'GET',
                f'coins/{coin_id}',
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


    async def get_market_data(self, coin_ids: list[str]) -> AsyncGenerator[dict, None]:
        """
        Get market data for multiple coins handling pagination.

        Args:
            coin_ids: List of CoinGecko coin IDs

        Yields:
            Market data dictionary for each coin as it's received
        """
        try:
            per_page = 250  # Maximum allowed by CoinGecko
            received_ids = set()
            ids_chunks = [coin_ids[i:i + per_page] for i in range(0, len(coin_ids), per_page)]

            # Process each chunk of IDs
            for chunk in ids_chunks:
                page = 1
                while True:
                    params = {
                        'ids': ','.join(chunk),
                        'vs_currency': 'usd',
                        'order': 'market_cap_desc',
                        'per_page': per_page,
                        'page': page,
                        'sparkline': 'false'
                    }

                    results = await self._request(
                        'GET',
                        'coins/markets',
                        params=params
                    )

                    if not results:
                        break

                    # Yield each result as we get it
                    for result in results:
                        received_ids.add(result['id'])
                        yield result

                    # If we got less than per_page results, we've hit the last page
                    if len(results) < per_page:
                        break

                    page += 1

            # Final check for any missing IDs
            missing_ids = set(coin_ids) - received_ids
            if missing_ids:
                self.logger.warning(f"Missing market data for coins: {missing_ids}")

        except Exception as e:
            self.logger.error(f"Error getting market data for {len(coin_ids)} coins: {e}")
            raise
