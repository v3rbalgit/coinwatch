# src/adapters/twitter.py

from typing import Dict, Optional
import aiohttp
from decimal import Decimal

from ...utils.logger import LoggerSetup
from ...utils.rate_limit import RateLimiter, RateLimitConfig

logger = LoggerSetup.setup(__name__)

class TwitterAdapter:
    """
    Adapter for Twitter data using RapidAPI.
    Handles rate limiting and data transformation.
    """

    def __init__(self, api_key: str, api_host: str, rate_limit: int, rate_limit_window: int):
        """
        Initialize the Twitter adapter.

        Args:
            api_key (str): RapidAPI key
            api_host (str): RapidAPI host for Twitter endpoint
            rate_limit (int): Maximum requests per window
            rate_limit_window (int): Time window in seconds
        """
        self.api_key = api_key
        self.api_host = api_host
        self.headers = {
            'X-RapidAPI-Key': api_key,
            'X-RapidAPI-Host': api_host
        }

        # Configure rate limiting
        rate_limit_config = RateLimitConfig(
            calls_per_window=rate_limit,
            window_size=rate_limit_window,
            max_monthly_calls=100000  # 100k/month plan
        )
        self.rate_limiter = RateLimiter(config=rate_limit_config)
        self.session = aiohttp.ClientSession(headers=self.headers)

    async def get_user_metrics(self, username: str) -> Optional[Dict]:
        """
        Get user metrics for a Twitter account.

        Args:
            username (str): Twitter username without @ symbol

        Returns:
            Optional[Dict]: User metrics including followers, following, etc.
        """
        try:
            await self.rate_limiter.acquire()

            url = f"https://{self.api_host}/user/details"
            params = {'username': username}

            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Twitter API error: {response.status} for user {username}")
                    return None

                data = await response.json()
                if not data.get('data'):
                    return None

                user_data = data['data']
                return {
                    'followers': user_data.get('public_metrics', {}).get('followers_count', 0),
                    'following': user_data.get('public_metrics', {}).get('following_count', 0),
                }

        except Exception as e:
            logger.error(f"Error fetching Twitter user metrics: {e}")
            return None

    async def get_recent_tweets(self, username: str, hours: int = 24) -> Optional[Dict]:
        """
        Get recent tweets and their metrics.

        Args:
            username (str): Twitter username without @ symbol
            hours (int): Hours of tweets to fetch

        Returns:
            Optional[Dict]: Tweet metrics including count and engagement
        """
        try:
            await self.rate_limiter.acquire()

            url = f"https://{self.api_host}/user/tweets"
            params = {
                'username': username,
                'limit': 100  # Maximum allowed by API
            }

            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Twitter API error: {response.status} for user {username}")
                    return None

                data = await response.json()
                if not data.get('data'):
                    return None

                tweets = data['data']
                recent_tweets = [
                    tweet for tweet in tweets
                    if self._is_within_hours(tweet.get('created_at', ''), hours)
                ]

                # Calculate engagement metrics
                total_likes = sum(tweet.get('public_metrics', {}).get('like_count', 0) for tweet in recent_tweets)
                total_retweets = sum(tweet.get('public_metrics', {}).get('retweet_count', 0) for tweet in recent_tweets)
                total_replies = sum(tweet.get('public_metrics', {}).get('reply_count', 0) for tweet in recent_tweets)

                total_engagement = total_likes + total_retweets + total_replies
                engagement_rate = Decimal(str(total_engagement / len(recent_tweets))) if recent_tweets else Decimal('0')

                return {
                    'posts_24h': len(recent_tweets),
                    'engagement_rate': engagement_rate,
                    'texts': [tweet.get('text', '') for tweet in recent_tweets]
                }

        except Exception as e:
            logger.error(f"Error fetching Twitter tweets: {e}")
            return None

    def _is_within_hours(self, created_at: str, hours: int) -> bool:
        """Check if a tweet is within the specified hours"""
        from datetime import datetime, timezone
        from dateutil import parser

        try:
            tweet_time = parser.parse(created_at)
            now = datetime.now(timezone.utc)
            age = now - tweet_time
            return age.total_seconds() <= hours * 3600
        except Exception:
            return False

    async def cleanup(self):
        """Clean up resources"""
        if self.session and not self.session.closed:
            await self.session.close()
