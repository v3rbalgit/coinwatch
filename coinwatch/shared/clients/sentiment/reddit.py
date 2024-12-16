# src/adapters/reddit.py

from typing import Dict, Optional
import asyncpraw
from datetime import datetime, timezone, timedelta

from shared.utils.logger import LoggerSetup
from shared.utils.rate_limit import RateLimiter

logger = LoggerSetup.setup(__name__)

class RedditAdapter:
    """
    Adapter for Reddit data using asyncpraw.
    Handles rate limiting and data transformation.
    """

    def __init__(self, client_id: str, client_secret: str, rate_limit: int, rate_limit_window: int):
        """
        Initialize the Reddit adapter.

        Args:
            client_id (str): Reddit API client ID
            client_secret (str): Reddit API client secret
            rate_limit (int): Maximum requests per window
            rate_limit_window (int): Time window in seconds
        """
        self.reddit = asyncpraw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent="Coinwatch/1.0"
        )

        self.rate_limiter = RateLimiter(calls_per_window=rate_limit,
                                        window_size=rate_limit_window)

    def _extract_subreddit_name(self, url: str) -> str:
        """Extract subreddit name from URL"""
        parts = url.rstrip('/').split('/')
        for i, part in enumerate(parts):
            if part == 'r':
                return parts[i + 1]
        return ''

    async def get_subreddit_metrics(self, subreddit_url: str) -> Optional[Dict]:
        """
        Get metrics for a subreddit.

        Args:
            subreddit_url (str): Full URL to the subreddit

        Returns:
            Optional[Dict]: Subreddit metrics including subscribers, active users, etc.
        """
        try:
            await self.rate_limiter.acquire()

            subreddit_name = self._extract_subreddit_name(subreddit_url)

            if not subreddit_name:
                return None

            subreddit = await self.reddit.subreddit(subreddit_name)

            # Get basic subreddit info
            await subreddit.load()

            return {
                'subscribers': subreddit.subscribers,
                'active_users': subreddit.active_user_count
            }

        except Exception as e:
            logger.error(f"Error fetching Reddit metrics: {e}")
            return None

    async def get_recent_activity(self, subreddit_url: str, hours: int = 24) -> Optional[Dict]:
        """
        Get recent posts and comments from a subreddit.

        Args:
            subreddit_url (str): Full URL to the subreddit
            hours (int): Hours of activity to fetch

        Returns:
            Optional[Dict]: Activity metrics and content for sentiment analysis
        """
        try:
            await self.rate_limiter.acquire()

            subreddit_name = self._extract_subreddit_name(subreddit_url)
            subreddit = await self.reddit.subreddit(subreddit_name)

            # Calculate cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

            # Get recent posts
            posts = []
            post_texts = []
            total_comments = 0

            async for post in subreddit.new():
                post_time = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
                if post_time < cutoff_time:
                    break

                posts.append(post)
                post_texts.append(f"{post.title} {post.selftext}")
                total_comments += post.num_comments

            return {
                'posts_24h': len(posts),
                'comments_24h': total_comments,
                'texts': post_texts
            }

        except Exception as e:
            logger.error(f"Error fetching Reddit activity: {e}")
            return None

    async def cleanup(self):
        """Clean up resources"""
        await self.reddit.close()
