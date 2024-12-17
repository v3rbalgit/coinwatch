# src/services/fundamental_data/sentiment_metrics_collector.py

from decimal import Decimal
from typing import Dict, List, Optional
from textblob import TextBlob

from .collector import FundamentalCollector
from shared.clients.sentiment import RedditAdapter, TelegramAdapter, TwitterAdapter
from shared.core.config import SentimentConfig
from shared.core.enums import DataSource
from shared.core.exceptions import ServiceError
from shared.core.models import SentimentMetrics, Metadata, SymbolInfo
from shared.database.repositories import MetadataRepository, SentimentRepository
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils

logger = LoggerSetup.setup(__name__)

class SentimentMetricsCollector(FundamentalCollector):
    """
    Collector for social media sentiment metrics.

    Collects:
    - Social media engagement metrics (followers, posts, etc.)
    - Sentiment analysis of recent posts/comments
    - Aggregated sentiment and social scores
    """

    def __init__(self,
                 sentiment_repository: SentimentRepository,
                 metadata_repository: MetadataRepository,
                 sentiment_config: SentimentConfig,
                 collection_interval: int = 3600  # Default 1 hour
                ):
        """
        Initialize the SentimentMetricsCollector.

        Args:
            sentiment_repository (SentimentRepository): Repository for storing sentiment metrics.
            metadata_repository (MetadataRepository): Repository for accessing token metadata.
            sentiment_config (SentimentConfig): Configuration for social media API interactions.
            collection_interval (int): Time interval between collections in seconds.
        """
        super().__init__(collection_interval)
        self.sentiment_repository = sentiment_repository
        self.metadata_repository = metadata_repository

        # Initialize adapters using config helper methods
        self.twitter = TwitterAdapter(**sentiment_config.get_twitter_config())
        self.reddit = RedditAdapter(**sentiment_config.get_reddit_config())
        self.telegram = TelegramAdapter(**sentiment_config.get_telegram_config())

    @property
    def collector_type(self) -> str:
        """
        Get the type of this collector.

        Returns:
            str: The collector type ("sentiment_metrics").
        """
        return "sentiment_metrics"

    def _analyze_sentiment(self, texts: List[str]) -> Optional[Decimal]:
        """
        Calculate average sentiment polarity score for a list of texts.

        Uses TextBlob to perform sentiment analysis on each text and returns
        the average polarity score across all texts.

        Args:
            texts (List[str]): List of text content to analyze.

        Returns:
            Optional[Decimal]: Average sentiment score between -1 and 1, or None if no texts provided.
        """
        if not texts:
            return None

        sentiment_scores = []
        for text in texts:
            analysis = TextBlob(text)
            sentiment_scores.append(getattr(analysis.sentiment,'polarity'))

        return Decimal(str(sum(sentiment_scores) / len(sentiment_scores)))

    async def collect_symbol_data(self, tokens: List[SymbolInfo]) -> List[SentimentMetrics]:
        """
        Collect sentiment metrics for multiple tokens across social media platforms.

        For each token:
        1. Retrieves social media links from metadata
        2. Collects metrics from each platform (Twitter, Reddit, Telegram)
        3. Calculates aggregated sentiment and engagement scores
        4. Combines all metrics into a SentimentMetrics object

        Args:
            tokens (List[SymbolInfo]): List of tokens to collect sentiment data for.

        Returns:
            List[SentimentMetrics]: List of collected sentiment metrics for each token.

        Raises:
            ServiceError: If collection fails due to API or processing errors.
        """
        try:
            # Get metadata for all tokens to access social media links
            metadata_list: List[Metadata] = []
            for token in tokens:
                if metadata := await self.metadata_repository.get_metadata(token):
                    metadata_list.append(metadata)

            if not metadata_list:
                return []

            metrics = []
            for metadata in metadata_list:
                # Collect metrics from each platform
                twitter_metrics = await self._collect_twitter_metrics(metadata) if metadata.twitter else None
                reddit_metrics = await self._collect_reddit_metrics(metadata) if metadata.reddit else None
                telegram_metrics = await self._collect_telegram_metrics(metadata) if metadata.telegram else None

                # Calculate aggregated scores
                platform_scores = []
                engagement_scores = []

                if twitter_metrics:
                    if twitter_metrics['sentiment_score'] is not None:
                        platform_scores.append(twitter_metrics['sentiment_score'])
                    if twitter_metrics['engagement_rate'] is not None:
                        engagement_scores.append(twitter_metrics['engagement_rate'])

                if reddit_metrics:
                    if reddit_metrics['sentiment_score'] is not None:
                        platform_scores.append(reddit_metrics['sentiment_score'])
                    if reddit_metrics.get('active_users') and reddit_metrics.get('subscribers'):
                        engagement = Decimal(reddit_metrics['active_users']) / Decimal(reddit_metrics['subscribers'])
                        engagement_scores.append(engagement)

                if telegram_metrics:
                    if telegram_metrics['sentiment_score'] is not None:
                        platform_scores.append(telegram_metrics['sentiment_score'])
                    if telegram_metrics.get('online_members') and telegram_metrics.get('members'):
                        engagement = Decimal(telegram_metrics['online_members']) / Decimal(telegram_metrics['members'])
                        engagement_scores.append(engagement)

                # Calculate weighted averages
                overall_sentiment = Decimal(
                    sum(platform_scores) / len(platform_scores)
                    if platform_scores else Decimal('0')
                )
                social_score = Decimal(
                    sum(engagement_scores) / len(engagement_scores)
                    if engagement_scores else Decimal('0')
                )

                metrics.append(SentimentMetrics(
                    id=metadata.id,
                    symbol=metadata.symbol,
                    # Twitter metrics
                    twitter_followers=twitter_metrics['followers'] if twitter_metrics else None,
                    twitter_following=twitter_metrics['following'] if twitter_metrics else None,
                    twitter_posts_24h=twitter_metrics['posts_24h'] if twitter_metrics else None,
                    twitter_engagement_rate=twitter_metrics['engagement_rate'] if twitter_metrics else None,
                    twitter_sentiment_score=twitter_metrics['sentiment_score'] if twitter_metrics else None,
                    # Reddit metrics
                    reddit_subscribers=reddit_metrics['subscribers'] if reddit_metrics else None,
                    reddit_active_users=reddit_metrics['active_users'] if reddit_metrics else None,
                    reddit_posts_24h=reddit_metrics['posts_24h'] if reddit_metrics else None,
                    reddit_comments_24h=reddit_metrics['comments_24h'] if reddit_metrics else None,
                    reddit_sentiment_score=reddit_metrics['sentiment_score'] if reddit_metrics else None,
                    # Telegram metrics
                    telegram_members=telegram_metrics['members'] if telegram_metrics else None,
                    telegram_online_members=telegram_metrics['online_members'] if telegram_metrics else None,
                    telegram_messages_24h=telegram_metrics['messages_24h'] if telegram_metrics else None,
                    telegram_sentiment_score=telegram_metrics['sentiment_score'] if telegram_metrics else None,
                    # Aggregated metrics
                    overall_sentiment_score=overall_sentiment,
                    social_score=social_score,
                    updated_at=TimeUtils.get_current_timestamp(),
                    data_source=DataSource.INTERNAL
                ))

            return metrics

        except Exception as e:
            logger.error(f"Error collecting sentiment metrics: {e}")
            raise ServiceError(f"Sentiment metrics collection failed: {str(e)}")

    async def _collect_twitter_metrics(self, metadata: Metadata) -> Optional[Dict]:
        """
        Collect metrics from Twitter for a given token.

        Retrieves:
        - User metrics (followers, following)
        - Recent tweet metrics (post count, engagement rate)
        - Sentiment analysis of tweet content

        Args:
            metadata (Metadata): Token metadata containing Twitter handle.

        Returns:
            Optional[Dict]: Dictionary of Twitter metrics or None if collection fails.
        """
        try:
            if not metadata.twitter:
                return None

            # Get user metrics
            username = metadata.twitter.split('/')[-1].lstrip('@')
            user_metrics = await self.twitter.get_user_metrics(username)
            if not user_metrics:
                return None

            # Get recent tweets and calculate sentiment
            tweet_metrics = await self.twitter.get_recent_tweets(username)
            if not tweet_metrics:
                return None

            return {
                'followers': user_metrics['followers'],
                'following': user_metrics['following'],
                'posts_24h': tweet_metrics['posts_24h'],
                'engagement_rate': tweet_metrics['engagement_rate'],
                'sentiment_score': self._analyze_sentiment(tweet_metrics['texts'])
            }

        except Exception as e:
            logger.error(f"Error collecting Twitter metrics: {e}")
            return None

    async def _collect_reddit_metrics(self, metadata: Metadata) -> Optional[Dict]:
        """
        Collect metrics from Reddit for a given token.

        Retrieves:
        - Subreddit metrics (subscribers, active users)
        - Recent activity metrics (posts, comments)
        - Sentiment analysis of post/comment content

        Args:
            metadata (Metadata): Token metadata containing subreddit URL.

        Returns:
            Optional[Dict]: Dictionary of Reddit metrics or None if collection fails.
        """
        try:
            if not metadata.reddit:
                return None

            # Get subreddit metrics
            metrics = await self.reddit.get_subreddit_metrics(metadata.reddit)
            if not metrics:
                return None

            # Get recent activity and calculate sentiment
            activity = await self.reddit.get_recent_activity(metadata.reddit)
            if not activity:
                return None

            return {
                'subscribers': metrics['subscribers'],
                'active_users': metrics['active_users'],
                'posts_24h': activity['posts_24h'],
                'comments_24h': activity['comments_24h'],
                'sentiment_score': self._analyze_sentiment(activity['texts'])
            }

        except Exception as e:
            logger.error(f"Error collecting Reddit metrics: {e}")
            return None

    async def _collect_telegram_metrics(self, metadata: Metadata) -> Optional[Dict]:
        """
        Collect metrics from Telegram for a given token.

        Retrieves:
        - Channel metrics (total members, online members)
        - Recent message metrics (message count)
        - Sentiment analysis of message content

        Args:
            metadata (Metadata): Token metadata containing Telegram channel ID.

        Returns:
            Optional[Dict]: Dictionary of Telegram metrics or None if collection fails.
        """
        try:
            if not metadata.telegram:
                return None

            # Get channel metrics
            metrics = await self.telegram.get_channel_metrics(metadata.telegram)
            if not metrics:
                return None

            # Get recent messages and calculate sentiment
            messages = await self.telegram.get_recent_messages(metadata.telegram)
            if not messages:
                return None

            return {
                'members': metrics['members'],
                'online_members': metrics['online_members'],
                'messages_24h': messages['messages_24h'],
                'sentiment_score': self._analyze_sentiment(messages['texts'])
            }

        except Exception as e:
            logger.error(f"Error collecting Telegram metrics: {e}")
            return None

    async def store_symbol_data(self, data: List[SentimentMetrics]) -> None:
        """
        Store collected sentiment metrics for multiple tokens.

        Args:
            data (List[SentimentMetrics]): List of sentiment metrics to be stored.
        """
        await self.sentiment_repository.upsert_sentiment_metrics(data)

    async def delete_symbol_data(self, token: str) -> None:
        """
        Delete sentiment metrics for a specific token.

        Args:
            token (str): The token symbol for which to delete sentiment metrics.
        """
        await self.sentiment_repository.delete_sentiment_metrics(token)

    async def cleanup(self) -> None:
        """Clean up resources"""
        await super().cleanup()
