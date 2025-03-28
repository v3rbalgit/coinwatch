from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from shared.core.enums import DataSource, IsolationLevel
from shared.database.models import TokenSentiment
from shared.core.models import SentimentMetricsModel
from shared.core.exceptions import RepositoryError
from shared.database.connection import DatabaseConnection
from shared.utils.logger import LoggerSetup



class SentimentRepository:
    """Repository for managing token sentiment metrics in the database"""

    def __init__(self, db_service: DatabaseConnection):
        """
        Initialize the repository with database service.

        Args:
            db_service (DatabaseService): Service for database operations
        """
        self.db = db_service
        self.logger = LoggerSetup.setup(__class__.__name__)


    async def get_sentiment_metrics(self, tokens: set[str]) -> list[SentimentMetricsModel]:
        """
        Get stored sentiment metrics for multiple tokens.

        Args:
            tokens (set[str]): Set of token symbols to get sentiment metrics for.

        Returns:
            list[SentimentMetricsModel]: List of sentiment metrics for found tokens. Empty list if none found.

        Raises:
            RepositoryError: If the retrieval operation fails.
        """
        try:
            async with self.db.session() as session:
                stmt = select(TokenSentiment).where(
                    TokenSentiment.symbol.in_(tokens)
                )
                result = await session.execute(stmt)
                db_metrics_list = result.scalars().all()

                return [SentimentMetricsModel(
                    id=db_metrics.id,
                    symbol=db_metrics.symbol,
                    twitter_followers=db_metrics.twitter_followers,
                    twitter_following=db_metrics.twitter_following,
                    twitter_posts_24h=db_metrics.twitter_posts_24h,
                    twitter_engagement_rate=db_metrics.twitter_engagement_rate,
                    twitter_sentiment_score=db_metrics.twitter_sentiment_score,
                    reddit_subscribers=db_metrics.reddit_subscribers,
                    reddit_active_users=db_metrics.reddit_active_users,
                    reddit_posts_24h=db_metrics.reddit_posts_24h,
                    reddit_comments_24h=db_metrics.reddit_comments_24h,
                    reddit_sentiment_score=db_metrics.reddit_sentiment_score,
                    telegram_members=db_metrics.telegram_members,
                    telegram_online_members=db_metrics.telegram_online_members,
                    telegram_messages_24h=db_metrics.telegram_messages_24h,
                    telegram_sentiment_score=db_metrics.telegram_sentiment_score,
                    overall_sentiment_score=db_metrics.overall_sentiment_score,
                    social_score=db_metrics.social_score,
                    updated_at=db_metrics.updated_at,
                    data_source=DataSource(db_metrics.data_source)
                ) for db_metrics in db_metrics_list]

        except Exception as e:
            self.logger.error(f"Error getting sentiment metrics for {len(tokens)} tokens: {e}")
            raise RepositoryError(f"Failed to get sentiment metrics: {str(e)}")


    async def upsert_sentiment_metrics(self, metrics: list[SentimentMetricsModel]) -> None:
        """
        Create or update sentiment metrics for multiple symbols.

        Args:
            metrics (List[SentimentMetrics]): List of sentiment metrics to upsert.

        Raises:
            RepositoryError: If the upsert operation fails.
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                # Convert all metrics to database format
                db_data = [metrics.model_dump() for metrics in metrics]

                stmt = insert(TokenSentiment).values(db_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        col.name: stmt.excluded[col.name]
                        for col in TokenSentiment.__table__.columns
                        if col.name != 'id'
                    }
                )
                await session.execute(stmt)

                self.logger.debug(f"Updated sentiment metrics for {len(metrics)} tokens")

        except Exception as e:
            self.logger.error(f"Error in bulk sentiment metrics upsert: {e}")
            raise RepositoryError(f"Failed to upsert sentiment metrics batch: {str(e)}")


    async def delete_sentiment_metrics(self, tokens: set[str]) -> None:
        """
        Delete sentiment metrics for multiple tokens.

        Args:
            tokens (set[str]): Set of tokens to delete sentiment metrics for.

        Raises:
            RepositoryError: If the deletion operation fails.
        """
        try:
            async with self.db.session() as session:
                stmt = select(TokenSentiment).where(
                    TokenSentiment.symbol.in_(tokens)
                )
                result = await session.execute(stmt)
                sentiment_records = result.scalars().all()

                if sentiment_records:
                    for record in sentiment_records:
                        await session.delete(record)
                    await session.flush()
                    self.logger.info(f"Deleted sentiment metrics for {len(sentiment_records)} tokens")
                else:
                    self.logger.warning(f"No sentiment metrics found for tokens: {tokens}")

        except Exception as e:
            self.logger.error(f"Error deleting sentiment metrics for {len(tokens)} tokens: {e}")
            raise RepositoryError(f"Failed to delete sentiment metrics: {str(e)}")
