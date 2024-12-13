from datetime import datetime
from decimal import Decimal
from sqlalchemy import Text, BigInteger, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from ..base import FundamentalDataBase

class TokenSentiment(FundamentalDataBase):
    """Token sentiment metrics from social media analysis"""
    __tablename__ = 'token_sentiment'

    # Token identification (matches metadata)
    id: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)

    # Twitter metrics
    twitter_followers: Mapped[int] = mapped_column(BigInteger, nullable=True)
    twitter_following: Mapped[int] = mapped_column(BigInteger, nullable=True)
    twitter_posts_24h: Mapped[int] = mapped_column(BigInteger, nullable=True)
    twitter_engagement_rate: Mapped[Decimal] = mapped_column(Numeric, nullable=True)
    twitter_sentiment_score: Mapped[Decimal] = mapped_column(Numeric, nullable=True)

    # Reddit metrics
    reddit_subscribers: Mapped[int] = mapped_column(BigInteger, nullable=True)
    reddit_active_users: Mapped[int] = mapped_column(BigInteger, nullable=True)
    reddit_posts_24h: Mapped[int] = mapped_column(BigInteger, nullable=True)
    reddit_comments_24h: Mapped[int] = mapped_column(BigInteger, nullable=True)
    reddit_sentiment_score: Mapped[Decimal] = mapped_column(Numeric, nullable=True)

    # Telegram metrics
    telegram_members: Mapped[int] = mapped_column(BigInteger, nullable=True)
    telegram_online_members: Mapped[int] = mapped_column(BigInteger, nullable=True)
    telegram_messages_24h: Mapped[int] = mapped_column(BigInteger, nullable=True)
    telegram_sentiment_score: Mapped[Decimal] = mapped_column(Numeric, nullable=True)

    # Aggregated sentiment
    overall_sentiment_score: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    social_score: Mapped[Decimal] = mapped_column(Numeric, nullable=False)  # Weighted engagement score

    # Metadata
    updated_at: Mapped[datetime] = mapped_column(nullable=False)
    data_source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"TokenSentiment(id={self.id}, overall_sentiment={self.overall_sentiment_score}, social_score={self.social_score})"
