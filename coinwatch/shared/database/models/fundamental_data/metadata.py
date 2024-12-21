from datetime import datetime
from sqlalchemy import Text, BigInteger, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB

from ..base import FundamentalDataBase
from .platform import TokenPlatform

class TokenMetadata(FundamentalDataBase):
    """Token metadata information"""
    __tablename__ = 'token_metadata'

    # Coingecko identification
    id: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)

    # Core information
    description: Mapped[str] = mapped_column(Text, nullable=False)
    categories: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    launch_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    market_cap_rank: Mapped[int] = mapped_column(BigInteger, nullable=False)
    hashing_algorithm: Mapped[str | None ] = mapped_column(Text, nullable=True)

    # Images
    image_thumb: Mapped[str] = mapped_column(Text, nullable=False)
    image_small: Mapped[str] = mapped_column(Text, nullable=False)
    image_large: Mapped[str] = mapped_column(Text, nullable=False)

    # Links
    website: Mapped[str | None ] = mapped_column(Text, nullable=True)
    whitepaper: Mapped[str | None ] = mapped_column(Text, nullable=True)
    reddit: Mapped[str | None ] = mapped_column(Text, nullable=True)
    twitter: Mapped[str | None ] = mapped_column(Text, nullable=True)
    telegram: Mapped[str | None ] = mapped_column(Text, nullable=True)
    github: Mapped[str | None ] = mapped_column(Text, nullable=True)

    # Platform info (for tokens)
    platforms: Mapped[list[TokenPlatform]] = relationship(
        "TokenPlatform",
        back_populates="token"
    )

    # Metadata management
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    data_source: Mapped[str] = mapped_column(Text, default='coingecko')

    def __repr__(self) -> str:
        return f"TokenMetadata(id={self.id}, symbol='{self.symbol}', name='{self.name}')"
