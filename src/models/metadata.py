# src/models/metadata.py

from datetime import datetime
from typing import Optional
from sqlalchemy import Text, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class TokenMetadata(Base):
    """Token metadata information"""
    __tablename__ = 'token_metadata'

    # Coingecko identification
    id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    # Core information
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    launch_time: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    market_cap_rank: Mapped[int] = mapped_column(BigInteger, nullable=False)
    hashing_algorithm: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Images
    image_thumb: Mapped[str] = mapped_column(Text, nullable=False)
    image_small: Mapped[str] = mapped_column(Text, nullable=False)
    image_large: Mapped[str] = mapped_column(Text, nullable=False)

    # Links
    website: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    whitepaper: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reddit: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    twitter: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    telegram: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    github: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Platform info (for tokens)
    platform: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # e.g., Ethereum, BSC
    contract_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Metadata management
    updated_at: Mapped[datetime] = mapped_column(nullable=False)
    data_source: Mapped[str] = mapped_column(Text, default='coingecko')

    def __repr__(self) -> str:
        return f"TokenMetadata(id={self.id}, symbol='{self.symbol}', name='{self.name}')"