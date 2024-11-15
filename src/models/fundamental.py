# src/models/fundamental.py

from datetime import datetime
from typing import Optional, List
from sqlalchemy import Text, BigInteger, Table, Column, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

# Many-to-many relationship between symbols and metadata
symbol_metadata = Table(
    'symbol_metadata_map',
    Base.metadata,
    Column('symbol_id', BigInteger, ForeignKey('symbols.id', ondelete='CASCADE'), primary_key=True),
    Column('metadata_id', BigInteger, ForeignKey('token_metadata.id', ondelete='CASCADE'), primary_key=True)
)

class TokenMetadata(Base):
    """Token metadata information"""
    __tablename__ = 'token_metadata'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    coingecko_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)

    # Core information
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    genesis_date: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    categories: Mapped[List[str]] = mapped_column(Text, nullable=True)  # Store as JSON array
    hashing_algorithm: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Images
    image_thumb: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    image_small: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    image_large: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Links
    website: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    whitepaper: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    github_repository: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Platform info (for tokens)
    parent_platform: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # e.g., Ethereum, BSC
    contract_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Metadata management
    updated_at: Mapped[datetime] = mapped_column(nullable=False)
    data_source: Mapped[str] = mapped_column(Text, default='coingecko')

    # Relationships
    symbols = relationship("Symbol", secondary=symbol_metadata, back_populates="metadata")

    def __repr__(self) -> str:
        return f"TokenMetadata(id={self.id}, coingecko_id='{self.coingecko_id}', symbol='{self.symbol}')"