# src/models/platform.py

from typing import TYPE_CHECKING
from sqlalchemy import ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .metadata import TokenMetadata


class TokenPlatform(Base):
    """Platform information for tokens"""
    __tablename__ = 'token_platforms'

    token_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('token_metadata.id', ondelete='CASCADE'),
        primary_key=True
    )
    platform_id: Mapped[str] = mapped_column(Text, primary_key=True)  # e.g., 'ethereum', 'polygon-pos'
    contract_address: Mapped[str] = mapped_column(Text, nullable=False)

    # Relationship back to metadata
    token: Mapped["TokenMetadata"] = relationship("TokenMetadata", back_populates="platforms")

    def __repr__(self) -> str:
        return f"TokenPlatform(platform_id={self.platform_id}, contract_address='{self.contract_address}'"
