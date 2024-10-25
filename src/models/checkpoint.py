# src/models/checkpoint.py

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, BigInteger, Index
from src.models.base import Base

class Checkpoint(Base):
    __tablename__ = 'checkpoints'

    symbol: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Add an index for efficient lookups
    __table_args__ = (
        Index('idx_checkpoint_symbol', 'symbol', unique=True),
    )