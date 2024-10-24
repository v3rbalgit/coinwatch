# src/models/checkpoint.py

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, BigInteger
from models.base import Base

class Checkpoint(Base):
    __tablename__ = 'checkpoints'

    symbol: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
