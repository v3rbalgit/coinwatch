# src/models/symbol.py

from __future__ import annotations

from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String
from models.base import Base
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from models.kline import Kline  # Import Kline only for type checking

class Symbol(Base):
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    # Establishing relationship to Kline
    klines: Mapped[List['Kline']] = relationship('Kline', back_populates='symbol', cascade='all, delete-orphan')
