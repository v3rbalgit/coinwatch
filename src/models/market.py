# src/models/market.py

from typing import List
from sqlalchemy import BigInteger, Float, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Symbol(Base):
    """Port from existing symbol.py"""
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    exchange: Mapped[str] = mapped_column(String(50))  # New field

    klines: Mapped[List['Kline']] = relationship(
        back_populates='symbol',
        cascade='all, delete-orphan'
    )

class Kline(Base):
    """Port from existing kline.py"""
    __tablename__ = 'kline_data'

    # Existing fields
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id'))
    start_time: Mapped[int] = mapped_column(BigInteger)

    # Keep existing price and volume fields
    open_price: Mapped[float] = mapped_column(Float)
    high_price: Mapped[float] = mapped_column(Float)
    low_price: Mapped[float] = mapped_column(Float)
    close_price: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    turnover: Mapped[float] = mapped_column(Float)

    # Add new field for timeframe
    timeframe: Mapped[str] = mapped_column(String(10))