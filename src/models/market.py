# src/models/market.py

from typing import List
from sqlalchemy import Index, BigInteger, Float, PrimaryKeyConstraint, String, Text, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from .base import Base

class Symbol(Base):
    """
    Symbol model for database - regular PostgreSQL table (not TimescaleDB)
    Needs primary key and unique constraints for data integrity
    """
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    first_trade_time: Mapped[int] = mapped_column(
        BigInteger,
        nullable=True,
        comment='First trading time for the symbol in ms'
    )

    __table_args__ = (
        UniqueConstraint('name', 'exchange', name='uix_symbol_exchange'),
        Index('idx_symbol_lookup', 'name', 'exchange')
    )

    klines: Mapped[List['Kline']] = relationship(
        'Kline',
        back_populates='symbol',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return f"Symbol(id={self.id}, name='{self.name}', exchange='{self.exchange}')"

class Kline(Base):
    """
    Kline (candlestick) data model - TimescaleDB hypertable
    No primary key or unique constraints needed for TimescaleDB optimization
    """
    __tablename__ = 'kline_data'

    id: Mapped[int] = mapped_column(
        BigInteger,
        autoincrement=True,
        nullable=False
    )

    symbol_id: Mapped[int] = mapped_column(
        ForeignKey('symbols.id', ondelete='CASCADE'),
        nullable=False
    )

    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        comment='Candlestick timestamp in UTC'
    )

    timeframe: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment='Timeframe of the kline (e.g., "1m", "5m", "1h")'
    )

    open_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    high_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    low_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    close_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    volume: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    turnover: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)

    symbol: Mapped[Symbol] = relationship('Symbol', back_populates='klines')

    __table_args__ = (
        PrimaryKeyConstraint('id', 'timestamp'),
        Index(
            'idx_kline_query',
            'symbol_id', 'timeframe', 'timestamp',
            postgresql_using='btree'
        ),
        {'comment': 'Time-series price data for trading pairs'}
    )

    def __repr__(self) -> str:
        return (
            f"Kline(id={self.id}, symbol_id={self.symbol_id}, "
            f"timestamp={self.timestamp}, timeframe='{self.timeframe}')"
        )