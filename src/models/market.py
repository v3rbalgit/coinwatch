# src/models/market.py

from typing import List
from sqlalchemy import BigInteger, Float, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Symbol(Base):
    """Symbol model for database"""
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    exchange: Mapped[str] = mapped_column(String(50), index=True)
    first_trade_time: Mapped[int] = mapped_column(BigInteger, nullable=True)

    # Define the relationship to Kline
    klines: Mapped[List['Kline']] = relationship(
        'Kline',
        back_populates='symbol',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return f"Symbol(id={self.id}, name='{self.name}', exchange='{self.exchange}')"


class Kline(Base):
    """Kline (candlestick) data model"""
    __tablename__ = 'kline_data'

    # Add unique constraint for symbol_id + timeframe + start_time combination
    __table_args__ = (
        UniqueConstraint('symbol_id', 'timeframe', 'start_time',
                        name='uix_kline_symbol_timeframe_start'),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol_id: Mapped[int] = mapped_column(
        ForeignKey('symbols.id', ondelete='CASCADE'),
        index=True
    )
    start_time: Mapped[int] = mapped_column(BigInteger, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), index=True)

    # Price and volume fields
    open_price: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))
    high_price: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))
    low_price: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))
    close_price: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))
    volume: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))
    turnover: Mapped[float] = mapped_column(Float(precision=10, decimal_return_scale=8))

    # Define the relationship back to Symbol
    symbol: Mapped['Symbol'] = relationship('Symbol', back_populates='klines')

    def __repr__(self) -> str:
        return (
            f"Kline(id={self.id}, symbol_id={self.symbol_id}, "
            f"start_time={self.start_time}, timeframe='{self.timeframe}')"
        )