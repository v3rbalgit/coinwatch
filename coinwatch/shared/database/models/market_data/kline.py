from datetime import datetime
from sqlalchemy import Index, BigInteger, Numeric, PrimaryKeyConstraint, Text, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..base import MarketDataBase
from .symbol import Symbol

class Kline(MarketDataBase):
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
        ForeignKey('market_data.symbols.id', ondelete='CASCADE'),
        nullable=False
    )

    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        comment='Candlestick timestamp in UTC'
    )

    interval: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment='Time interval of the kline (e.g., "1m", "5m", "1h")'
    )

    # Price fields use precision=18, scale=8 for exact decimal arithmetic
    open_price: Mapped[float] = mapped_column(Numeric(precision=18, scale=8), nullable=False)
    high_price: Mapped[float] = mapped_column(Numeric(precision=18, scale=8), nullable=False)
    low_price: Mapped[float] = mapped_column(Numeric(precision=18, scale=8), nullable=False)
    close_price: Mapped[float] = mapped_column(Numeric(precision=18, scale=8), nullable=False)

    # Volume fields use precision=36, scale=8 to handle large numbers while maintaining precision
    volume: Mapped[float] = mapped_column(Numeric(precision=36, scale=8), nullable=False)
    turnover: Mapped[float] = mapped_column(Numeric(precision=36, scale=8), nullable=False)

    # Relationships
    symbol: Mapped[Symbol] = relationship('Symbol', back_populates='klines')

    __table_args__ = (
        PrimaryKeyConstraint('id', 'timestamp'),
        UniqueConstraint('symbol_id', 'interval', 'timestamp', name='uix_kline_symbol_interval_time'),
        Index(
            'idx_kline_query',
            'symbol_id', 'interval', 'timestamp',
            postgresql_using='btree'
        ),
        {'comment': 'Time-series price data for trading pairs'}
    )

    def __repr__(self) -> str:
        return (
            f"Kline(id={self.id}, symbol_id={self.symbol_id}, "
            f"timestamp={self.timestamp}, interval='{self.interval}')"
        )
