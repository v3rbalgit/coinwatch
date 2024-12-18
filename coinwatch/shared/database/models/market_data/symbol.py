from typing import TYPE_CHECKING
from sqlalchemy import Index, BigInteger, Text, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..base import MarketDataBase

if TYPE_CHECKING:
    from .kline import Kline

class Symbol(MarketDataBase):
    """
    Symbol model for database - regular PostgreSQL table (not TimescaleDB)
    Needs primary key and unique constraints for data integrity
    """
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    # Trading parameters
    base_asset: Mapped[str] = mapped_column(Text, nullable=True)
    quote_asset: Mapped[str] = mapped_column(Text, nullable=True)
    price_scale: Mapped[int] = mapped_column(Integer, nullable=True)
    tick_size: Mapped[str] = mapped_column(Text, nullable=True)
    qty_step: Mapped[str] = mapped_column(Text, nullable=True)
    max_qty: Mapped[str] = mapped_column(Text, nullable=True)
    min_notional: Mapped[str] = mapped_column(Text, nullable=True)
    max_leverage: Mapped[str] = mapped_column(Text, nullable=True)
    funding_interval: Mapped[int] = mapped_column(Integer, nullable=True)

    launch_time: Mapped[int] = mapped_column(
        BigInteger,
        nullable=True,
        comment='First trading time for the symbol in ms'
    )

    __table_args__ = (
        UniqueConstraint('name', 'exchange', name='uix_symbol_exchange'),
        Index('idx_symbol_lookup', 'name', 'exchange')
    )

    # Relationships
    klines: Mapped[list['Kline']] = relationship(
        'Kline',
        back_populates='symbol',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return f"Symbol(id={self.id}, name='{self.name}', exchange='{self.exchange}')"
