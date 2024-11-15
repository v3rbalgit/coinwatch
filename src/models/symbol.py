# src/models/symbol.py

from typing import TYPE_CHECKING, List
from sqlalchemy import Index, BigInteger, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .fundamental import symbol_metadata

if TYPE_CHECKING:
    from .kline import Kline
    from .fundamental import TokenMetadata

class Symbol(Base):
    """
    Symbol model for database - regular PostgreSQL table (not TimescaleDB)
    Needs primary key and unique constraints for data integrity
    """
    __tablename__ = 'symbols'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    first_trade_time: Mapped[int] = mapped_column(
        BigInteger,
        nullable=True,
        comment='First trading time for the symbol in ms'
    )

    __table_args__ = (
        UniqueConstraint('name', 'exchange', name='uix_symbol_exchange'),
        Index('idx_symbol_lookup', 'name', 'exchange')
    )

    # Relationships
    klines: Mapped[List['Kline']] = relationship(
        'Kline',
        back_populates='symbol',
        cascade='all, delete-orphan'
    )
    metadata: Mapped[List["TokenMetadata"]] = relationship(
        "TokenMetadata",
        secondary=symbol_metadata,
        back_populates="symbols"
    )

    def __repr__(self) -> str:
        return f"Symbol(id={self.id}, name='{self.name}', exchange='{self.exchange}')"