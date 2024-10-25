# src/models/kline.py

from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, Float, Index, UniqueConstraint, ForeignKey, PrimaryKeyConstraint, text
from src.models.base import Base

class Kline(Base):
    __tablename__ = 'kline_data'

    id: Mapped[int] = mapped_column(BigInteger, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('symbols.id'), nullable=False)
    start_time: Mapped[int] = mapped_column(BigInteger, nullable=False)

    symbol = relationship(
        'Symbol',
        back_populates='klines'
    )

    open_price: Mapped[float] = mapped_column(Float, nullable=False)
    high_price: Mapped[float] = mapped_column(Float, nullable=False)
    low_price: Mapped[float] = mapped_column(Float, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    turnover: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('id', 'start_time'),
        UniqueConstraint('symbol_id', 'start_time', name='uix_symbol_id_start_time'),
        Index('idx_symbol_time', 'symbol_id', 'start_time'),
        {
            'mysql_engine': 'InnoDB',
            'mysql_default_charset': 'utf8mb4',
            'mysql_collate': 'utf8mb4_unicode_ci',
            'mysql_row_format': 'DYNAMIC'
        }
    )