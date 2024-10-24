# src/models/kline.py

from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, Float, ForeignKey, UniqueConstraint, PrimaryKeyConstraint
from models.base import Base
from models.symbol import Symbol

class Kline(Base):
    __tablename__ = 'kline_data'

    # Change primary key to match partitioned table
    id: Mapped[int] = mapped_column(BigInteger, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey('symbols.id'), nullable=False)
    start_time: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Establishing relationship to Symbol
    symbol: Mapped[Symbol] = relationship('Symbol', back_populates='klines')

    open_price: Mapped[float] = mapped_column(Float, nullable=False)
    high_price: Mapped[float] = mapped_column(Float, nullable=False)
    low_price: Mapped[float] = mapped_column(Float, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    turnover: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        # Define composite primary key
        PrimaryKeyConstraint('id', 'start_time'),
        # Keep the unique constraint for data integrity
        UniqueConstraint('symbol_id', 'start_time', name='uix_symbol_id_start_time'),
        # Add index for efficient querying
        {'mysql_partition_by': 'RANGE(start_time)'}
    )