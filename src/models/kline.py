# src/models/kline.py
from sqlalchemy import Column, BigInteger, DECIMAL, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from src.models.base import Base


class Kline(Base):
    __tablename__ = 'kline_data'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol_id = Column(BigInteger, ForeignKey('symbols.id'), nullable=False)
    start_time = Column(BigInteger, nullable=False)
    open_price = Column(DECIMAL(50, 10), nullable=False)
    high_price = Column(DECIMAL(50, 10), nullable=False)
    low_price = Column(DECIMAL(50, 10), nullable=False)
    close_price = Column(DECIMAL(50, 10), nullable=False)
    volume = Column(DECIMAL(50, 10), nullable=False)
    turnover = Column(DECIMAL(50, 10), nullable=False)

    symbol = relationship('Symbol')

    __table_args__ = (UniqueConstraint('symbol_id', 'start_time', name='uix_symbol_id_start_time'),)