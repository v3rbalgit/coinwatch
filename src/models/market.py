# src/models/market.py

from datetime import datetime
from typing import Optional
from sqlalchemy import Float, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class TokenMarketMetrics(Base):
    """Token market metrics"""
    __tablename__ = 'token_market_metrics'

    # Coingecko identification
    id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    # Price metrics
    current_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    fully_diluted_valuation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_volume: Mapped[float] = mapped_column(Float, nullable=False)
    high_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    low_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_change_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_change_percentage_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Market capitalization metrics
    market_cap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_cap_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    market_cap_change_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_cap_change_percentage_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Supply metrics
    circulating_supply: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_supply: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    max_supply: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # All time high and low metrics
    ath: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ath_change_percentage: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ath_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    atl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    atl_change_percentage: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    atl_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Market metrics management
    updated_at: Mapped[datetime] = mapped_column(nullable=False)
    data_source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"TokenMarketMetrics(id={self.id}, symbol='{self.symbol}')"