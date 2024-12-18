from datetime import datetime
from sqlalchemy import Float, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column

from ..base import FundamentalDataBase

class TokenMarketMetrics(FundamentalDataBase):
    """Token market metrics"""
    __tablename__ = 'token_market_metrics'

    # Coingecko identification
    id: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)

    # Price metrics
    current_price: Mapped[float] = mapped_column(Float(precision=18, decimal_return_scale=8), nullable=False)
    fully_diluted_valuation: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_volume: Mapped[float] = mapped_column(Float, nullable=False)
    high_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    low_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_change_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_change_percentage_24h: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Market capitalization metrics
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    market_cap_change_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap_change_percentage_24h: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Supply metrics
    circulating_supply: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_supply: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_supply: Mapped[float | None] = mapped_column(Float, nullable=True)

    # All time high and low metrics
    ath: Mapped[float | None] = mapped_column(Float, nullable=True)
    ath_change_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    ath_date: Mapped[str | None] = mapped_column(Text, nullable=True)
    atl: Mapped[float | None] = mapped_column(Float, nullable=True)
    atl_change_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    atl_date: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Market metrics management
    updated_at: Mapped[datetime] = mapped_column(nullable=False)
    data_source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"TokenMarketMetrics(id={self.id}, symbol='{self.symbol}')"
