from datetime import datetime
from decimal import Decimal
from sqlalchemy import Text, Integer, Numeric, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from ..base import FundamentalDataBase

class TokenMarketMetrics(FundamentalDataBase):
    """Token market metrics"""
    __tablename__ = 'token_market_metrics'

    # Coingecko identification (only required fields)
    id: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)

    # Price fields (all nullable)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    high_24h: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    low_24h: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    price_change_24h: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    ath: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    atl: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    # Percentage fields (all nullable)
    price_change_percentage_24h: Mapped[Decimal | None] = mapped_column(Numeric(16, 4), nullable=True)
    market_cap_change_percentage_24h: Mapped[Decimal | None] = mapped_column(Numeric(16, 4), nullable=True)
    ath_change_percentage: Mapped[Decimal | None] = mapped_column(Numeric(16, 4), nullable=True)
    atl_change_percentage: Mapped[Decimal | None] = mapped_column(Numeric(16, 4), nullable=True)

    # Large number fields (all nullable)
    market_cap: Mapped[Decimal | None] = mapped_column(Numeric(30, 2), nullable=True)
    fully_diluted_valuation: Mapped[Decimal | None] = mapped_column(Numeric(30, 2), nullable=True)
    total_volume: Mapped[Decimal | None] = mapped_column(Numeric(30, 2), nullable=True)
    market_cap_change_24h: Mapped[Decimal | None] = mapped_column(Numeric(30, 2), nullable=True)

    # Supply fields (all nullable)
    circulating_supply: Mapped[Decimal | None] = mapped_column(Numeric(30, 8), nullable=True)
    total_supply: Mapped[Decimal | None] = mapped_column(Numeric(30, 8), nullable=True)
    max_supply: Mapped[Decimal | None] = mapped_column(Numeric(30, 8), nullable=True)

    # Other fields (all nullable except required management fields)
    market_cap_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ath_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    atl_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    # Market metrics management (required fields)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    data_source: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"TokenMarketMetrics(id={self.id}, symbol='{self.symbol}')"
