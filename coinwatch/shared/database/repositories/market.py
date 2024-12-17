# src/repositories/market.py

from decimal import Decimal
from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from shared.core.enums import DataSource, IsolationLevel
from shared.core.models import SymbolInfo, MarketMetrics
from shared.database.connection import DatabaseConnection
from shared.database.models.fundamental_data import TokenMarketMetrics
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils

logger = LoggerSetup.setup(__name__)

class MarketMetricsRepository:
    """Repository for token market metrics operations"""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service

    async def upsert_market_metrics(self, metrics_list: List[MarketMetrics]) -> None:
        """
        Create or update market metrics for multiple symbols.

        Args:
            metrics_list (List[MarketMetrics]): List of market metrics to upsert.

        Raises:
            RepositoryError: If the upsert operation fails.
        """
        try:
            async with self.db_service.session(
                isolation_level=IsolationLevel.REPEATABLE_READ
            ) as session:
                # Convert all metrics to database format
                db_data = [metrics.to_dict() for metrics in metrics_list]

                stmt = insert(TokenMarketMetrics).values(db_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        col.name: stmt.excluded[col.name]
                        for col in TokenMarketMetrics.__table__.columns
                        if col.name != 'id'
                    }
                )
                await session.execute(stmt)

                logger.debug(f"Updated market metrics for {len(metrics_list)} tokens")

        except Exception as e:
            logger.error(f"Error in bulk market metrics upsert: {e}")
            raise RepositoryError(f"Failed to upsert market metrics batch: {str(e)}")

    async def get_market_metrics(self, symbol: SymbolInfo) -> Optional[MarketMetrics]:
        """
        Get stored market metrics for a symbol.

        Args:
            symbol (SymbolInfo): The symbol to retrieve market metrics for.

        Returns:
            Optional[MarketMetrics]: The market metrics for the symbol, or None if not found.

        Raises:
            RepositoryError: If the retrieval operation fails.
        """
        try:
            async with self.db_service.session() as session:
                stmt = select(TokenMarketMetrics).where(
                    TokenMarketMetrics.symbol == symbol.name.lower()
                )
                result = await session.execute(stmt)
                db_metrics = result.scalar_one_or_none()

                if db_metrics:
                    return MarketMetrics(
                        id=db_metrics.id,
                        symbol=symbol,
                        current_price=Decimal(str(db_metrics.current_price)),
                        market_cap=Decimal(str(db_metrics.market_cap)) if db_metrics.market_cap else None,
                        market_cap_rank=db_metrics.market_cap_rank,
                        fully_diluted_valuation=Decimal(str(db_metrics.fully_diluted_valuation)) if db_metrics.fully_diluted_valuation else None,
                        total_volume=Decimal(str(db_metrics.total_volume)),
                        high_24h=Decimal(str(db_metrics.high_24h)) if db_metrics.high_24h else None,
                        low_24h=Decimal(str(db_metrics.low_24h)) if db_metrics.low_24h else None,
                        price_change_24h=Decimal(str(db_metrics.price_change_24h)) if db_metrics.price_change_24h else None,
                        price_change_percentage_24h=Decimal(str(db_metrics.price_change_percentage_24h)) if db_metrics.price_change_percentage_24h else None,
                        market_cap_change_24h=Decimal(str(db_metrics.market_cap_change_24h)) if db_metrics.market_cap_change_24h else None,
                        market_cap_change_percentage_24h=Decimal(str(db_metrics.market_cap_change_percentage_24h)) if db_metrics.market_cap_change_percentage_24h else None,
                        circulating_supply=Decimal(str(db_metrics.circulating_supply)) if db_metrics.circulating_supply else None,
                        total_supply=Decimal(str(db_metrics.total_supply)) if db_metrics.total_supply else None,
                        max_supply=Decimal(str(db_metrics.max_supply)) if db_metrics.max_supply else None,
                        ath=Decimal(str(db_metrics.ath)) if db_metrics.ath else None,
                        ath_change_percentage=Decimal(str(db_metrics.ath_change_percentage)) if db_metrics.ath_change_percentage else None,
                        ath_date=db_metrics.ath_date,
                        atl=Decimal(str(db_metrics.atl)) if db_metrics.atl else None,
                        atl_change_percentage=Decimal(str(db_metrics.atl_change_percentage)) if db_metrics.atl_change_percentage else None,
                        atl_date=db_metrics.atl_date,
                        updated_at=TimeUtils.to_timestamp(db_metrics.updated_at),
                        data_source=DataSource(db_metrics.data_source)
                    )
                return None

        except Exception as e:
            logger.error(f"Error getting market metrics for {symbol}: {e}")
            raise RepositoryError(f"Failed to get market metrics: {str(e)}")

    async def delete_market_metrics(self, symbol: str) -> None:
        """
        Delete market metrics for a token.

        Args:
            symbol (str): Symbol representing the token to delete market metrics for.

        Raises:
            RepositoryError: If the deletion operation fails.
        """
        try:
            async with self.db_service.session() as session:

                stmt = select(TokenMarketMetrics).where(
                    TokenMarketMetrics.symbol == symbol
                )
                result = await session.execute(stmt)
                market_metrics_record = result.scalar_one_or_none()

                if market_metrics_record:
                    await session.delete(market_metrics_record)
                    logger.info(f"Deleted market metrics for symbol '{symbol.upper()}'")
                else:
                    logger.warning(f"No market metrics found for symbol '{symbol.upper()}'")

        except Exception as e:
            logger.error(f"Error deleting market metrics for {symbol}: {e}")
            raise RepositoryError(f"Failed to delete market metrics: {str(e)}")