from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from shared.core.models import MarketMetricsModel
from shared.database.connection import DatabaseConnection
from shared.database.models.fundamental_data import TokenMarketMetrics
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup



class MarketMetricsRepository:
    """Repository for token market metrics operations"""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service
        self.logger = LoggerSetup.setup(__class__.__name__)


    async def upsert_market_metrics(self, metrics_set: list[MarketMetricsModel]) -> None:
        """
        Create or update market metrics for multiple symbols.

        Args:
            metrics_set (List[MarketMetrics]): Set of market metrics to upsert.

        Raises:
            RepositoryError: If the upsert operation fails.
        """
        try:
            async with self.db_service.session() as session:
                # Convert all metrics to database format
                db_data = [metrics.model_dump() for metrics in metrics_set]

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

                self.logger.debug(f"Upserted {len(metrics_set)} market metrics records into database")

        except Exception as e:
            self.logger.error(f"Error in bulk market metrics upsert: {e}")
            raise RepositoryError(f"Failed to upsert market metrics: {str(e)}")


    async def get_market_metrics(self, tokens: set[str]) -> list[MarketMetricsModel]:
        """
        Get stored market metrics for multiple tokens.

        Args:
            tokens (set[str]): Set of token symbols to get market metrics for.

        Returns:
            list[MarketMetrics]: List of market metrics for found tokens. Empty list if none found.

        Raises:
            RepositoryError: If the retrieval operation fails.
        """
        try:
            async with self.db_service.session() as session:
                stmt = select(TokenMarketMetrics).where(
                    TokenMarketMetrics.symbol.in_(tokens)
                )
                result = await session.execute(stmt)
                db_metrics_list = result.scalars().all()

                return [
                    MarketMetricsModel.model_validate(metrics.__dict__)
                    for metrics in db_metrics_list
                ]

        except Exception as e:
            self.logger.error(f"Error getting market metrics for {len(tokens)} tokens: {e}")
            raise RepositoryError(f"Failed to get market metrics: {str(e)}")


    async def delete_market_metrics(self, tokens: set[str]) -> None:
        """
        Delete market metrics for multiple tokens.

        Args:
            tokens (set[str]): Set of tokens to delete market metrics for.

        Raises:
            RepositoryError: If the deletion operation fails.
        """
        try:
            async with self.db_service.session() as session:
                stmt = select(TokenMarketMetrics).where(
                    TokenMarketMetrics.symbol.in_(tokens)
                )
                result = await session.execute(stmt)
                records = result.scalars().all()

                if records:
                    for record in records:
                        await session.delete(record)
                    await session.flush()
                    self.logger.info(f"Deleted market metrics for {len(records)} tokens")
                else:
                    self.logger.warning(f"No market metrics found for tokens: {tokens}")

        except Exception as e:
            self.logger.error(f"Error deleting market metrics for {len(tokens)} tokens: {e}")
            raise RepositoryError(f"Failed to delete market metrics: {str(e)}")
