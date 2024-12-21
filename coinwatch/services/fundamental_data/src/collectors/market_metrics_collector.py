from typing import AsyncGenerator

from .collector import FundamentalCollector
from shared.clients.coingecko import CoinGeckoAdapter
from shared.core.models import MarketMetricsModel
from shared.core.exceptions import ServiceError
from shared.database.repositories.market import MarketMetricsRepository
from shared.utils.logger import LoggerSetup


class MarketMetricsCollector(FundamentalCollector):
    """
    Collector for market-related token metrics using CoinGecko data.

    Collects:
    - Price and volume metrics
    - Supply information
    - Market cap and FDV
    - Historical price points (ATH/ATL)
    """

    def __init__(self,
                 market_metrics_repository: MarketMetricsRepository,
                 coingecko: CoinGeckoAdapter,
                 collection_interval: int
                ):

        """
        Initialize the MarketMetricsCollector with necessary dependencies and configuration.

        Args:
            market_metrics_repository (MarketMetricsRepository): Repository for storing market metrics.
            coingecko (CoinGeckoAdapter): Adapter for CoinGecko API interactions.
            collection_interval (int): Time interval between collections in seconds. Default is 1 hour.
        """
        super().__init__(collection_interval)
        self.market_metrics_repository = market_metrics_repository
        self.coingecko = coingecko
        self.logger = LoggerSetup.setup(__class__.__name__)


    @property
    def collector_type(self) -> str:
        """
        Get the type of this collector.

        Returns:
            str: The collector type ("market_metrics").
        """
        return "market_metrics"


    async def fetch_token_data(self, tokens: set[str]) -> AsyncGenerator[MarketMetricsModel, None]:
        """Fetch market metrics from Coingecko API for multiple tokens"""
        # Get CoinGecko IDs for all tokens (using cached values)
        coin_ids = []
        failed_tokens = set()

        for token in tokens:
            try:
                if coin_id := await self.coingecko.get_coin_id(token):
                    coin_ids.append(coin_id)
                else:
                    failed_tokens.add(token)
                    self.logger.warning(f"Could not find CoinGecko ID for token: {token}")
            except Exception as e:
                failed_tokens.add(token)
                self.logger.warning(f"Error getting CoinGecko ID for token {token}: {str(e)}")

        if failed_tokens:
            self.logger.warning(f"Skipping tokens without CoinGecko IDs: {failed_tokens}")

        try:
            # Process each market data item yielded by the adapter
            async for market_data in self.coingecko.get_market_data(coin_ids):
                try:
                    model = MarketMetricsModel.from_raw_data(market_data)
                    yield model
                except Exception as e:
                    self.logger.warning(f"Failed to process market data for {market_data.get('id', 'unknown')}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error collecting market metrics: {e}")
            raise ServiceError(f"Market metrics collection failed: {str(e)}")


    async def get_token_data(self, tokens: set[str]) -> list[MarketMetricsModel]:
        """Get stored metadata for multiple tokens"""
        return await self.market_metrics_repository.get_market_metrics(tokens)


    async def store_token_data(self, data: list[MarketMetricsModel]) -> None:
        """
        Store collected market metrics for multiple symbols.

        Args:
            data (set[MarketMetrics]): List of MarketMetrics objects to be stored.
        """
        await self.market_metrics_repository.upsert_market_metrics(data)


    async def delete_token_data(self, tokens: set[str]) -> None:
        """
        Delete market metrics for multiple tokens.

        Args:
            tokens (set[str]): Set of tokens to delete market metrics for.
        """
        await self.market_metrics_repository.delete_market_metrics(tokens)
