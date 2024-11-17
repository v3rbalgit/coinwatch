# src/services/fundamental_data/market_metrics_collector.py

from decimal import Decimal
from typing import List, Set

from .collector import FundamentalCollector
from ...adapters.coingecko import CoinGeckoAdapter
from ...core.models import MarketMetrics
from ...core.exceptions import ServiceError
from ...repositories.market import MarketMetricsRepository
from ...utils.domain_types import DataSource
from ...utils.logger import LoggerSetup
from ...utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

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
                 collection_interval: int = 3600
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

    @property
    def collector_type(self) -> str:
        """
        Get the type of this collector.

        Returns:
            str: The collector type ("market_metrics").
        """
        return "market_metrics"

    async def collect_symbol_data(self, tokens: Set[str]) -> List[MarketMetrics]:
        """Collect market metrics for multiple tokens"""
        try:
            # Get CoinGecko IDs for all tokens (using cached values)
            coin_ids = []
            for token in tokens:
                if coin_id := await self.coingecko.get_coin_id(token):
                    coin_ids.append(coin_id)

            if not coin_ids:
                return []

            # Collect market data in bulk (handles pagination internally)
            market_data = await self.coingecko.get_market_data(coin_ids)

            metrics = []
            for data in market_data:
                metrics.append(MarketMetrics(
                    id=data['id'],
                    symbol=data['symbol'],
                    current_price=Decimal(str(data['current_price'])),
                    market_cap=Decimal(str(data['market_cap'])) if data.get('market_cap') else None,
                    market_cap_rank=data.get('market_cap_rank'),
                    fully_diluted_valuation=Decimal(str(data['fully_diluted_valuation'])) if data.get('fully_diluted_valuation') else None,
                    total_volume=Decimal(str(data['total_volume'])),
                    high_24h=Decimal(str(data['high_24h'])) if data.get('high_24h') else None,
                    low_24h=Decimal(str(data['low_24h'])) if data.get('low_24h') else None,
                    price_change_24h=Decimal(str(data['price_change_24h'])) if data.get('price_change_24h') else None,
                    price_change_percentage_24h=Decimal(str(data['price_change_percentage_24h'])) if data.get('price_change_percentage_24h') else None,
                    market_cap_change_24h=Decimal(str(data['market_cap_change_24h'])) if data.get('market_cap_change_24h') else None,
                    market_cap_change_percentage_24h=Decimal(str(data['market_cap_change_percentage_24h'])) if data.get('market_cap_change_percentage_24h') else None,
                    circulating_supply=Decimal(str(data['circulating_supply'])) if data.get('circulating_supply') else None,
                    total_supply=Decimal(str(data['total_supply'])) if data.get('total_supply') else None,
                    max_supply=Decimal(str(data['max_supply'])) if data.get('max_supply') else None,
                    ath=Decimal(str(data['ath'])) if data.get('ath') else None,
                    ath_change_percentage=Decimal(str(data['ath_change_percentage'])) if data.get('ath_change_percentage') else None,
                    ath_date=data.get('ath_date'),
                    atl=Decimal(str(data['atl'])) if data.get('atl') else None,
                    atl_change_percentage=Decimal(str(data['atl_change_percentage'])) if data.get('atl_change_percentage') else None,
                    atl_date=data.get('atl_date'),
                    updated_at=TimeUtils.get_current_timestamp(),
                    data_source=DataSource.COINGECKO
                ))

            return metrics

        except Exception as e:
            logger.error(f"Error collecting market metrics: {e}")
            raise ServiceError(f"Market metrics collection failed: {str(e)}")

    async def store_symbol_data(self, data: List[MarketMetrics]) -> None:
        """
        Store collected market metrics for multiple symbols.

        Args:
            data (List[MarketMetrics]): List of MarketMetrics objects to be stored.
        """
        await self.market_metrics_repository.upsert_market_metrics(data)

    async def delete_symbol_data(self, token: str) -> None:
        """
        Delete market metrics for a specific symbol.

        Args:
            token (str): The token symbol for which to delete market metrics.
        """
        await self.market_metrics_repository.delete_market_metrics(token)