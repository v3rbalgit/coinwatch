# src/services/fundamental_data/metadata_collector.py

from datetime import datetime
from typing import List, Set

from .collector import FundamentalCollector
from shared.clients.coingecko import CoinGeckoAdapter
from shared.core.enums import DataSource
from shared.core.exceptions import ServiceError
from shared.core.models import Metadata, Platform
from shared.database.repositories.metadata import MetadataRepository
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)


class MetadataCollector(FundamentalCollector):
    """
    Collector for token metadata information from CoinGecko.

    Inherits from FundamentalCollector to manage collection and storage of metadata.
    """

    def __init__(self,
                 metadata_repository: MetadataRepository,
                 coingecko: CoinGeckoAdapter,
                 collection_interval: int
                ):
        """
        Initialize the MetadataCollector.

        Args:
            coordinator (ServiceCoordinator): Coordinator for service management.
            metadata_repository (MetadataRepository): Repository for storing metadata.
            coingecko (CoinGeckoAdapter): Adapter for CoinGecko API interactions.
            collection_interval (int): Time interval between collections in seconds.
        """
        super().__init__(collection_interval)
        self.metadata_repository = metadata_repository
        self.coingecko = coingecko

    @property
    def collector_type(self) -> str:
        """
        Get the type of this collector.

        Returns:
            str: The collector type ("metadata").
        """
        return "metadata"

    async def collect_symbol_data(self, tokens: Set[str]) -> List[Metadata]:
        """Collect metadata for multiple tokens"""
        collected_metadata = []
        try:
            for token in tokens:
                try:
                    # Get coin info (using cached coin_id)
                    coin_id = await self.coingecko.get_coin_id(token)
                    if not coin_id:
                        continue

                    coin_data = await self.coingecko.get_coin_info(coin_id)

                    # Convert launch time if provided
                    launch_time = None
                    if genesis_date := coin_data.get("genesis_date"):
                        try:
                            dt = datetime.strptime(genesis_date, "%Y-%m-%d")
                            launch_time = TimeUtils.to_timestamp(dt)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid launch time format for {token}")

                    # Process platforms data
                    platforms = []
                    if platform_data := coin_data.get("platforms"):
                        for platform_id, contract_address in platform_data.items():
                            if platform_id and contract_address:
                                platforms.append(Platform(
                                    platform_id=platform_id,
                                    contract_address=contract_address
                                ))

                    metadata = Metadata(
                        id=coin_data.get("id", ""),
                        symbol=coin_data.get("symbol", ""),
                        name=coin_data.get("name", token),
                        description=coin_data.get("description", {}).get("en"),
                        market_cap_rank=coin_data.get("market_cap_rank", -1),
                        updated_at=TimeUtils.get_current_timestamp(),
                        launch_time=launch_time,
                        categories=coin_data.get("categories", []),
                        platforms=platforms,
                        hashing_algorithm=coin_data.get("contract_address"),
                        website=coin_data.get("links", {}).get("homepage", [None])[0],
                        whitepaper=coin_data.get("links", {}).get("whitepaper"),
                        reddit=coin_data.get("links", {}).get("subreddit_url"),
                        twitter=coin_data.get("links", {}).get("twitter_screen_name"),
                        telegram=coin_data.get("links", {}).get("telegram_channel_identifier"),
                        github=coin_data.get("links", {}).get("repos_url", {}).get("github", [None])[0],
                        images={
                            'thumb': coin_data.get("image", {}).get("thumb"),
                            'small': coin_data.get("image", {}).get("small"),
                            'large': coin_data.get("image", {}).get("large")
                        },
                        data_source=DataSource.COINGECKO
                    )
                    collected_metadata.append(metadata)

                except Exception as e:
                    logger.error(f"Error collecting metadata for {token}: {e}")
                    continue

            return collected_metadata

        except Exception as e:
            logger.error(f"Error in bulk metadata collection: {e}")
            raise ServiceError(f"Metadata collection failed: {str(e)}")

    async def store_symbol_data(self, data: List[Metadata]) -> None:
        """
        Store collected metadata for multiple symbols.

        Args:
            data (List[Metadata]): List of Metadata objects to be stored.
        """
        await self.metadata_repository.upsert_metadata(data)

    async def delete_symbol_data(self, token: str) -> None:
        """
        Delete metadata for a specific symbol.

        Args:
            token (str): The token symbol for which to delete metadata.
        """
        await self.metadata_repository.delete_metadata(token)