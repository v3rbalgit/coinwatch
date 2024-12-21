from datetime import datetime
from typing import AsyncGenerator

from .collector import FundamentalCollector
from shared.clients.coingecko import CoinGeckoAdapter
from shared.core.enums import DataSource
from shared.core.exceptions import ServiceError
from shared.core.models import MetadataModel, PlatformModel
from shared.database.repositories.metadata import MetadataRepository
from shared.utils.logger import LoggerSetup
from shared.utils.time import get_current_datetime



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

        self.logger = LoggerSetup.setup(__class__.__name__)


    @property
    def collector_type(self) -> str:
        """
        Get the type of this collector.

        Returns:
            str: The collector type ("metadata").
        """
        return "metadata"


    async def fetch_token_data(self, tokens: set[str]) -> AsyncGenerator[MetadataModel, None]:
        """Fetch metadata from Coingecko API for multiple tokens"""
        try:
            for token in tokens:
                # Get coin info (using cached coin_id)
                coin_id = await self.coingecko.get_coin_id(token)
                if not coin_id:
                    continue

                coin_data = await self.coingecko.get_metadata(coin_id)

                # Convert launch time if provided
                launch_time = None
                if genesis_date := coin_data.get("genesis_date"):
                    try:
                        launch_time = datetime.strptime(genesis_date, "%Y-%m-%d")
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid launch time format for {token}")

                # Process platforms data
                platforms = []
                if platform_data := coin_data.get("platforms"):
                    for platform_id, contract_address in platform_data.items():
                        if platform_id and contract_address:
                            platforms.append(PlatformModel(
                                token_id=coin_id,
                                platform_id=platform_id,
                                contract_address=contract_address
                            ))

                yield MetadataModel(
                    id=coin_data.get("id", ""),
                    symbol=coin_data.get("symbol", ""),
                    name=coin_data.get("name", token),
                    description=coin_data.get("description", {}).get("en"),
                    market_cap_rank=coin_data.get("market_cap_rank", -1),
                    updated_at=get_current_datetime(),
                    launch_time=launch_time,
                    categories=coin_data.get("categories", []),
                    platforms=platforms,
                    hashing_algorithm=coin_data.get("contract_address"),
                    website=coin_data.get("links", {}).get("homepage", [None])[0] if coin_data.get("links", {}).get("homepage") else None,
                    whitepaper=coin_data.get("links", {}).get("whitepaper"),
                    reddit=coin_data.get("links", {}).get("subreddit_url"),
                    twitter=coin_data.get("links", {}).get("twitter_screen_name"),
                    telegram=coin_data.get("links", {}).get("telegram_channel_identifier"),
                    github=coin_data.get("links", {}).get("repos_url", {}).get("github", [None])[0] if coin_data.get("links", {}).get("repos_url", {}).get("github") else None,
                    images={
                        'thumb': coin_data.get("image", {}).get("thumb"),
                        'small': coin_data.get("image", {}).get("small"),
                        'large': coin_data.get("image", {}).get("large")
                    },
                    data_source=DataSource.COINGECKO
                )

        except Exception as e:
            self.logger.error(f"Error in metadata collection: {e}")
            raise ServiceError(f"Metadata collection failed: {str(e)}")


    async def get_token_data(self, tokens: set[str]) -> list[MetadataModel]:
        """Get stored metadata for multiple tokens"""
        return await self.metadata_repository.get_metadata(tokens)


    async def store_token_data(self, data: list[MetadataModel]) -> None:
        """
        Store collected metadata for multiple tokens.

        Args:
            data (List[Metadata]): Set of Metadata objects to be stored.
        """
        await self.metadata_repository.upsert_metadata(data)


    async def delete_token_data(self, tokens: set[str]) -> None:
        """
        Delete metadata for multiple tokens.

        Args:
            tokens (set[str]): Set of tokens to delete metadata for.
        """
        await self.metadata_repository.delete_metadata(tokens)
