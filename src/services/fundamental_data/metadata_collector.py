# src/services/fundamental_data/metadata_collector.py

from datetime import datetime

from .collector import FundamentalCollector
from ...adapters.coingecko import CoinGeckoAdapter
from ...core.coordination import ServiceCoordinator
from ...core.exceptions import ServiceError
from ...core.models import SymbolInfo, Metadata
from ...repositories.metadata import MetadataRepository
from ...utils.logger import LoggerSetup
from ...utils.time import TimeUtils
from ...utils.domain_types import DataSource

logger = LoggerSetup.setup(__name__)


class MetadataCollector(FundamentalCollector):
    """
    Collector for token metadata information from CoinGecko.

    Inherits from FundamentalCollector to manage collection and storage of metadata.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 metadata_repository: MetadataRepository,
                 coingecko: CoinGeckoAdapter,
                 collection_interval: int,
                 batch_size: int):
        """
        Initialize the MetadataCollector.

        Args:
            coordinator (ServiceCoordinator): Coordinator for service management.
            metadata_repository (MetadataRepository): Repository for storing metadata.
            coingecko (CoinGeckoAdapter): Adapter for CoinGecko API interactions.
            collection_interval (int): Time interval between collections in seconds.
            batch_size (int): Number of symbols to process concurrently.
        """
        super().__init__(collection_interval, batch_size)
        self.coordinator = coordinator
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

    async def collect_symbol_data(self, symbol: SymbolInfo) -> Metadata:
        """
        Collect metadata for a symbol from CoinGecko.

        Args:
            symbol (SymbolInfo): Symbol to collect metadata for.

        Returns:
            Metadata: Collected metadata for the symbol.

        Raises:
            ServiceError: If metadata collection fails.
        """
        try:
            coin_id = await self.coingecko.get_coin_id(symbol.name)
            if not coin_id:
                raise ServiceError(f"No CoinGecko ID found for {symbol}")

            coin_data = await self.coingecko.get_coin_info(coin_id)

            # Convert launch time if provided
            launch_time = None
            if genesis_date := coin_data.get("genesis_date"):
                try:
                    dt = datetime.strptime(genesis_date, "%Y-%m-%d")
                    launch_time = TimeUtils.to_timestamp(dt)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid launch time format for {symbol}")

            return Metadata(
                id=coin_data.get("id", ""),
                symbol=symbol,
                name=coin_data.get("name", symbol.symbol_name),
                description=coin_data.get("description", {}).get("en"),
                market_cap_rank=coin_data.get("market_cap_rank", -1),
                updated_at=TimeUtils.get_current_timestamp(),
                launch_time=launch_time,
                category=coin_data.get("categories", [None])[0],
                platform=coin_data.get("asset_platform_id"),
                contract_address=coin_data.get("contract_address"),
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

        except Exception as e:
            logger.error(f"Error collecting metadata for {symbol}: {e}")
            raise ServiceError(f"Metadata collection failed: {str(e)}")

    async def store_symbol_data(self, symbol: SymbolInfo, data: Metadata) -> None:
        """
        Store collected metadata for a symbol.

        Args:
            symbol (SymbolInfo): Symbol the metadata belongs to.
            data (Metadata): Collected metadata to store.
        """
        await self.metadata_repository.upsert_metadata(symbol, data)