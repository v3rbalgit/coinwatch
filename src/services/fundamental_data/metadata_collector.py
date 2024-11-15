# src/services/fundamental_data/metadata_collector.py

from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy import select

from ...services.database.service import DatabaseService
from ...core.coordination import ServiceCoordinator
from ...core.exceptions import ServiceError
from ...core.models import SymbolInfo
from ...models.fundamental import TokenMetadata
from ...models import Symbol
from ...utils.domain_types import IsolationLevel
from ...utils.time import TimeUtils
from ...utils.logger import LoggerSetup
from ...config import MarketDataConfig
from ...adapters.coingecko import CoinGeckoAdapter
from .collector import FundamentalCollector

logger = LoggerSetup.setup(__name__)

class MetadataCollector(FundamentalCollector):
    """
    Collects and manages token metadata information.

    Uses CoinGecko API to fetch basic token information and
    stores it in the database.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 db_service: DatabaseService,
                 coingecko: CoinGeckoAdapter,
                 config: MarketDataConfig):
        super().__init__(coordinator, config)
        self.db_service = db_service
        self.coingecko = coingecko

    @property
    def collector_type(self) -> str:
        return "metadata"

    async def collect_symbol_data(self, symbol: SymbolInfo) -> Dict[str, Any]:
        """
        Collect metadata for a symbol from CoinGecko

        Args:
            symbol: Symbol to collect metadata for

        Returns:
            Dict containing metadata information
        """
        try:
            # Get CoinGecko ID for the symbol
            coin_id = await self.coingecko.get_coin_id(symbol.name)
            if not coin_id:
                raise ServiceError(f"No CoinGecko ID found for {symbol}")

            # Fetch coin information
            coin_data = await self.coingecko.get_coin_info(coin_id)

            # Extract relevant metadata
            return {
                "description": coin_data.get("description", {}).get("en"),
                "launch_time": coin_data.get("genesis_date"),  # Will need conversion
                "category": coin_data.get("categories", [None])[0],
                "platform": coin_data.get("asset_platform_id"),
                "contract_address": coin_data.get("contract_address"),
                "website": coin_data.get("links", {}).get("homepage", [None])[0],
                "whitepaper": coin_data.get("links", {}).get("whitepaper")
            }

        except Exception as e:
            logger.error(f"Error collecting metadata for {symbol}: {e}")
            raise ServiceError(f"Metadata collection failed: {str(e)}")

    async def store_symbol_data(self, symbol: SymbolInfo, data: Dict[str, Any]) -> None:
        """
        Store collected metadata in the database

        Args:
            symbol: Symbol the metadata belongs to
            data: Collected metadata
        """
        try:
            async with self.db_service.get_session(
                isolation_level=IsolationLevel.REPEATABLE_READ
            ) as session:
                # Get symbol ID
                symbol_stmt = select(Symbol).where(
                    Symbol.name == symbol.name,
                    Symbol.exchange == symbol.exchange
                )
                symbol_result = await session.execute(symbol_stmt)
                symbol_record = symbol_result.scalar_one()

                # Check for existing metadata
                metadata_stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol_id == symbol_record.id
                )
                metadata_result = await session.execute(metadata_stmt)
                metadata = metadata_result.scalar_one_or_none()

                # Convert launch time if provided
                launch_time = None
                if data.get("launch_time"):
                    try:
                        # Convert from YYYY-MM-DD to timestamp
                        dt = datetime.strptime(data["launch_time"], "%Y-%m-%d")
                        launch_time = int(dt.timestamp() * 1000)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid launch time format for {symbol}")

                if metadata:
                    # Update existing record
                    metadata.description = data.get("description")
                    metadata.launch_time = launch_time or metadata.launch_time
                    metadata.category = data.get("category")
                    metadata.platform = data.get("platform")
                    metadata.contract_address = data.get("contract_address")
                    metadata.website = data.get("website")
                    metadata.whitepaper = data.get("whitepaper")
                    metadata.updated_at = TimeUtils.get_current_datetime()
                else:
                    # Create new record
                    metadata = TokenMetadata(
                        symbol_id=symbol_record.id,
                        description=data.get("description"),
                        launch_time=launch_time,
                        category=data.get("category"),
                        platform=data.get("platform"),
                        contract_address=data.get("contract_address"),
                        website=data.get("website"),
                        whitepaper=data.get("whitepaper"),
                        updated_at=TimeUtils.get_current_datetime()
                    )
                    session.add(metadata)

                await session.commit()
                logger.info(f"Stored metadata for {symbol}")

        except Exception as e:
            logger.error(f"Error storing metadata for {symbol}: {e}")
            raise ServiceError(f"Metadata storage failed: {str(e)}")

    async def get_metadata(self, symbol: SymbolInfo) -> Optional[Dict[str, Any]]:
        """
        Get stored metadata for a symbol

        Args:
            symbol: Symbol to get metadata for

        Returns:
            Dict containing metadata if found, None otherwise
        """
        try:
            async with self.db_service.get_session() as session:
                # Get symbol and metadata
                stmt = select(TokenMetadata).join(Symbol).where(
                    Symbol.name == symbol.name,
                    Symbol.exchange == symbol.exchange
                )
                result = await session.execute(stmt)
                metadata = result.scalar_one_or_none()

                if metadata:
                    return {
                        "description": metadata.description,
                        "launch_time": metadata.launch_time,
                        "category": metadata.category,
                        "platform": metadata.platform,
                        "contract_address": metadata.contract_address,
                        "website": metadata.website,
                        "whitepaper": metadata.whitepaper,
                        "updated_at": metadata.updated_at
                    }
                return None

        except Exception as e:
            logger.error(f"Error retrieving metadata for {symbol}: {e}")
            raise ServiceError(f"Metadata retrieval failed: {str(e)}")