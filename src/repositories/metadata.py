# src/repositories/metadata.py

from typing import Optional
from sqlalchemy import select

from ..services.database.service import DatabaseService
from ..core.models import SymbolInfo, Metadata
from ..core.exceptions import RepositoryError
from ..models.fundamental import TokenMetadata
from ..utils.time import TimeUtils
from ..utils.logger import LoggerSetup
from ..utils.domain_types import DataSource, IsolationLevel

logger = LoggerSetup.setup(__name__)

class MetadataRepository:
    """Repository for token metadata operations"""

    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service

    async def upsert_metadata(self, symbol: SymbolInfo, metadata: Metadata) -> None:
        """
        Create or update metadata for a symbol

        Args:
            symbol: Symbol to update metadata for
            metadata: Dictionary containing metadata fields
        """
        try:
            async with self.db_service.get_session(
                isolation_level=IsolationLevel.REPEATABLE_READ
            ) as session:
                # Check for existing metadata
                metadata_stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol == symbol.name.lower().replace('usdt', '')
                )
                metadata_result = await session.execute(metadata_stmt)
                metadata_record = metadata_result.scalar_one_or_none()

                metadata_dict = metadata.to_dict()

                if metadata_record:
                    # Update existing record
                    for key, value in metadata_dict.items():
                        setattr(metadata_record, key, value)
                else:
                    # Create new record
                    metadata_record = TokenMetadata(**metadata_dict)
                    session.add(metadata_record)

                await session.commit()
                logger.debug(f"Updated metadata for {symbol}")

        except Exception as e:
            logger.error(f"Error upserting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to upsert metadata: {str(e)}")

    async def get_metadata(self, symbol: SymbolInfo) -> Optional[Metadata]:
        """
        Get stored metadata for a symbol

        Args:
            symbol: Symbol to get metadata for (e.g., BTCUSDT on bybit)

        Returns:
            Metadata domain object if found, None otherwise
        """
        try:
            async with self.db_service.get_session() as session:
                # Extract base token from symbol name
                base_token = symbol.name.lower().replace('usdt', '')

                # Find metadata by CoinGecko symbol
                stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol == base_token
                )
                result = await session.execute(stmt)
                metadata_record = result.scalar_one_or_none()

                if metadata_record:
                    return Metadata(
                        id=metadata_record.coingecko_id,
                        symbol=symbol,
                        name=metadata_record.name,
                        description=metadata_record.description,
                        category=metadata_record.category,
                        market_cap_rank=metadata_record.market_cap_rank,
                        hashing_algorithm=metadata_record.hashing_algorithm,
                        launch_time=TimeUtils.to_timestamp(metadata_record.launch_time) if metadata_record.launch_time else None,
                        platform=metadata_record.platform,
                        contract_address=metadata_record.contract_address,
                        website=metadata_record.website,
                        whitepaper=metadata_record.whitepaper,
                        reddit=metadata_record.reddit,
                        twitter=metadata_record.twitter,
                        telegram=metadata_record.telegram,
                        github=metadata_record.github,
                        images={
                            'thumb': metadata_record.image_thumb,
                            'small': metadata_record.image_small,
                            'large': metadata_record.image_large
                        },
                        updated_at=TimeUtils.to_timestamp(metadata_record.updated_at),
                        data_source=DataSource.COINGECKO
                    )
                return None

        except Exception as e:
            logger.error(f"Error getting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to get metadata: {str(e)}")

    async def delete_metadata(self, symbol: SymbolInfo) -> None:
        """
        Delete metadata for a token

        Args:
            symbol: Any symbol representing the token (e.g., BTCUSDT from any exchange)
        """
        try:
            async with self.db_service.get_session() as session:
                base_token = symbol.name.lower().replace('usdt', '')

                # Find and delete metadata by CoinGecko symbol
                stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol == base_token
                )
                result = await session.execute(stmt)
                metadata_record = result.scalar_one_or_none()

                if metadata_record:
                    await session.delete(metadata_record)
                    await session.commit()
                    logger.info(f"Deleted metadata for symbol '{base_token.upper()}'")
                else:
                    logger.warning(f"No metadata found for symbol '{base_token.upper()}'")

        except Exception as e:
            logger.error(f"Error deleting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to delete metadata: {str(e)}")