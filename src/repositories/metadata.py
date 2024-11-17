# src/repositories/metadata.py

from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from ..services.database.service import DatabaseService
from ..core.models import SymbolInfo, Metadata
from ..core.exceptions import RepositoryError
from ..models.metadata import TokenMetadata
from ..utils.time import TimeUtils
from ..utils.logger import LoggerSetup
from ..utils.domain_types import DataSource, IsolationLevel

logger = LoggerSetup.setup(__name__)

class MetadataRepository:
    """Repository for token metadata operations"""

    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service

    async def upsert_metadata(self, metadata_list: List[Metadata]) -> None:
        """
        Create or update metadata for multiple symbols using PostgreSQL upsert.

        Args:
            metadata_list (List[Metadata]): List of metadata objects to upsert.

        Raises:
            RepositoryError: If the upsert operation fails.
        """
        try:
            async with self.db_service.get_session(
                isolation_level=IsolationLevel.REPEATABLE_READ
            ) as session:
                # Convert all metadata to database format
                db_data = [metadata.to_dict() for metadata in metadata_list]

                stmt = insert(TokenMetadata).values(db_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        col.name: stmt.excluded[col.name]
                        for col in TokenMetadata.__table__.columns
                        if col.name != 'id'
                    }
                )
                await session.execute(stmt)
                await session.commit()

                logger.debug(f"Updated metadata for {len(metadata_list)} tokens")

        except Exception as e:
            logger.error(f"Error in bulk metadata upsert: {e}")
            raise RepositoryError(f"Failed to upsert metadata batch: {str(e)}")

    async def get_metadata(self, symbol: SymbolInfo) -> Optional[Metadata]:
        """
        Get stored metadata for a symbol.

        Args:
            symbol (SymbolInfo): Symbol to get metadata for (e.g., BTCUSDT on bybit).

        Returns:
            Optional[Metadata]: Metadata domain object if found, None otherwise.

        Raises:
            RepositoryError: If the retrieval operation fails.
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
                        id=metadata_record.id,
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
                        data_source=DataSource(metadata_record.data_source)
                    )
                return None

        except Exception as e:
            logger.error(f"Error getting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to get metadata: {str(e)}")

    async def delete_metadata(self, symbol: str) -> None:
        """
        Delete metadata for a token.

        Args:
            symbol (str): Symbol representing the token to delete metadata for.

        Raises:
            RepositoryError: If the deletion operation fails.
        """
        try:
            async with self.db_service.get_session() as session:

                # Find and delete metadata by CoinGecko symbol
                stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol == symbol
                )
                result = await session.execute(stmt)
                metadata_record = result.scalar_one_or_none()

                if metadata_record:
                    await session.delete(metadata_record)
                    await session.commit()
                    logger.info(f"Deleted metadata for symbol '{symbol.upper()}'")
                else:
                    logger.warning(f"No metadata found for symbol '{symbol.upper()}'")

        except Exception as e:
            logger.error(f"Error deleting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to delete metadata: {str(e)}")