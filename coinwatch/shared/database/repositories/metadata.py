# src/repositories/metadata.py

from typing import List, Optional
from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert

from shared.core.enums import IsolationLevel, DataSource
from shared.database.connection import DatabaseConnection
from shared.core.models import Platform, SymbolInfo, Metadata
from shared.core.exceptions import RepositoryError
from shared.database.models import TokenMetadata, TokenPlatform
from shared.utils.time import TimeUtils
from shared.utils.logger import LoggerSetup


logger = LoggerSetup.setup(__name__)

class MetadataRepository:
    """Repository for token metadata operations"""

    def __init__(self, db_service: DatabaseConnection):
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
            async with self.db_service.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                metadata_values = [metadata.to_dict() for metadata in metadata_list]
                stmt = insert(TokenMetadata).values(metadata_values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        col.name: stmt.excluded[col.name]
                        for col in TokenMetadata.__table__.columns
                        if col.name != 'id'
                    }
                )
                await session.execute(stmt)

                # Bulk delete existing platforms for all tokens
                token_ids = [metadata.id for metadata in metadata_list]
                delete_stmt = delete(TokenPlatform).where(
                    TokenPlatform.token_id.in_(token_ids)
                )
                await session.execute(delete_stmt)

                # Bulk insert new platforms
                platform_values = [
                    {
                        'token_id': metadata.id,
                        'platform_id': platform.platform_id,
                        'contract_address': platform.contract_address
                    }
                    for metadata in metadata_list
                    for platform in metadata.platforms
                ]
                if platform_values:
                    await session.execute(
                        insert(TokenPlatform).values(platform_values)
                    )

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
            async with self.db_service.session() as session:
                base_token = symbol.name.lower().replace('usdt', '')

                # Find metadata by CoinGecko symbol
                stmt = (
                    select(TokenMetadata)
                    .outerjoin(TokenPlatform)
                    .where(TokenMetadata.symbol == base_token)
                    .options(selectinload(TokenMetadata.platforms))
                )
                result = await session.execute(stmt)
                metadata_record = result.scalar_one_or_none()

                if metadata_record:
                    platforms = [
                        Platform(
                            platform_id=p.platform_id,
                            contract_address=p.contract_address
                        )
                        for p in metadata_record.platforms
                    ]

                    return Metadata(
                        id=metadata_record.id,
                        symbol=symbol,
                        name=metadata_record.name,
                        description=metadata_record.description,
                        categories=metadata_record.categories,
                        market_cap_rank=metadata_record.market_cap_rank,
                        hashing_algorithm=metadata_record.hashing_algorithm,
                        launch_time=TimeUtils.to_timestamp(metadata_record.launch_time) if metadata_record.launch_time else None,
                        platforms=platforms,
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
            async with self.db_service.session() as session:

                # Find and delete metadata by CoinGecko symbol
                stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol == symbol
                )
                result = await session.execute(stmt)
                metadata_record = result.scalar_one_or_none()

                if metadata_record:
                    await session.delete(metadata_record)
                    logger.info(f"Deleted metadata for symbol '{symbol.upper()}'")
                else:
                    logger.warning(f"No metadata found for symbol '{symbol.upper()}'")

        except Exception as e:
            logger.error(f"Error deleting metadata for {symbol}: {e}")
            raise RepositoryError(f"Failed to delete metadata: {str(e)}")