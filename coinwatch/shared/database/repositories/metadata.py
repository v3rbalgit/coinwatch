from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.postgresql import insert

from shared.core.enums import DataSource
from shared.database.connection import DatabaseConnection
from shared.core.models import PlatformModel, MetadataModel
from shared.core.exceptions import RepositoryError
from shared.database.models import TokenMetadata, TokenPlatform
from shared.utils.logger import LoggerSetup



class MetadataRepository:
    """Repository for token metadata operations"""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service
        self.logger = LoggerSetup.setup(__class__.__name__)


    async def upsert_metadata(self, metadata_set: list[MetadataModel]) -> None:
        """
        Create or update metadata for multiple symbols using PostgreSQL upsert.
        Handles platform records independently to allow sharing between tokens.

        Args:
            metadata_list (List[Metadata]): Set of metadata objects to upsert.

        Raises:
            RepositoryError: If the upsert operation fails.
        """
        try:
            async with self.db_service.session() as session:
                # Single transaction for all operations

                # 1. Upsert metadata records
                metadata_values = [metadata.model_dump() for metadata in metadata_set]
                metadata_stmt = insert(TokenMetadata).values(metadata_values)
                metadata_stmt = metadata_stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        col.name: metadata_stmt.excluded[col.name]
                        for col in TokenMetadata.__table__.columns
                        if col.name != 'id'
                    }
                )
                await session.execute(metadata_stmt)

                # 2. Handle platform records
                for metadata in metadata_set:
                    # Remove existing token-platform relationships for this token
                    delete_stmt = delete(TokenPlatform).where(
                        TokenPlatform.token_id == metadata.id
                    )
                    await session.execute(delete_stmt)

                    # Create new token-platform relationships
                    if metadata.platforms:
                        platform_values = [platform.model_dump() for platform in metadata.platforms]
                        # Use on_conflict_do_update to handle cases where the platform
                        # exists but contract address might have changed
                        platform_stmt = insert(TokenPlatform).values(platform_values)
                        platform_stmt = platform_stmt.on_conflict_do_update(
                            index_elements=['token_id', 'platform_id'],
                            set_={
                                'contract_address': platform_stmt.excluded.contract_address
                            }
                        )
                        await session.execute(platform_stmt)

                self.logger.debug(f"Upserted {len(metadata_set)} metadata records into database")

        except Exception as e:
            self.logger.error(f"Error in bulk metadata upsert: {e}")
            raise RepositoryError(f"Failed to upsert metadata batch: {str(e)}")


    async def get_metadata(self, tokens: set[str]) -> list[MetadataModel]:
        """
        Get stored metadata for multiple tokens.

        Args:
            tokens (Set[str]): Set of symbols to get metadata for (e.g., {'btc', 'eth', 'doge'}).

        Returns:
            List[Metadata]: List of Metadata domain objects for found tokens. Empty list if none found.

        Raises:
            RepositoryError: If the retrieval operation fails.
        """
        try:
            async with self.db_service.session() as session:
                # Find metadata by CoinGecko symbols
                stmt = (
                    select(TokenMetadata)
                    .outerjoin(TokenPlatform)
                    .where(TokenMetadata.symbol.in_(tokens))
                    .options(selectinload(TokenMetadata.platforms))
                )
                result = await session.execute(stmt)
                metadata_records = result.scalars().all()

                return [
                    MetadataModel(
                        id=record.id,
                        symbol=record.symbol,
                        name=record.name,
                        description=record.description,
                        categories=record.categories,
                        market_cap_rank=record.market_cap_rank,
                        hashing_algorithm=record.hashing_algorithm,
                        launch_time=record.launch_time,
                        platforms=[PlatformModel.model_validate(p.__dict__) for p in record.platforms],
                        website=record.website,
                        whitepaper=record.whitepaper,
                        reddit=record.reddit,
                        twitter=record.twitter,
                        telegram=record.telegram,
                        github=record.github,
                        images={
                            'thumb': record.image_thumb,
                            'small': record.image_small,
                            'large': record.image_large
                        },
                        updated_at=record.updated_at,
                        data_source=DataSource(record.data_source)
                    )
                    for record in metadata_records
                ]

        except Exception as e:
            self.logger.error(f"Error getting metadata for {len(tokens)}: {e}")
            raise RepositoryError(f"Failed to get metadata: {str(e)}")


    async def delete_metadata(self, tokens: set[str]) -> None:
        """
        Delete metadata for multiple tokens.

        Args:
            tokens (set[str]): Set of tokens to delete metadata for.

        Raises:
            RepositoryError: If the deletion operation fails.
        """
        try:
            async with self.db_service.session() as session:
                # Find and delete metadata by CoinGecko symbols
                stmt = select(TokenMetadata).where(
                    TokenMetadata.symbol.in_(tokens)
                )
                result = await session.execute(stmt)
                metadata_records = result.scalars().all()

                if metadata_records:
                    for record in metadata_records:
                        await session.delete(record)
                    await session.flush()
                    self.logger.info(f"Deleted metadata for {len(metadata_records)} tokens")
                else:
                    self.logger.warning(f"No metadata found for tokens: {tokens}")

        except Exception as e:
            self.logger.error(f"Error deleting metadata for {len(tokens)} tokens: {e}")
            raise RepositoryError(f"Failed to delete metadata: {str(e)}")
