from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import TokenPlatform
from shared.utils.logger import LoggerSetup


class PlatformRepository:
    """Repository for managing TokenPlatform data in the database"""

    def __init__(self, session: AsyncSession):
        self._session = session
        self.logger = LoggerSetup.setup(__class__.__name__)

    async def delete_token_platforms(self, token_id: str) -> None:
        """
        Delete all platform records for a token

        Args:
            token_id: Token ID to delete platforms for
        """
        try:
            stmt = delete(TokenPlatform).where(TokenPlatform.token_id == token_id)
            await self._session.execute(stmt)
            await self._session.commit()

        except Exception as e:
            await self._session.rollback()
            self.logger.error(f"Failed to delete platform records for {token_id}: {e}")
            raise

    async def get_token_platforms(self, token_id: str) -> list[TokenPlatform]:
        """
        Get all platform records for a token

        Args:
            token_id: Token ID to get platforms for

        Returns:
            List of TokenPlatform records
        """
        try:
            stmt = select(TokenPlatform).where(TokenPlatform.token_id == token_id)
            result = await self._session.execute(stmt)
            return list(result.scalars().all())

        except Exception as e:
            self.logger.error(f"Failed to get platform records for {token_id}: {e}")
            raise
