# src/repositories/base.py

from typing import Generic, TypeVar, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.market import Symbol, Kline
from ..core.exceptions import RepositoryError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

ModelType = TypeVar('ModelType', Symbol, Kline)

class Repository(Generic[ModelType]):
    """Base repository class with common CRUD operations"""

    def __init__(self, session: AsyncSession, model_class: type[ModelType]):
        self.session = session
        self.model_class = model_class

    async def get_by_id(self, id: int) -> Optional[ModelType]:
        """Get entity by ID"""
        try:
            return await self.session.get(self.model_class, id)
        except Exception as e:
            logger.error(f"Error getting {self.model_class.__name__} by id: {e}")
            raise RepositoryError(f"Failed to get {self.model_class.__name__}: {str(e)}")

    async def get_all(self) -> List[ModelType]:
        """Get all entities"""
        try:
            stmt = select(self.model_class)
            result = await self.session.execute(stmt)
            # Convert Sequence to List explicitly
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting all {self.model_class.__name__}s: {e}")
            raise RepositoryError(f"Failed to list {self.model_class.__name__}s: {str(e)}")

    async def create(self, entity: ModelType) -> ModelType:
        """Create new entity"""
        try:
            self.session.add(entity)
            await self.session.flush()
            return entity
        except Exception as e:
            logger.error(f"Error creating {self.model_class.__name__}: {e}")
            raise RepositoryError(f"Failed to create {self.model_class.__name__}: {str(e)}")

    async def delete(self, entity: ModelType) -> None:
        """Delete entity"""
        try:
            await self.session.delete(entity)
            await self.session.flush()
        except Exception as e:
            logger.error(f"Error deleting {self.model_class.__name__}: {e}")
            raise RepositoryError(f"Failed to delete {self.model_class.__name__}: {str(e)}")