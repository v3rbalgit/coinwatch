# src/repositories/base.py

from typing import Generic, TypeVar, List, Optional, Type
from sqlalchemy import select

from ..services.database import DatabaseService
from ..models.market import Symbol, Kline
from ..core.exceptions import RepositoryError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

ModelType = TypeVar('ModelType', Symbol, Kline)

class Repository(Generic[ModelType]):
    """Base repository class with common CRUD operations"""

    def __init__(self, db_service: DatabaseService, model_class: Type[ModelType]):
        self.db_service = db_service
        self.model_class = model_class

    async def get_by_id(self, id: int) -> Optional[ModelType]:
        """Get entity by ID"""
        try:
            async with self.db_service.get_session() as session:
                return await session.get(self.model_class, id)
        except Exception as e:
            logger.error(f"Error getting {self.model_class.__name__} by id: {e}")
            raise RepositoryError(f"Failed to get {self.model_class.__name__}: {str(e)}")

    async def get_all(self) -> List[ModelType]:
        """Get all entities"""
        try:
            async with self.db_service.get_session() as session:
                stmt = select(self.model_class)
                result = await session.execute(stmt)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting all {self.model_class.__name__}s: {e}")
            raise RepositoryError(f"Failed to list {self.model_class.__name__}s: {str(e)}")

    async def create(self, entity: ModelType) -> ModelType:
        """Create new entity"""
        try:
            async with self.db_service.get_session() as session:
                session.add(entity)
                await session.flush()
                return entity
        except Exception as e:
            logger.error(f"Error creating {self.model_class.__name__}: {e}")
            raise RepositoryError(f"Failed to create {self.model_class.__name__}: {str(e)}")

    async def delete(self, entity: ModelType) -> None:
        """Delete entity"""
        try:
            async with self.db_service.get_session() as session:
                await session.delete(entity)
                await session.flush()
        except Exception as e:
            logger.error(f"Error deleting {self.model_class.__name__}: {e}")
            raise RepositoryError(f"Failed to delete {self.model_class.__name__}: {str(e)}")