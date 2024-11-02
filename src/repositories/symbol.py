# src/repositories/symbol.py

from typing import List
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.core.models import SymbolInfo
from src.utils.time import from_timestamp

from .base import Repository
from ..models.market import Symbol
from ..utils.domain_types import ExchangeName
from ..core.exceptions import RepositoryError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class SymbolRepository(Repository[Symbol]):
    """Repository for Symbol operations"""

    def __init__(self, session_factory: async_sessionmaker):
        super().__init__(session_factory, Symbol)

    async def get_or_create(self, symbol_info: SymbolInfo) -> Symbol:
        try:
            async with self.get_session() as session:
                stmt = select(self.model_class).where(
                    and_(Symbol.name == symbol_info.name, Symbol.exchange == symbol_info.exchange)
                )
                result = await session.execute(stmt)
                symbol = result.scalar_one_or_none()

                if symbol:
                    if symbol.first_trade_time is None:
                        symbol.first_trade_time = symbol_info.launch_time
                        await session.flush()
                    return symbol

                symbol = Symbol(
                    name=symbol_info.name,
                    exchange=symbol_info.exchange,
                    first_trade_time=symbol_info.launch_time
                )
                session.add(symbol)
                await session.flush()
                await session.refresh(symbol)
                logger.info(
                    f"Created new symbol: {symbol_info.name} for {symbol_info.exchange} "
                    f"with launch time {from_timestamp(symbol_info.launch_time)}"
                )
                return symbol

        except Exception as e:
            logger.error(f"Error in get_or_create: {e}")
            raise RepositoryError(f"Failed to get or create symbol: {str(e)}")

    async def get_by_exchange(self, exchange: ExchangeName) -> List[Symbol]:
        """Get all symbols for an exchange"""
        try:
            async with self.get_session() as session:
                stmt = select(self.model_class).where(Symbol.exchange == exchange)
                result = await session.execute(stmt)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            raise RepositoryError(f"Failed to get symbols: {str(e)}")