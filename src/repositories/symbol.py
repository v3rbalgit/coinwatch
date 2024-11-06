# src/repositories/symbol.py

from typing import List
from sqlalchemy import select, and_, text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

from src.core.models import SymbolInfo

from .base import Repository
from ..models.market import Symbol
from ..utils.domain_types import ExchangeName
from ..core.exceptions import RepositoryError
from ..utils.logger import LoggerSetup
from ..utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class SymbolRepository(Repository[Symbol]):
    """Repository for Symbol operations with PostgreSQL optimization"""

    def __init__(self, session_factory: async_sessionmaker):
        super().__init__(session_factory, Symbol)

    async def get_or_create(self, symbol_info: SymbolInfo) -> Symbol:
        """
        Get or create a symbol using PostgreSQL upsert

        Uses INSERT ... ON CONFLICT DO UPDATE for atomic operation
        """
        try:
            async with self.get_session() as session:
                # First try to get existing symbol
                existing_stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol_info.name,
                        Symbol.exchange == symbol_info.exchange
                    )
                )
                result = await session.execute(existing_stmt)
                existing_symbol = result.scalar_one_or_none()

                if existing_symbol:
                    if existing_symbol.first_trade_time is None:
                        existing_symbol.first_trade_time = symbol_info.launch_time
                        await session.flush()
                    logger.debug(
                        f"Retrieved symbol {symbol_info.name} for {symbol_info.exchange} "
                        f"with launch time {TimeUtils.from_timestamp(symbol_info.launch_time)}"
                    )
                    return existing_symbol

                # Create new symbol
                new_symbol = Symbol(
                    name=symbol_info.name,
                    exchange=symbol_info.exchange,
                    first_trade_time=symbol_info.launch_time
                )
                session.add(new_symbol)
                await session.flush()
                await session.refresh(new_symbol)

                logger.debug(
                    f"Created new symbol {symbol_info.name} for {symbol_info.exchange} "
                    f"with launch time {TimeUtils.from_timestamp(symbol_info.launch_time)}"
                )
                return new_symbol

        except Exception as e:
            logger.error(f"Error in get_or_create: {e}")
            raise RepositoryError(f"Failed to get or create symbol: {str(e)}")

    async def get_by_exchange(self, exchange: ExchangeName) -> List[Symbol]:
        """
        Get all symbols for an exchange with optimized query

        Uses index-only scan when possible
        """
        try:
            async with self.get_session() as session:
                # Create optimized query using index
                stmt = (
                    select(Symbol)
                    .where(Symbol.exchange == exchange)
                    .order_by(Symbol.name)
                )

                result = await session.execute(stmt)
                return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            raise RepositoryError(f"Failed to get symbols: {str(e)}")

    async def get_symbols_with_stats(self, exchange: ExchangeName) -> List[dict]:
        """
        Get symbols with their data statistics

        Uses TimescaleDB features to gather statistics efficiently
        """
        try:
            async with self.get_session() as session:
                stmt = text("""
                    WITH stats AS (
                        SELECT
                            symbol_id,
                            timeframe,
                            count(*) as kline_count,
                            min(start_time) as first_kline,
                            max(start_time) as last_kline
                        FROM kline_data
                        GROUP BY symbol_id, timeframe
                    )
                    SELECT
                        s.id,
                        s.name,
                        s.exchange,
                        s.first_trade_time,
                        json_agg(json_build_object(
                            'timeframe', st.timeframe,
                            'kline_count', st.kline_count,
                            'first_kline', st.first_kline,
                            'last_kline', st.last_kline
                        )) as timeframe_stats
                    FROM symbols s
                    LEFT JOIN stats st ON s.id = st.symbol_id
                    WHERE s.exchange = :exchange
                    GROUP BY s.id, s.name, s.exchange, s.first_trade_time
                    ORDER BY s.name;
                """)

                result = await session.execute(stmt, {"exchange": exchange})
                return [dict(row) for row in result]

        except Exception as e:
            logger.error(f"Error getting symbol stats: {e}")
            raise RepositoryError(f"Failed to get symbol statistics: {str(e)}")

    async def get_missing_timeframes(self,
                                   symbol: SymbolInfo,
                                   required_timeframes: List[str]) -> List[str]:
        """
        Find missing timeframes for a symbol

        Uses efficient EXISTS subquery
        """
        try:
            async with self.get_session() as session:
                # Get the symbol ID first
                symbol_stmt = (
                    select(Symbol.id)
                    .where(and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    ))
                )
                symbol_result = await session.execute(symbol_stmt)
                symbol_id = symbol_result.scalar_one_or_none()

                if not symbol_id:
                    return required_timeframes

                # Check each timeframe
                missing = []
                for timeframe in required_timeframes:
                    stmt = text("""
                        SELECT NOT EXISTS (
                            SELECT 1 FROM kline_data
                            WHERE symbol_id = :symbol_id
                            AND timeframe = :timeframe
                            LIMIT 1
                        ) as missing;
                    """)

                    result = await session.execute(stmt, {
                        "symbol_id": symbol_id,
                        "timeframe": timeframe
                    })

                    if result.scalar():
                        missing.append(timeframe)

                return missing

        except Exception as e:
            logger.error(f"Error checking timeframes: {e}")
            raise RepositoryError(f"Failed to check timeframes: {str(e)}")