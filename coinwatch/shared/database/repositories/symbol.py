# src/repositories/symbol.py

from typing import List, Optional
from sqlalchemy import select, and_, text

from shared.core.models import SymbolInfo
from shared.database.connection import DatabaseConnection, IsolationLevel
from shared.database.models import Symbol
from shared.utils.domain_types import ExchangeName
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)


class SymbolRepository:
    """Repository for managing Symbol entities in the database."""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service

    async def get_or_create(self, symbol: SymbolInfo) -> Symbol:
        """
        Get an existing symbol or create a new one if it doesn't exist.

        Args:
            symbol (SymbolInfo): The symbol information to get or create.

        Returns:
            Symbol: The retrieved or created Symbol entity.

        Raises:
            RepositoryError: If there's an error during the operation.
        """
        try:
            async with self.db_service.session(isolation_level=IsolationLevel.SERIALIZABLE) as session:
                # First try to get existing symbol
                existing_stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(existing_stmt)
                existing_symbol = result.scalar_one_or_none()

                if existing_symbol:
                    if existing_symbol.first_trade_time is None:
                        existing_symbol.first_trade_time = symbol.launch_time
                        await session.flush()
                    logger.debug(
                        f"Retrieved symbol {symbol.name} for {symbol.exchange} "
                        f"with launch time {TimeUtils.from_timestamp(symbol.launch_time)}"
                    )
                    return existing_symbol

                # Create new symbol
                new_symbol = Symbol(
                    name=symbol.name,
                    exchange=symbol.exchange,
                    first_trade_time=symbol.launch_time
                )
                session.add(new_symbol)
                await session.flush()
                await session.refresh(new_symbol)

                logger.debug(
                    f"Created new symbol {symbol.name} for {symbol.exchange} "
                    f"with launch time {TimeUtils.from_timestamp(symbol.launch_time)}"
                )
                return new_symbol

        except Exception as e:
            logger.error(f"Error in get_or_create: {e}")
            raise RepositoryError(f"Failed to get or create symbol: {str(e)}")

    async def get_symbol(self, symbol: SymbolInfo) -> Optional[Symbol]:
        """
        Retrieve a symbol by its name and exchange.

        Args:
            symbol (SymbolInfo): The symbol information to search for.

        Returns:
            Optional[Symbol]: The found Symbol entity or None if not found.

        Raises:
            RepositoryError: If there's an error during the retrieval.
        """
        try:
            async with self.db_service.session() as session:
                # If exchange provided, get exact match
                stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(stmt)
                return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"Error getting symbol by name: {e}")
            raise RepositoryError(f"Failed to get symbol by name: {str(e)}")

    async def get_symbols_with_stats(self, exchange: ExchangeName) -> List[dict]:
        """
        Get all symbols from a specific exchange with their associated statistics.

        Args:
            exchange (ExchangeName): The name of the exchange to get symbols from.

        Returns:
            List[dict]: A list of dictionaries containing symbol information and statistics.

        Raises:
            RepositoryError: If there's an error retrieving the statistics.
        """
        try:
            async with self.db_service.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
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

    async def delete_symbol(self, symbol: SymbolInfo) -> None:
        """
        Delete a symbol and its associated data from the database.

        Args:
            symbol (SymbolInfo): The symbol information to delete.

        Raises:
            RepositoryError: If there's an error during the deletion process.
        """
        try:
            async with self.db_service.session() as session:
                # Find the symbol record
                stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(stmt)
                symbol_record = result.scalar_one_or_none()

                if symbol_record:
                    await session.delete(symbol_record)
                    await session.flush()
                    logger.info(f"Deleted symbol {symbol.name} from {symbol.exchange}")
                else:
                    logger.warning(f"Symbol {symbol.name} from {symbol.exchange} not found for deletion")

        except Exception as e:
            logger.error(f"Error deleting symbol: {e}")
            raise RepositoryError(f"Failed to delete symbol: {str(e)}")