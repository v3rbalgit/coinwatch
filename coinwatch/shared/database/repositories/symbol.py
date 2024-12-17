# src/repositories/symbol.py

from typing import List, Optional
from sqlalchemy import select, and_, text

from shared.core.models import SymbolInfo
from shared.database.connection import DatabaseConnection, IsolationLevel
from shared.database.models import Symbol
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)


class SymbolRepository:
    """Repository for managing Symbol entities in the database."""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service

    async def get_symbol(self, symbol: SymbolInfo) -> Optional[Symbol]:
        """
        Retrieve a symbol by its information.

        Args:
            symbol (SymbolInfo): The symbol information to search by.

        Returns:
            Optional[Symbol]: The found Symbol entity or None if not found.

        Raises:
            RepositoryError: If there's an error during the retrieval.
        """
        try:
            async with self.db_service.session() as session:
                stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(stmt)
                existing_symbol = result.scalar_one_or_none()

                return existing_symbol

        except Exception as e:
            logger.error(f"Error getting symbol: {e}")
            raise RepositoryError(f"Failed to get symbol: {str(e)}")

    async def create_symbol(self, symbol: SymbolInfo) -> bool:
        """
        Create a new symbol in the database.

        Args:
            symbol (SymbolInfo): The symbol information to create.

        Returns:
            bool: True if creation was successful, False otherwise.

        Raises:
            RepositoryError: If there's an error during the creation.
        """
        try:
            async with self.db_service.session() as session:
                # Check if symbol already exists
                stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(stmt)
                if result.scalar_one_or_none() is not None:
                    logger.debug(f"Symbol {symbol.name} already exists for {symbol.exchange}")
                    return False

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
                return True

        except Exception as e:
            logger.error(f"Error creating symbol: {e}")
            raise RepositoryError(f"Failed to create symbol: {str(e)}")

    async def get_symbols_with_stats(self, exchange: str) -> List[dict]:
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
