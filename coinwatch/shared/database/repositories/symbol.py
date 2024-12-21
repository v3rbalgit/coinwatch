from sqlalchemy import select, and_, text

from shared.core.models import SymbolModel
from shared.database.connection import DatabaseConnection, IsolationLevel
from shared.database.models import Symbol
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup
from shared.utils.time import from_timestamp


class SymbolRepository:
    """Repository for managing Symbol entities in the database."""

    def __init__(self, db_service: DatabaseConnection):
        self.db_service = db_service
        self.logger = LoggerSetup.setup(__class__.__name__)

    async def get_symbol(self, symbol: SymbolModel) -> SymbolModel | None:
        """
        Retrieve a symbol by its information.

        Args:
            symbol (SymbolModel): The symbol information to search by.

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

                return symbol if existing_symbol else None

        except Exception as e:
            self.logger.error(f"Error getting symbol: {e}")
            raise RepositoryError(f"Failed to get symbol: {str(e)}")

    async def create_symbol(self, symbol: SymbolModel) -> bool:
        """
        Create a new symbol in the database.

        Args:
            symbol (SymbolModel): The symbol information to create.

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
                    self.logger.debug(f"Symbol {symbol.name} already exists for {symbol.exchange}")
                    return False

                # Create new symbol with all fields
                new_symbol = Symbol(
                    name=symbol.name,
                    exchange=symbol.exchange,
                    launch_time=symbol.launch_time,
                    base_asset=symbol.base_asset,
                    quote_asset=symbol.quote_asset,
                    price_scale=symbol.price_scale,
                    tick_size=symbol.tick_size,
                    qty_step=symbol.qty_step,
                    max_qty=symbol.max_qty,
                    min_notional=symbol.min_notional,
                    max_leverage=symbol.max_leverage,
                    funding_interval=symbol.funding_interval
                )
                session.add(new_symbol)
                await session.flush()
                await session.refresh(new_symbol)

                self.logger.debug(
                    f"Created new symbol {symbol.name} for {symbol.exchange} "
                    f"with launch time {from_timestamp(symbol.launch_time).strftime("%d-%m-%Y %H:%M:%S")}"
                )
                return True

        except Exception as e:
            self.logger.error(f"Error creating symbol: {e}")
            raise RepositoryError(f"Failed to create symbol: {str(e)}")

    async def get_symbols_with_stats(self, exchange: str) -> list[dict]:
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
                            interval,
                            count(*) as kline_count,
                            min(timestamp) as first_kline,
                            max(timestamp) as last_kline
                        FROM kline_data
                        GROUP BY symbol_id, interval
                    )
                    SELECT
                        s.id,
                        s.name,
                        s.exchange,
                        s.launch_time,
                        s.base_asset,
                        s.quote_asset,
                        s.price_scale,
                        s.tick_size,
                        s.qty_step,
                        s.max_qty,
                        s.min_notional,
                        s.max_leverage,
                        s.funding_interval,
                        json_agg(json_build_object(
                            'interval', st.interval,
                            'kline_count', st.kline_count,
                            'first_kline', st.first_kline,
                            'last_kline', st.last_kline
                        )) as interval_stats
                    FROM symbols s
                    LEFT JOIN stats st ON s.id = st.symbol_id
                    WHERE s.exchange = :exchange
                    GROUP BY
                        s.id, s.name, s.exchange, s.launch_time,
                        s.base_asset, s.quote_asset, s.price_scale,
                        s.tick_size, s.qty_step, s.max_qty,
                        s.min_notional, s.max_leverage, s.funding_interval
                    ORDER BY s.name;
                """)

                result = await session.execute(stmt, {"exchange": exchange})
                return [dict(row) for row in result]

        except Exception as e:
            self.logger.error(f"Error getting symbol stats: {e}")
            raise RepositoryError(f"Failed to get symbol statistics: {str(e)}")

    async def delete_symbol(self, symbol: SymbolModel) -> None:
        """
        Delete a symbol and its associated data from the database.

        Args:
            symbol (SymbolModel): The symbol information to delete.

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
                    self.logger.info(f"Deleted symbol {symbol.name} from {symbol.exchange}")
                else:
                    self.logger.warning(f"Symbol {symbol.name} from {symbol.exchange} not found for deletion")

        except Exception as e:
            self.logger.error(f"Error deleting symbol: {e}")
            raise RepositoryError(f"Failed to delete symbol: {str(e)}")
