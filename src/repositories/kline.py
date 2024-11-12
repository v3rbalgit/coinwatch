# src/repositories/kline.py

from typing import List, Tuple
from sqlalchemy import select, and_, text
from sqlalchemy.dialects.postgresql import insert

from..core.models import SymbolInfo
from ..services.database import DatabaseService, IsolationLevel
from .base import Repository
from ..models.market import Symbol, Kline
from ..utils.domain_types import Timeframe, Timestamp
from ..core.exceptions import RepositoryError, ValidationError
from ..utils.logger import LoggerSetup
from ..utils.validation import MarketDataValidator
from ..utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class KlineRepository(Repository[Kline]):
    """Repository for Kline operations with TimescaleDB optimization"""

    def __init__(self, db_service: DatabaseService):
        super().__init__(db_service, Kline)
        self.validator = MarketDataValidator()
        self._batch_size = 1000

    async def get_latest_timestamp(self,
                             symbol: SymbolInfo,
                             timeframe: Timeframe) -> Timestamp:
        """Get latest timestamp using TimescaleDB's last() function"""
        try:
            async with self.db_service.get_session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                symbol_stmt = select(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                symbol_result = await session.execute(symbol_stmt)
                symbol_record = symbol_result.scalar_one()

                stmt = text("""
                    SELECT COALESCE(
                        EXTRACT(EPOCH FROM MAX(timestamp)) * 1000,
                        :default_time
                    )::BIGINT as latest_time
                    FROM kline_data
                    WHERE symbol_id = :symbol_id
                    AND timeframe = :timeframe;
                """)

                default_time = symbol_record.first_trade_time

                result = await session.execute(stmt, {
                    "symbol_id": symbol_record.id,
                    "timeframe": timeframe.value,
                    "default_time": default_time
                })

                latest = result.scalar()
                if latest is None:
                    latest = default_time
                logger.debug(
                    f"Latest timestamp for {symbol.name}: {TimeUtils.from_timestamp(Timestamp(latest))}"
                )

                return Timestamp(latest)

        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            raise RepositoryError(f"Failed to get latest timestamp: {str(e)}")

    async def insert_batch(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        klines: List[Tuple]) -> int:
        """
        Insert a batch of klines using PostgreSQL bulk insert

        Uses COPY command for efficient bulk loading
        """
        try:
            inserted_count = 0
            async with self.db_service.get_session() as session:
                symbol_stmt = select(Symbol.id).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(symbol_stmt)
                symbol_id = result.scalar_one()

                # Process klines in batches
                for i in range(0, len(klines), self._batch_size):
                    batch = klines[i:i + self._batch_size]
                    valid_klines = []

                    # Validate klines
                    for k in batch:
                        try:
                            if self.validator.validate_kline(*k):
                                valid_klines.append({
                                    "symbol_id": symbol_id,
                                    "timeframe": timeframe.value,
                                    "timestamp": TimeUtils.from_timestamp(Timestamp(k[0])),  # Convert to datetime
                                    "open_price": k[1],
                                    "high_price": k[2],
                                    "low_price": k[3],
                                    "close_price": k[4],
                                    "volume": k[5],
                                    "turnover": k[6]
                                })
                        except ValidationError as e:
                            logger.warning(f"Invalid kline data for {symbol}: {e}")
                            continue

                    if valid_klines:
                        stmt = insert(Kline).values(valid_klines)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['symbol_id', 'timeframe', 'timestamp'],
                            set_=dict(
                                open_price=stmt.excluded.open_price,
                                high_price=stmt.excluded.high_price,
                                low_price=stmt.excluded.low_price,
                                close_price=stmt.excluded.close_price,
                                volume=stmt.excluded.volume,
                                turnover=stmt.excluded.turnover
                            )
                        )
                        await session.execute(stmt)
                        inserted_count += len(valid_klines)

                if klines:
                    logger.debug(
                        f"Processed {inserted_count} klines for {symbol}, "
                        f"last timestamp: {TimeUtils.from_timestamp(klines[-1][0])}"
                    )

                return inserted_count

        except Exception as e:
            logger.error(f"Error inserting klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to insert klines: {str(e)}")

    async def get_data_gaps(self,
                           symbol: SymbolInfo,
                           timeframe: Timeframe,
                           start_time: Timestamp,
                           end_time: Timestamp) -> List[Tuple[Timestamp, Timestamp]]:
        """Find gaps in time series data using TimescaleDB features"""
        try:
            async with self.db_service.get_session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                # Convert timestamps to datetime for the query
                start_dt = TimeUtils.from_timestamp(start_time)
                end_dt = TimeUtils.from_timestamp(end_time)

                stmt = text("""
                    WITH time_series AS (
                        SELECT
                            EXTRACT(EPOCH FROM timestamp) * 1000 as ts_start,
                            EXTRACT(EPOCH FROM lead(timestamp) OVER (ORDER BY timestamp)) * 1000 as ts_next,
                            :interval as expected_interval
                        FROM kline_data k
                        JOIN symbols s ON k.symbol_id = s.id
                        WHERE s.name = :symbol
                        AND s.exchange = :exchange
                        AND k.timeframe = :timeframe
                        AND k.timestamp BETWEEN :start_time AND :end_time
                    )
                    SELECT ts_start, ts_next
                    FROM time_series
                    WHERE (ts_next - ts_start) > (expected_interval * 2)
                    ORDER BY ts_start;
                """)

                result = await session.execute(stmt, {
                    "symbol": symbol.name,
                    "exchange": symbol.exchange,
                    "timeframe": timeframe.value,
                    "start_time": start_dt,
                    "end_time": end_dt,
                    "interval": timeframe.to_milliseconds()
                })

                return [(Timestamp(int(row.ts_start + timeframe.to_milliseconds())),
                         Timestamp(int(row.ts_next)))
                        for row in result]

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            raise RepositoryError(f"Failed to find data gaps: {str(e)}")

    async def delete_symbol_data(self, symbol: SymbolInfo) -> None:
        """Delete all kline data for a symbol"""
        try:
            async with self.db_service.get_session() as session:
                # Get the symbol ID first
                symbol_stmt = select(Symbol.id).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(symbol_stmt)
                symbol_id = result.scalar_one_or_none()

                if symbol_id:
                    # Delete all klines for this symbol
                    delete_stmt = text("""
                        DELETE FROM kline_data
                        WHERE symbol_id = :symbol_id
                    """)
                    await session.execute(delete_stmt, {"symbol_id": symbol_id})
                    await session.flush()

                    logger.info(f"Deleted all kline data for {symbol.name} from {symbol.exchange}")
                else:
                    logger.warning(f"No kline data found for {symbol.name} from {symbol.exchange}")

        except Exception as e:
            logger.error(f"Error deleting kline data: {e}")
            raise RepositoryError(f"Failed to delete kline data: {str(e)}")