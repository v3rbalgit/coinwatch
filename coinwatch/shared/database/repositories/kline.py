from decimal import Decimal
from typing import List, Optional, Tuple
from sqlalchemy import select, and_, text
from sqlalchemy.dialects.postgresql import insert

from shared.core.enums import IsolationLevel, Interval
from shared.core.models import KlineData, SymbolInfo
from shared.database.connection import DatabaseConnection
from shared.database.models.market_data import Symbol, Kline
from shared.core.exceptions import RepositoryError
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils

logger = LoggerSetup.setup(__name__)


class KlineRepository:
    """
    Repository for Kline operations with TimescaleDB optimization.

    Handles database operations related to kline data, including insertion,
    retrieval, and management of time series data.
    """

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self._batch_size = 1000

    async def get_latest_timestamp(self, symbol: SymbolInfo) -> Optional[int]:
        """
        Get the latest timestamp for a symbol.

        Args:
            symbol (SymbolInfo): The symbol to query.

        Returns:
            Optional[int]: The latest timestamp, or None if no data exists
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                # First check if we have any data
                stmt = text("""
                    SELECT (EXTRACT(EPOCH FROM MAX(timestamp)) * 1000)::BIGINT as latest_time
                    FROM kline_data k
                    JOIN symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol_name
                    AND s.exchange = :exchange
                """)

                result = await session.execute(stmt, {
                    "symbol_name": symbol.name,
                    "exchange": symbol.exchange
                })

                latest = result.scalar_one_or_none()

                if latest is not None:
                    logger.debug(
                        f"Latest timestamp for {symbol.name} on {symbol.exchange}: "
                        f"{TimeUtils.from_timestamp(latest).strftime('%d-%m-%Y, %H:%M:%S')}"
                    )
                    return latest
                else:
                    logger.debug(f"No latest timestamp found for {symbol.name} on {symbol.exchange}")
                    return None

        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            raise RepositoryError(f"Failed to get latest timestamp: {str(e)}")

    async def get_base_klines(self,
                            symbol: SymbolInfo,
                            interval: Interval,
                            start_time: int,
                            end_time: int,
                            limit: Optional[int] = None) -> List[KlineData]:
        """
        Get kline data directly at base interval without any aggregation.

        Args:
            symbol: Symbol to get data for
            interval: Base interval
            start_time: Start timestamp
            end_time: End timestamp
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                stmt = text("""
                    SELECT
                        k.timestamp,
                        k.open_price,
                        k.high_price,
                        k.low_price,
                        k.close_price,
                        k.volume,
                        k.turnover,
                        s.name as symbol,
                        k.interval
                    FROM kline_data k
                    JOIN symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol
                    AND s.exchange = :exchange
                    AND k.interval = :interval
                    AND k.timestamp BETWEEN :start_time AND :end_time
                    ORDER BY k.timestamp DESC
                    LIMIT :limit
                """)

                result = await session.execute(
                    stmt,
                    {
                        "symbol": symbol.name,
                        "exchange": symbol.exchange,
                        "interval": interval.value,
                        "start_time": TimeUtils.from_timestamp(start_time),
                        "end_time": TimeUtils.from_timestamp(end_time),
                        "limit": limit or 2147483647
                    }
                )

            return [
                KlineData(
                    timestamp=TimeUtils.to_timestamp(row.timestamp),
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    interval=Interval(row.interval)
                )
                for row in result
            ]

        except Exception as e:
            logger.error(f"Error getting base klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to get base klines: {str(e)}")

    async def get_stored_klines(self,
                              symbol: SymbolInfo,
                              interval: Interval,
                              start_time: int,
                              end_time: int,
                              limit: Optional[int] = None) -> List[KlineData]:
        """
        Get kline data from continuous aggregate views.

        Args:
            symbol: Symbol to get data for
            interval: Target interval (must be a continuous aggregate interval)
            start_time: Start timestamp
            end_time: End timestamp
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                stmt = text(f"""
                    SELECT
                        EXTRACT(EPOCH FROM k.bucket) * 1000 AS timestamp,
                        k.open_price,
                        k.high_price,
                        k.low_price,
                        k.close_price,
                        k.volume,
                        k.turnover,
                        s.name AS symbol_name,
                        s.exchange AS symbol_exchange,
                        k.interval
                    FROM {interval.continuous_aggregate_view} k
                    JOIN market_data.symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol_name
                    AND s.exchange = :exchange
                    AND k.bucket BETWEEN :start_time AND :end_time
                    ORDER BY k.bucket DESC
                    LIMIT :limit
                """)

                result = await session.execute(
                    stmt,
                    {
                        "symbol_name": symbol.name,
                        "exchange": symbol.exchange,
                        "start_time": TimeUtils.from_timestamp(start_time),
                        "end_time": TimeUtils.from_timestamp(end_time),
                        "bucket": interval.get_bucket_interval(),
                        "limit": limit or 2147483647
                    }
                )

            return [
                KlineData(
                    timestamp=int(row.timestamp),
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    interval=Interval(row.interval)
                )
                for row in result
            ]

        except Exception as e:
            logger.error(f"Error getting stored klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to get stored klines: {str(e)}")

    async def get_calculated_klines(self,
                                  symbol: SymbolInfo,
                                  interval: Interval,
                                  base_interval: Interval,
                                  start_time: int,
                                  end_time: int,
                                  limit: Optional[int] = None) -> List[KlineData]:
        """
        Calculate klines on demand for non-stored intervals.

        Args:
            symbol: Symbol to get data for
            interval: Target interval to calculate
            base_interval: Base interval to calculate from
            start_time: Start timestamp
            end_time: End timestamp
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                stmt = text("""
                    WITH base_aligned AS (
                        SELECT
                            public.time_bucket(:bucket, k.timestamp) AS bucket_timestamp,
                            public.first(k.open_price, k.timestamp) AS open_price,
                            public.last(k.close_price, k.timestamp) AS close_price,
                            MAX(k.high_price) AS high_price,
                            MIN(k.low_price) AS low_price,
                            SUM(k.volume) AS volume,
                            SUM(k.turnover) AS turnover,
                            s.name AS symbol_name,
                            s.exchange AS symbol_exchange,
                            :target_interval AS interval,
                            COUNT(*) AS candle_count
                        FROM market_data.kline_data k
                        JOIN market_data.symbols s ON k.symbol_id = s.id
                        WHERE s.name = :symbol_name
                        AND s.exchange = :exchange
                        AND k.interval = :base_interval
                        AND k.timestamp BETWEEN :start_time AND :end_time
                        GROUP BY
                            public.time_bucket(:bucket, k.timestamp),
                            s.name,
                            s.exchange
                        HAVING COUNT(*) >= :min_candles
                        ORDER BY bucket_timestamp DESC
                        LIMIT :limit
                    )
                    SELECT
                        EXTRACT(EPOCH FROM bucket_timestamp) * 1000 AS timestamp,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        turnover,
                        symbol_name,
                        symbol_exchange,
                        interval,
                        candle_count
                    FROM base_aligned
                    ORDER BY bucket_timestamp DESC;
                """)

                min_candles = interval.to_milliseconds() // base_interval.to_milliseconds()

                result = await session.execute(
                    stmt,
                    {
                        "symbol_name": symbol.name,
                        "exchange": symbol.exchange,
                        "base_interval": base_interval.value,
                        "target_interval": interval.value,
                        "start_time": TimeUtils.from_timestamp(start_time),
                        "end_time": TimeUtils.from_timestamp(end_time),
                        "bucket": interval.get_bucket_interval(),
                        "min_candles": min_candles,
                        "limit": limit or 2147483647
                    }
                )

            return [
                KlineData(
                    timestamp=int(row.timestamp),
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    interval=Interval(row.interval)
                )
                for row in result
            ]

        except Exception as e:
            logger.error(f"Error calculating klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to calculate klines: {str(e)}")

    async def insert_batch(self, symbol: SymbolInfo, klines: List[KlineData]) -> int:
        """
        Insert a batch of klines using PostgreSQL bulk insert.

        Args:
            symbol (SymbolInfo): The symbol information for the klines.
            klines (List[KlineData]): A list of kline data.

        Returns:
            int: The number of klines successfully inserted.
        """
        try:
            inserted_count = 0
            async with self.db.session() as session:
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
                    valid_klines = [{
                                    "symbol_id": symbol_id,
                                    "interval": k.interval.value,
                                    "timestamp": TimeUtils.from_timestamp(k.timestamp),
                                    "open_price": k.open_price,
                                    "high_price": k.high_price,
                                    "low_price": k.low_price,
                                    "close_price": k.close_price,
                                    "volume": k.volume,
                                    "turnover": k.turnover
                                } for k in batch]

                    if valid_klines:
                        stmt = insert(Kline).values(valid_klines)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['symbol_id', 'interval', 'timestamp'],
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
                    f"Inserted {inserted_count} klines for {symbol.name} on {symbol.exchange}, "
                    f"last timestamp: {TimeUtils.from_timestamp(klines[-1].timestamp)}"
                )

            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to insert klines: {str(e)}")

    async def get_data_gaps(self,
                           symbol: SymbolInfo,
                           interval: Interval,
                           start_time: int,
                           end_time: int) -> List[Tuple[int, int]]:
        """
        Find gaps in time series data using TimescaleDB features.

        Args:
            symbol (SymbolInfo): The symbol to check for gaps.
            interval (Interval): The interval of the data.
            start_time (Timestamp): The start time of the range to check.
            end_time (Timestamp): The end time of the range to check.

        Returns:
            List[Tuple[Timestamp, Timestamp]]: A list of tuples representing the start and end of each gap.

        Raises:
            RepositoryError: If there's an error during the gap finding process.
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
                # Calculate expected interval in milliseconds
                expected_interval = interval.to_milliseconds()

                stmt = text("""
                    WITH time_series AS (
                        SELECT
                            EXTRACT(EPOCH FROM timestamp) * 1000 as ts_start,
                            EXTRACT(EPOCH FROM lead(timestamp) OVER (ORDER BY timestamp)) * 1000 as ts_next
                        FROM kline_data k
                        JOIN symbols s ON k.symbol_id = s.id
                        WHERE s.name = :symbol
                        AND s.exchange = :exchange
                        AND k.interval = :interval_value
                        AND k.timestamp BETWEEN :start_time AND :end_time
                    )
                    SELECT ts_start, ts_next
                    FROM time_series
                    WHERE (ts_next - ts_start) > (:expected_interval * 2)
                    ORDER BY ts_start;
                """)

                result = await session.execute(stmt, {
                    "symbol": symbol.name,
                    "exchange": symbol.exchange,
                    "interval_value": interval.value,
                    "start_time": TimeUtils.from_timestamp(start_time),
                    "end_time": TimeUtils.from_timestamp(end_time),
                    "expected_interval": expected_interval
                })

            return [(int(row.ts_start + expected_interval),
                        int(row.ts_next))
                    for row in result]

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            raise RepositoryError(f"Failed to find data gaps: {str(e)}")

    async def delete_symbol_data(self, symbol: SymbolInfo) -> None:
        """
        Delete all kline data for a specific symbol.

        Args:
            symbol (SymbolInfo): The symbol whose data should be deleted.

        Raises:
            RepositoryError: If there's an error during the deletion process.
        """
        try:
            async with self.db.session(isolation_level=IsolationLevel.SERIALIZABLE) as session:
                symbol_stmt = select(Symbol.id).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange
                    )
                )
                result = await session.execute(symbol_stmt)
                symbol_id = result.scalar_one_or_none()

                if symbol_id:
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
