# src/repositories/kline.py

from typing import List, Optional, Tuple
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.core.models import SymbolInfo
from src.utils.time import from_timestamp, get_past_timestamp

from .base import Repository
from ..models.market import Symbol, Kline
from ..utils.domain_types import Timeframe, Timestamp
from ..core.exceptions import RepositoryError, ValidationError
from ..utils.logger import LoggerSetup
from ..utils.validation import MarketDataValidator

logger = LoggerSetup.setup(__name__)

class KlineRepository(Repository[Kline]):
    """Repository for Kline operations"""

    def __init__(self, session_factory: async_sessionmaker):
        super().__init__(session_factory, Kline)
        self.validator = MarketDataValidator()
        self._batch_size = 1000

    async def get_latest_timestamp(self,
                                   symbol: SymbolInfo,
                                   timeframe: Timeframe) -> Timestamp:
        """Get latest timestamp to check for updates"""
        try:
            async with self.get_session() as session:
                # First get the symbol record to check first_trade_time
                symbol_stmt = select(Symbol).where(
                    and_(Symbol.name == symbol.name, Symbol.exchange == symbol.exchange)
                )
                symbol_result = await session.execute(symbol_stmt)
                symbol_record = symbol_result.scalar_one()

                # Get latest timestamp we have
                stmt = select(func.max(Kline.start_time)).join(Symbol).where(
                    and_(
                        Symbol.name == symbol.name,
                        Symbol.exchange == symbol.exchange,
                        Kline.timeframe == timeframe.value
                    )
                )
                result = await session.execute(stmt)
                latest = result.scalar_one_or_none()
                logger.debug(f"Latest timestamp in DB for {symbol}: {latest}")

                if latest is None:
                    # Start from first_trade_time if we have it
                    if symbol_record.first_trade_time:
                        start_time = symbol_record.first_trade_time
                        logger.info(
                            f"No '{timeframe.value}' data for {symbol.name} ({symbol.exchange}), "
                            f"starting from first trade at {from_timestamp(start_time)}"
                        )
                        return Timestamp(start_time)
                    else:
                        # Fallback to 30 days if no first_trade_time
                        start_time = get_past_timestamp(30)
                        logger.warning(
                            f"No first trade time for {symbol}, "
                            f"falling back to {from_timestamp(start_time)}"
                        )
                        return Timestamp(start_time)

                return Timestamp(latest)

        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            raise RepositoryError(f"Failed to get latest timestamp: {str(e)}")

    async def insert_batch(self,
                      symbol: SymbolInfo,
                      timeframe: Timeframe,
                      klines: List[Tuple]) -> int:
        """
        Insert a batch of klines for a symbol with UPSERT functionality.

        Args:
            symbol: Trading symbol name
            timeframe: Kline timeframe
            klines: List of kline data tuples

        Returns:
            int: Number of klines processed

        Raises:
            RepositoryError: If insert fails
        """
        try:
            inserted_count = 0

            async with self.get_session() as session:
                # Get the symbol record first
                symbol_stmt = select(Symbol).where(Symbol.name == symbol.name)
                result = await session.execute(symbol_stmt)
                symbol_record = result.scalar_one_or_none()

                if not symbol_record:
                    raise RepositoryError(f"Symbol not found: {symbol}")

                # Process klines in batches
                for i in range(0, len(klines), self._batch_size):
                    batch = klines[i:i + self._batch_size]
                    valid_klines = []

                    # Validate klines first
                    for k in batch:
                        try:
                            if self.validator.validate_kline(*k):
                                valid_klines.append({
                                    "symbol_id": symbol_record.id,
                                    "timeframe": timeframe.value,
                                    "start_time": k[0],
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
                        # Use UPSERT statement
                        stmt = text("""
                            INSERT INTO kline_data (
                                symbol_id, timeframe, start_time,
                                open_price, high_price, low_price, close_price,
                                volume, turnover
                            ) VALUES (
                                :symbol_id, :timeframe, :start_time,
                                :open_price, :high_price, :low_price, :close_price,
                                :volume, :turnover
                            )
                            ON DUPLICATE KEY UPDATE
                                open_price = VALUES(open_price),
                                high_price = VALUES(high_price),
                                low_price = VALUES(low_price),
                                close_price = VALUES(close_price),
                                volume = VALUES(volume),
                                turnover = VALUES(turnover)
                        """)

                        # Execute batch upsert
                        await session.execute(stmt, valid_klines)
                        await session.flush()
                        inserted_count += len(valid_klines)

                if klines:  # Only log if we had data to process
                    logger.debug(f"Processed {inserted_count} klines for {symbol}, last timestamp: {klines[-1][0]}")

                return inserted_count

        except Exception as e:
            logger.error(f"Error inserting klines for {symbol}: {e}")
            raise RepositoryError(f"Failed to insert klines for {symbol}: {str(e)}")

    async def get_data_gaps(self,
                            symbol: SymbolInfo,
                            timeframe: Timeframe,
                            start_time: Timestamp,
                            end_time: Timestamp) -> List[Tuple[Timestamp, Timestamp]]:
        try:
            async with self.get_session() as session:
                interval_ms = timeframe.to_milliseconds() * 2
                stmt = text("""
                    WITH time_series AS (
                        SELECT
                            k.start_time,
                            LEAD(k.start_time) OVER (ORDER BY k.start_time) as next_start
                        FROM kline_data k
                        JOIN symbols s ON k.symbol_id = s.id
                        WHERE s.name = :symbol
                        AND s.exchange = :exchange
                        AND k.timeframe = :timeframe
                        AND k.start_time BETWEEN :start_time AND :end_time
                    )
                    SELECT start_time, next_start
                    FROM time_series
                    WHERE next_start - start_time > :interval_ms
                """)

                result = await session.execute(
                    stmt,
                    {
                        "symbol": symbol.name,
                        "exchange": symbol.exchange,
                        "timeframe": timeframe.value,
                        "start_time": start_time,
                        "end_time": end_time,
                        "interval_ms": interval_ms
                    }
                )

                gaps = [
                    (Timestamp(row._mapping['start_time'] + timeframe.to_milliseconds()),
                     Timestamp(row._mapping['next_start']))
                    for row in result
                ]

                if gaps:
                    logger.warning(f"Found {len(gaps)} gaps for {symbol} {timeframe.value}")
                return gaps

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            raise RepositoryError(f"Failed to find data gaps: {str(e)}")

    async def delete_old_data(self,
                              symbol: SymbolInfo,
                              timeframe: Timeframe,
                              before_timestamp: Timestamp) -> int:
        try:
            async with self.get_session() as session:
                stmt = text("""
                    DELETE FROM kline_data k
                    USING symbols s
                    WHERE k.symbol_id = s.id
                    AND s.name = :symbol
                    AND s.exchange = :exchange
                    AND k.timeframe = :timeframe
                    AND k.start_time < :before_timestamp
                    RETURNING 1
                """)

                result = await session.execute(
                    stmt,
                    {
                        "symbol": symbol.name,
                        "exchange": symbol.exchange,
                        "timeframe": timeframe.value,
                        "before_timestamp": before_timestamp
                    }
                )

                deleted_count = len(result.fetchall())
                logger.info(f"Deleted {deleted_count} old records for {symbol} {timeframe.value}")
                return deleted_count

        except Exception as e:
            logger.error(f"Error deleting old data: {e}")
            raise RepositoryError(f"Failed to delete old data: {str(e)}")
