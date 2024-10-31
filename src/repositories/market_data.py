# src/repositories/market_data.py

from datetime import datetime, timezone
from typing import List, Tuple
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.exc import IntegrityError

from src.utils.time import from_timestamp, get_current_timestamp, get_past_timestamp

from .base import Repository
from ..models.market import Symbol, Kline
from ..utils.domain_types import SymbolName, ExchangeName, Timeframe, Timestamp
from ..core.exceptions import RepositoryError, ValidationError
from ..utils.logger import LoggerSetup
from ..utils.validation import MarketDataValidator

logger = LoggerSetup.setup(__name__)

class SymbolRepository(Repository[Symbol]):
    """Repository for Symbol operations"""

    def __init__(self, session_factory: async_sessionmaker):
        super().__init__(session_factory, Symbol)

    async def get_or_create(self, name: SymbolName, exchange: ExchangeName) -> Symbol:
        """Get existing symbol or create new one"""
        try:
            async with self.get_session() as session:
                stmt = select(self.model_class).where(
                    and_(Symbol.name == name, Symbol.exchange == exchange)
                )
                result = await session.execute(stmt)
                symbol = result.scalar_one_or_none()

                if symbol:
                    return symbol

                symbol = Symbol(name=name, exchange=exchange)
                session.add(symbol)
                await session.flush()
                await session.refresh(symbol)
                logger.info(f"Created new symbol: {name} for {exchange}")
                return symbol

        except IntegrityError:
            # Retry get in case of race condition
            async with self.get_session() as session:
                stmt = select(self.model_class).where(
                    and_(Symbol.name == name, Symbol.exchange == exchange)
                )
                result = await session.execute(stmt)
                symbol = result.scalar_one_or_none()
                if symbol:
                    return symbol
                raise RepositoryError(f"Failed to get/create symbol: {name}")

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

class KlineRepository(Repository[Kline]):
    """Repository for Kline operations"""

    def __init__(self, session_factory: async_sessionmaker):
        super().__init__(session_factory, Kline)
        self.validator = MarketDataValidator()
        self._batch_size = 100

    async def get_latest_timestamp(self,
                                   symbol: SymbolName,
                                   timeframe: Timeframe,
                                   exchange: ExchangeName) -> Timestamp:
        """Get latest timestamp to check for updates"""
        try:
            async with self.get_session() as session:
                # Get last stored timestamp
                stmt = select(func.max(Kline.start_time)).join(Symbol).where(
                    and_(
                        Symbol.name == symbol,
                        Symbol.exchange == exchange,
                        Kline.timeframe == timeframe.value
                    )
                )
                result = await session.execute(stmt)
                latest = result.scalar_one_or_none()

                current_time = get_current_timestamp()

                if latest is None:
                    # First time fetching - start from 30 days ago
                    return Timestamp(get_past_timestamp(30))

                # Calculate next timestamp we should check
                # This should be latest + timeframe interval
                interval_ms = timeframe.to_milliseconds()
                next_timestamp = latest + interval_ms

                # Don't request future data
                if next_timestamp > current_time:
                    next_timestamp = current_time - (current_time % interval_ms)

                return Timestamp(next_timestamp)

        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            raise RepositoryError(f"Failed to get latest timestamp: {str(e)}")

    async def insert_batch(self,
                           symbol_id: int,
                           timeframe: Timeframe,
                           klines: List[Tuple]) -> int:
        try:
            inserted_count = 0
            for i in range(0, len(klines), self._batch_size):
                batch = klines[i:i + self._batch_size]
                kline_objects = []

                for k in batch:
                    try:
                        if self.validator.validate_kline(*k):
                            kline_objects.append(
                                Kline(
                                    symbol_id=symbol_id,
                                    timeframe=timeframe.value,
                                    start_time=k[0],
                                    open_price=k[1],
                                    high_price=k[2],
                                    low_price=k[3],
                                    close_price=k[4],
                                    volume=k[5],
                                    turnover=k[6]
                                )
                            )
                    except ValidationError as e:
                        logger.warning(f"Invalid kline data: {e}")
                        continue

                if kline_objects:
                    async with self.get_session() as session:
                        session.add_all(kline_objects)
                        await session.flush()
                        inserted_count += len(kline_objects)
                        logger.debug(f"Inserted batch of {len(kline_objects)} klines for symbol {symbol_id}")

            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting klines: {e}")
            raise RepositoryError(f"Failed to insert klines: {str(e)}")

    async def get_data_gaps(self,
                            symbol: SymbolName,
                            timeframe: Timeframe,
                            exchange: ExchangeName,
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
                        "symbol": symbol,
                        "exchange": exchange,
                        "timeframe": timeframe.value,
                        "start_time": start_time,
                        "end_time": end_time,
                        "interval_ms": interval_ms
                    }
                )

                gaps = [
                    (Timestamp(row.start_time + timeframe.to_milliseconds()),
                     Timestamp(row.next_start))
                    for row in result
                ]

                if gaps:
                    logger.warning(f"Found {len(gaps)} gaps for {symbol} {timeframe.value}")
                return gaps

        except Exception as e:
            logger.error(f"Error finding data gaps: {e}")
            raise RepositoryError(f"Failed to find data gaps: {str(e)}")

    async def delete_old_data(self,
                              symbol: SymbolName,
                              timeframe: Timeframe,
                              exchange: ExchangeName,
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
                        "symbol": symbol,
                        "exchange": exchange,
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
