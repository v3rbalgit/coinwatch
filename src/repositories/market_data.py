# src/repositories/market_data.py

from datetime import datetime, timezone
from typing import List, Tuple
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from .base import Repository
from ..models.market import Symbol, Kline
from ..domain_types import SymbolName, ExchangeName, Timeframe, Timestamp
from ..core.exceptions import RepositoryError, ValidationError
from ..utils.logger import LoggerSetup
from ..utils.validation import MarketDataValidator

logger = LoggerSetup.setup(__name__)

class SymbolRepository(Repository[Symbol]):
    """Repository for Symbol operations"""

    def __init__(self, session: AsyncSession):
        super().__init__(session, Symbol)

    async def get_or_create(self, name: SymbolName, exchange: ExchangeName) -> Symbol:
        """Get existing symbol or create new one"""
        try:
            stmt = select(self.model_class).where(
                and_(Symbol.name == name, Symbol.exchange == exchange)
            )
            result = await self.session.execute(stmt)
            symbol = result.scalar_one_or_none()

            if symbol:
                return symbol

            symbol = await self.create(Symbol(name=name, exchange=exchange))
            logger.info(f"Created new symbol: {name} for {exchange}")
            return symbol

        except IntegrityError:
            await self.session.rollback()
            # Retry get in case of race condition
            stmt = select(self.model_class).where(
                and_(Symbol.name == name, Symbol.exchange == exchange)
            )
            result = await self.session.execute(stmt)
            symbol = result.scalar_one_or_none()
            if symbol:
                return symbol
            raise RepositoryError(f"Failed to get/create symbol: {name}")

    async def get_by_exchange(self, exchange: ExchangeName) -> List[Symbol]:
        """Get all symbols for an exchange"""
        try:
            stmt = select(self.model_class).where(Symbol.exchange == exchange)
            result = await self.session.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            raise RepositoryError(f"Failed to get symbols: {str(e)}")

class KlineRepository(Repository[Kline]):
    """Repository for Kline operations"""

    def __init__(self, session: AsyncSession):
        super().__init__(session, Kline)
        self.validator = MarketDataValidator()
        self._batch_size = 100

    async def get_latest_timestamp(self,
                                 symbol: SymbolName,
                                 timeframe: Timeframe,
                                 exchange: ExchangeName) -> Timestamp:
        try:
            stmt = select(func.max(Kline.start_time)).join(Symbol).where(
                and_(
                    Symbol.name == symbol,
                    Symbol.exchange == exchange,
                    Kline.timeframe == timeframe.value
                )
            )
            result = await self.session.execute(stmt)
            latest = result.scalar_one_or_none()

            if latest is None:
                start_time = Timestamp(int(
                    (datetime.now(timezone.utc).timestamp() - (30 * 24 * 3600)) * 1000
                ))
                logger.info(f"No data for {symbol} {timeframe.value}, starting from {datetime.fromtimestamp(start_time/1000)}")
                return start_time

            return Timestamp(latest)

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
                    self.session.add_all(kline_objects)
                    await self.session.flush()
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

            result = await self.session.execute(
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

            result = await self.session.execute(
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

