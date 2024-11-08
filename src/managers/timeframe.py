# src/managers/timeframe.py

import asyncio
from decimal import Decimal
from typing import List, Optional, Set
from sqlalchemy import text

from ..core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ..core.exceptions import ValidationError
from ..core.models import KlineData, SymbolInfo
from ..repositories.kline import KlineRepository
from ..utils.domain_types import Timeframe, Timestamp
from ..utils.logger import LoggerSetup
from ..utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class TimeframeManager:
    """
    Manages timeframe calculations and data access.

    Responsibilities:
    - Provides access to kline data across different timeframes
    - Manages continuous aggregates for common timeframes
    - Handles timeframe calculations and validations
    - Responds to market data updates via coordinator

    The manager uses TimescaleDB continuous aggregates for common timeframes
    (1h, 4h, 1d) and provides on-demand calculation for other timeframes.
    All calculations maintain proper UTC time boundaries.
    """

    def __init__(self,
                 kline_repository: KlineRepository,
                 coordinator: ServiceCoordinator,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5):
        self.coordinator = coordinator
        self.kline_repository = kline_repository
        self.base_timeframe = base_timeframe

        # Common timeframes stored as continuous aggregates
        self._stored_timeframes = {
            Timeframe.HOUR_1,
            Timeframe.HOUR_4,
            Timeframe.DAY_1
        }

        # Cache of valid higher timeframes
        self._valid_timeframes: Optional[Set[Timeframe]] = None

        # Register command handlers
        asyncio.create_task(self._register_command_handlers())

    @property
    def valid_timeframes(self) -> Set[Timeframe]:
        """Get all valid timeframes that can be calculated from base timeframe"""
        if self._valid_timeframes is None:
            self._valid_timeframes = self._calculate_valid_timeframes()
        return self._valid_timeframes

    def _calculate_valid_timeframes(self) -> Set[Timeframe]:
        """
        Calculate which timeframes can be derived from base timeframe.
        A timeframe is valid if it's a multiple of the base timeframe.
        """
        base_ms = self.base_timeframe.to_milliseconds()
        valid = set()

        for timeframe in Timeframe:
            # Only include timeframes larger than base
            if timeframe.to_milliseconds() >= base_ms:
                # Check if timeframe is cleanly divisible by base
                if timeframe.to_milliseconds() % base_ms == 0:
                    valid.add(timeframe)

        return valid

    def validate_timeframe(self, timeframe: Timeframe) -> None:
        """
        Validate that a timeframe can be calculated from base timeframe.

        Raises:
            ValidationError: If timeframe is invalid
        """
        if timeframe not in self.valid_timeframes:
            raise ValidationError(
                f"Cannot calculate {timeframe.value} timeframe from "
                f"{self.base_timeframe.value} base timeframe"
            )

    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[Timestamp] = None,
                        end_time: Optional[Timestamp] = None,
                        limit: Optional[int] = None) -> List[KlineData]:
        """
        Get kline data for specified timeframe.
        Uses continuous aggregates for common timeframes, calculates others on demand.

        Args:
            symbol: Symbol to get data for
            timeframe: Target timeframe
            start_time: Optional start time (None for latest)
            end_time: Optional end time (None for latest)
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order

        Raises:
            ValidationError: If timeframe is invalid
            ServiceError: If data retrieval fails
        """
        try:
            # Validate timeframe
            self.validate_timeframe(timeframe)

            # Set default time range if not provided
            if end_time is None:
                end_time = TimeUtils.get_current_timestamp()

            if start_time is None and limit:
                # Calculate start time based on limit
                start_time = Timestamp(
                    end_time - (limit * timeframe.to_milliseconds())
                )
            elif start_time is None:
                # If no start_time and no limit, use a default period (e.g., last day)
                start_time = Timestamp(end_time - (24 * 60 * 60 * 1000))  # Last 24 hours

            # Now we can be sure both timestamps are not None
            aligned_start = self._align_to_timeframe(start_time, timeframe)
            aligned_end = self._align_to_timeframe(end_time, timeframe, round_up=True)

            # At this point, aligned_start and aligned_end cannot be None
            assert aligned_start is not None and aligned_end is not None

            # Check for data gaps
            gaps = await self._check_data_gaps(
                symbol,
                aligned_start,
                aligned_end
            )

            if gaps:
                await self._handle_data_gaps(symbol, gaps)

            # Check if this is a stored timeframe
            if timeframe in self._stored_timeframes:
                return await self._get_stored_klines(
                    symbol,
                    timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )

            # For non-stored timeframes, calculate on demand
            return await self._calculate_klines(
                symbol,
                timeframe,
                aligned_start,
                aligned_end,
                limit
            )

        except Exception as e:
            logger.error(
                f"Error getting {timeframe.value} klines for {symbol}: {e}"
            )
            raise

    async def _register_command_handlers(self) -> None:
        """Register handlers for market data commands"""
        handlers = {
            MarketDataCommand.COLLECTION_COMPLETE: self._handle_collection_complete,
            MarketDataCommand.SYNC_COMPLETE: self._handle_sync_complete
        }

        for command, handler in handlers.items():
            await self.coordinator.register_handler(command, handler)
            logger.debug(f"Registered handler for {command.value}")

    async def _handle_collection_complete(self, command: Command) -> None:
        """Handle completion of historical data collection"""
        try:
            symbol = command.params['symbol']
            start_time = command.params['start_time']
            end_time = command.params['end_time']

            # Trigger recalculation of stored timeframes
            for timeframe in self._stored_timeframes:
                if timeframe in self.valid_timeframes:
                    await self._refresh_continuous_aggregate(
                        symbol,
                        timeframe,
                        start_time,
                        end_time
                    )
        except Exception as e:
            logger.error(f"Error handling collection complete: {e}")

    async def _handle_sync_complete(self, command: Command) -> None:
        """Handle completion of real-time data sync"""
        try:
            symbol = command.params['symbol']
            sync_time = command.params['sync_time']

            # For real-time updates, we only need to process the latest period
            interval_ms = self.base_timeframe.to_milliseconds()
            start_time = Timestamp(sync_time - interval_ms)

            # Refresh stored timeframes
            for timeframe in self._stored_timeframes:
                if timeframe in self.valid_timeframes:
                    await self._refresh_continuous_aggregate(
                        symbol,
                        timeframe,
                        start_time,
                        sync_time
                    )
        except Exception as e:
            logger.error(f"Error handling sync complete: {e}")

    async def _get_stored_klines(self,
                                symbol: SymbolInfo,
                                timeframe: Timeframe,
                                start_time: Timestamp,
                                end_time: Timestamp,
                                limit: Optional[int] = None) -> List[KlineData]:
        """
        Get klines from continuous aggregate views.

        Args:
            symbol: Symbol to get data for
            timeframe: Target timeframe
            start_time: Start timestamp
            end_time: End timestamp
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order
        """
        async with self.kline_repository.get_session() as session:
            # Query the appropriate continuous aggregate view
            stmt = text("""
                SELECT
                    k.start_time,
                    k.open_price,
                    k.high_price,
                    k.low_price,
                    k.close_price,
                    k.volume,
                    k.turnover,
                    s.name as symbol,
                    k.timeframe
                FROM kline_data k
                JOIN symbols s ON k.symbol_id = s.id
                WHERE s.name = :symbol
                AND k.timeframe = :timeframe
                AND k.start_time BETWEEN :start_time AND :end_time
                ORDER BY k.start_time DESC
                LIMIT :limit
            """)

            result = await session.execute(
                stmt,
                {
                    "symbol": symbol.name,
                    "timeframe": timeframe.value,
                    "start_time": start_time,
                    "end_time": end_time,
                    "limit": limit or 2147483647
                }
            )

            return [
                KlineData(
                    timestamp=row.start_time,
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    symbol=row.symbol,
                    timeframe=row.timeframe
                )
                for row in result
            ]

    async def _calculate_klines(self,
                              symbol: SymbolInfo,
                              timeframe: Timeframe,
                              start_time: Timestamp,
                              end_time: Timestamp,
                              limit: Optional[int] = None) -> List[KlineData]:
        """
        Calculate klines on demand for non-stored timeframes.

        Args:
            symbol: Symbol to get data for
            timeframe: Target timeframe
            start_time: Start timestamp
            end_time: End timestamp
            limit: Optional limit on number of klines

        Returns:
            List[KlineData]: Kline data in ascending order
        """
        async with self.kline_repository.get_session() as session:
            stmt = text("""
                WITH aggregated AS (
                    SELECT
                        (EXTRACT(EPOCH FROM time_bucket(:bucket, to_timestamp(start_time/1000))) * 1000)::bigint as start_time,
                        first(open_price, start_time) as open_price,
                        max(high_price) as high_price,
                        min(low_price) as low_price,
                        last(close_price, start_time) as close_price,
                        sum(volume) as volume,
                        sum(turnover) as turnover
                    FROM kline_data k
                    JOIN symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol
                    AND k.timeframe = :base_timeframe
                    AND k.start_time BETWEEN :start_time AND :end_time
                    GROUP BY time_bucket(:bucket, to_timestamp(start_time/1000))
                )
                SELECT
                    a.*,
                    :symbol as symbol,
                    :timeframe as timeframe
                FROM aggregated a
                ORDER BY start_time DESC
                LIMIT :limit
            """)

            result = await session.execute(
                stmt,
                {
                    "symbol": symbol.name,
                    "timeframe": timeframe.value,
                    "base_timeframe": self.base_timeframe.value,
                    "start_time": start_time,
                    "end_time": end_time,
                    "bucket": f"{timeframe.to_milliseconds() // 1000} seconds",
                    "limit": limit or 2147483647  # 2^31 - 1 (max postgres integer limit)
                }
            )

            return [
                KlineData(
                    timestamp=row.start_time,
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    symbol=row.symbol,
                    timeframe=row.timeframe
                )
                for row in result
            ]

    def _align_to_timeframe(self,
                           timestamp: Optional[Timestamp],
                           timeframe: Timeframe,
                           round_up: bool = False) -> Optional[Timestamp]:
        """Align timestamp to timeframe boundary"""
        if timestamp is None:
            return None

        interval_ms = timeframe.to_milliseconds()
        ts = timestamp - (timestamp % interval_ms)

        if round_up and ts < timestamp:
            ts += interval_ms

        return Timestamp(ts)

    async def _check_data_gaps(self,
                              symbol: SymbolInfo,
                              start_time: Timestamp,
                              end_time: Timestamp) -> List[tuple[Timestamp, Timestamp]]:
        """Check for gaps in base timeframe data"""
        return await self.kline_repository.get_data_gaps(
            symbol,
            self.base_timeframe,
            start_time,
            end_time
        )

    async def _handle_data_gaps(self, symbol: SymbolInfo, gaps: List[tuple[Timestamp, Timestamp]]) -> None:
        """Report detected data gaps"""
        if not gaps:
            return

        await self.coordinator.execute(Command(
            type=MarketDataCommand.GAP_DETECTED,
            params={
                "symbol": symbol,
                "gaps": gaps,
                "timeframe": self.base_timeframe.value,
                "timestamp": TimeUtils.get_current_timestamp()
            }
        ))

    async def _refresh_continuous_aggregate(self,
                                          symbol: SymbolInfo,
                                          timeframe: Timeframe,
                                          start_time: Timestamp,
                                          end_time: Timestamp) -> None:
        """Refresh TimescaleDB continuous aggregate for given period"""
        async with self.kline_repository.get_session() as session:
            start_dt = TimeUtils.from_timestamp(start_time)
            end_dt = TimeUtils.from_timestamp(end_time)

            await session.execute(
                text(f"""
                CALL refresh_continuous_aggregate(
                    'kline_{timeframe.value}',
                    :start_dt,
                    :end_dt
                );
                """),
                {
                    "start_dt": start_dt,
                    "end_dt": end_dt
                }
            )