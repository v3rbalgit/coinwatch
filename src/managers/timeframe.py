# src/managers/timeframe.py

import asyncio
from datetime import timedelta
from decimal import Decimal
from typing import List, Optional, Set
from sqlalchemy import text

from ..services.database import IsolationLevel
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
            tf for tf in Timeframe if tf.is_stored_timeframe()
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
        async with self.kline_repository.db_service.get_session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
            start_dt = TimeUtils.from_timestamp(start_time)
            end_dt = TimeUtils.from_timestamp(end_time)

            stmt = text(f"""
                WITH aligned_klines AS (
                    SELECT
                        time_bucket(:bucket, timestamp) as bucket_timestamp,
                        first(open_price, timestamp) as open_price,
                        max(high_price) as high_price,
                        min(low_price) as low_price,
                        last(close_price, timestamp) as close_price,
                        sum(volume) as volume,
                        sum(turnover) as turnover,
                        s.name as symbol_name,
                        s.exchange as symbol_exchange,  -- Add exchange to uniquely identify symbol
                        k.timeframe
                    FROM {timeframe.continuous_aggregate_view} k
                    JOIN symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol_name
                    AND s.exchange = :exchange  -- Add exchange filter
                    AND k.timestamp BETWEEN :start_time AND :end_time
                    GROUP BY
                        time_bucket(:bucket, timestamp),
                        s.name,
                        s.exchange,  -- Include exchange in GROUP BY
                        k.timeframe
                    ORDER BY bucket_timestamp DESC
                    LIMIT :limit
                )
                SELECT
                    EXTRACT(EPOCH FROM bucket_timestamp) * 1000 as timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover,
                    symbol_name,
                    symbol_exchange,
                    timeframe
                FROM aligned_klines
                ORDER BY bucket_timestamp DESC;
            """)

            result = await session.execute(
                stmt,
                {
                    "symbol_name": symbol.name,
                    "exchange": symbol.exchange,  # Add exchange parameter
                    "start_time": start_dt,
                    "end_time": end_dt,
                    "bucket": timeframe.get_bucket_interval(),
                    "limit": limit or 2147483647
                }
            )

            return [
                KlineData(
                    timestamp=Timestamp(row.timestamp),
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    symbol=SymbolInfo(  # Reconstruct full SymbolInfo
                        name=row.symbol_name,
                        exchange=row.symbol_exchange,
                        base_asset=symbol.base_asset,  # Use from original symbol
                        quote_asset=symbol.quote_asset,
                        price_precision=symbol.price_precision,
                        qty_precision=symbol.qty_precision,
                        min_order_qty=symbol.min_order_qty,
                        launch_time=symbol.launch_time
                    ),
                    timeframe=Timeframe(row.timeframe)
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
        async with self.kline_repository.db_service.get_session(isolation_level=IsolationLevel.REPEATABLE_READ) as session:
            start_dt = TimeUtils.from_timestamp(start_time)
            end_dt = TimeUtils.from_timestamp(end_time)

            stmt = text("""
                WITH base_aligned AS (
                    SELECT
                        time_bucket(:bucket, k.timestamp) as bucket_timestamp,
                        first(open_price, timestamp) as open_price,
                        max(high_price) as high_price,
                        min(low_price) as low_price,
                        last(close_price, timestamp) as close_price,
                        sum(volume) as volume,
                        sum(turnover) as turnover,
                        s.name as symbol_name,
                        s.exchange as symbol_exchange,  -- Add exchange to uniquely identify symbol
                        :target_timeframe as timeframe,
                        count(*) as candle_count
                    FROM kline_data k
                    JOIN symbols s ON k.symbol_id = s.id
                    WHERE s.name = :symbol_name
                    AND s.exchange = :exchange  -- Add exchange filter
                    AND k.timeframe = :base_timeframe
                    AND k.timestamp BETWEEN :start_time AND :end_time
                    GROUP BY
                        time_bucket(:bucket, k.timestamp),
                        s.name,
                        s.exchange  -- Include exchange in GROUP BY
                    HAVING count(*) >= :min_candles
                    ORDER BY bucket_timestamp DESC
                    LIMIT :limit
                )
                SELECT
                    EXTRACT(EPOCH FROM bucket_timestamp) * 1000 as timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover,
                    symbol_name,
                    symbol_exchange,
                    timeframe,
                    candle_count
                FROM base_aligned
                ORDER BY bucket_timestamp DESC;
            """)

            min_candles = timeframe.to_milliseconds() // self.base_timeframe.to_milliseconds()

            result = await session.execute(
                stmt,
                {
                    "symbol_name": symbol.name,
                    "exchange": symbol.exchange,  # Add exchange parameter
                    "base_timeframe": self.base_timeframe.value,
                    "target_timeframe": timeframe.value,
                    "start_time": start_dt,
                    "end_time": end_dt,
                    "bucket": timeframe.get_bucket_interval(),
                    "min_candles": min_candles,
                    "limit": limit or 2147483647
                }
            )

            return [
                KlineData(
                    timestamp=Timestamp(row.timestamp),
                    open_price=Decimal(str(row.open_price)),
                    high_price=Decimal(str(row.high_price)),
                    low_price=Decimal(str(row.low_price)),
                    close_price=Decimal(str(row.close_price)),
                    volume=Decimal(str(row.volume)),
                    turnover=Decimal(str(row.turnover)),
                    symbol=SymbolInfo(  # Reconstruct full SymbolInfo
                        name=row.symbol_name,
                        exchange=row.symbol_exchange,
                        base_asset=symbol.base_asset,  # Use from original symbol
                        quote_asset=symbol.quote_asset,
                        price_precision=symbol.price_precision,
                        qty_precision=symbol.qty_precision,
                        min_order_qty=symbol.min_order_qty,
                        launch_time=symbol.launch_time
                    ),
                    timeframe=Timeframe(row.timeframe)
                )
                for row in result
            ]

    def _align_to_timeframe(self,
                           timestamp: Optional[Timestamp],
                           timeframe: Timeframe,
                           round_up: bool = False) -> Optional[Timestamp]:
        """
        Align timestamp to timeframe boundary ensuring proper interval alignment.

        Examples:
        - 15min: :00, :15, :30, :45
        - 30min: :00, :30
        - 1h: :00
        - 4h: :00, 04:00, 08:00, etc.
        - 1d: 00:00 UTC
        """
        if timestamp is None:
            return None

        # Convert to datetime for easier manipulation
        dt = TimeUtils.from_timestamp(timestamp)

        if timeframe == Timeframe.DAY_1:
            # Align to UTC midnight
            aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == Timeframe.WEEK_1:
            # Align to UTC midnight Monday
            aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            days_since_monday = dt.weekday()
            aligned -= timedelta(days=days_since_monday)
        else:
            # For minute-based timeframes
            minutes = int(timeframe.value) if timeframe.value.isdigit() else 0
            total_minutes = dt.hour * 60 + dt.minute
            aligned_minutes = (total_minutes // minutes) * minutes

            aligned = dt.replace(
                hour=aligned_minutes // 60,
                minute=aligned_minutes % 60,
                second=0,
                microsecond=0
            )

        if round_up and aligned < dt:
            if timeframe == Timeframe.WEEK_1:
                aligned += timedelta(days=7)
            elif timeframe == Timeframe.DAY_1:
                aligned += timedelta(days=1)
            else:
                aligned += timedelta(minutes=minutes)

        return TimeUtils.to_timestamp(aligned)

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
        if not timeframe.is_stored_timeframe():
            logger.debug(f"Skipping refresh for non-stored timeframe: {timeframe.value}")
            return

        async with self.kline_repository.db_service.get_session(isolation_level=IsolationLevel.SERIALIZABLE) as session:
            # Align to timeframe boundaries
            aligned_start = self._align_to_timeframe(start_time, timeframe, round_up=False)
            aligned_end = self._align_to_timeframe(end_time, timeframe, round_up=True)

            # Only proceed if we have valid timestamps
            if aligned_start is not None and aligned_end is not None:
                start_dt = TimeUtils.from_timestamp(aligned_start)
                end_dt = TimeUtils.from_timestamp(aligned_end)

                view_name = timeframe.continuous_aggregate_view
                if view_name:
                    await session.execute(
                        text("""
                        CALL refresh_continuous_aggregate(
                            :view_name,
                            :start_dt,
                            :end_dt
                        );
                        """),
                        {
                            "view_name": view_name,
                            "start_dt": start_dt,
                            "end_dt": end_dt
                        }
                    )
                    logger.debug(f"Refreshed continuous aggregate {view_name} for period "
                            f"{start_dt} to {end_dt}")
            else:
                logger.warning("Skipping continuous aggregate refresh due to invalid timestamps")