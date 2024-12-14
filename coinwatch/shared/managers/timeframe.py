# src/managers/timeframe.py

from datetime import timedelta
from typing import List, Optional, Set

from shared.core.exceptions import ValidationError
from shared.core.models import KlineData, SymbolInfo
from shared.database.repositories.kline import KlineRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, GapMessage
from shared.utils.domain_types import Timeframe
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

class TimeframeManager:
    """
    Manages timeframe calculations and data access.

    Responsibilities:
    - Provides access to kline data across different timeframes
    - Manages continuous aggregates for common timeframes
    - Handles timeframe calculations and validations

    The manager uses TimescaleDB continuous aggregates for common timeframes
    (1h, 4h, 1d) and provides on-demand calculation for other timeframes.
    All calculations maintain proper UTC time boundaries.
    """

    def __init__(self,
                 message_broker: MessageBroker,
                 kline_repository: KlineRepository,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5):
        self.message_broker = message_broker
        self.kline_repository = kline_repository
        self.base_timeframe = base_timeframe

        # Common timeframes stored as continuous aggregates
        self._stored_timeframes = {
            tf for tf in Timeframe if tf.is_stored_timeframe()
        }

        # Cache of valid higher timeframes
        self._valid_timeframes: Optional[Set[Timeframe]] = None

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
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
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
                start_time = end_time - (limit * timeframe.to_milliseconds())
            elif start_time is None:
                # If no start_time and no limit, use a default period (e.g., last day)
                start_time = end_time - (24 * 60 * 60 * 1000)  # Last 24 hours

            # Now we can be sure both timestamps are not None
            aligned_start = self._align_to_timeframe(start_time, timeframe)
            aligned_end = self._align_to_timeframe(end_time, timeframe, round_up=True)

            # At this point, aligned_start and aligned_end cannot be None
            assert aligned_start is not None and aligned_end is not None

            # Check for data gaps
            gaps = await self._check_data_gaps(symbol, aligned_start, aligned_end)

            if gaps:
                await self._fill_data_gaps(symbol, gaps)

            # Choose appropriate repository method based on timeframe
            if timeframe == self.base_timeframe:
                # Get base timeframe data directly
                return await self.kline_repository.get_base_klines(
                    symbol,
                    timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )
            elif timeframe in self._stored_timeframes:
                # Get data from continuous aggregates
                return await self.kline_repository.get_stored_klines(
                    symbol,
                    timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )
            else:
                # Calculate timeframe on demand
                return await self.kline_repository.get_calculated_klines(
                    symbol,
                    timeframe,
                    self.base_timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )

        except Exception as e:
            logger.error(
                f"Error getting {timeframe.value} klines for {symbol}: {e}"
            )
            raise

    def _align_to_timeframe(self,
                           timestamp: Optional[int],
                           timeframe: Timeframe,
                           round_up: bool = False) -> Optional[int]:
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
                              start_time: int,
                              end_time: int) -> List[tuple[int, int]]:
        """Check for gaps in base timeframe data"""
        return await self.kline_repository.get_data_gaps(
            symbol,
            self.base_timeframe,
            start_time,
            end_time
        )

    async def _fill_data_gaps(self, symbol: SymbolInfo, gaps: List[tuple[int, int]]) -> None:
        """Fill detected data gaps"""
        if not gaps:
            return

        await self.message_broker.publish(MessageType.GAP_DETECTED,
                                          GapMessage(
                                              service="market_data",
                                              type=MessageType.GAP_DETECTED,
                                              timestamp=TimeUtils.get_current_timestamp(),
                                              symbol=symbol.name,
                                              exchange=symbol.exchange,
                                              gaps=gaps,
                                              timeframe=self.base_timeframe.value
                                          ).model_dump())
