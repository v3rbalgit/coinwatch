from typing import List, Optional, Set

from shared.core.enums import Timeframe
from shared.core.exceptions import ValidationError
from shared.core.models import KlineData, SymbolInfo
from shared.database.repositories.kline import KlineRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, GapMessage
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils
from shared.utils.cache import RedisCache, redis_cached

logger = LoggerSetup.setup(__name__)

class TimeframeManager:
    """
    Manages timeframe calculations and data access.

    Responsibilities:
    - Provides access to kline data across different timeframes
    - Manages continuous aggregates for common timeframes
    - Handles timeframe calculations and validations
    - Caches frequently accessed kline data in Redis
    """

    def __init__(self,
                 message_broker: MessageBroker,
                 kline_repository: KlineRepository,
                 redis_url: str,
                 base_timeframe: Timeframe = Timeframe.MINUTE_5):
        self.message_broker = message_broker
        self.kline_repository = kline_repository
        self.base_timeframe = base_timeframe
        self.cache = RedisCache(redis_url, namespace="timeframe")

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
        if not isinstance(timeframe, Timeframe):
            raise ValidationError(f"Invalid timeframe type: {type(timeframe)}")

        if timeframe not in self.valid_timeframes:
            raise ValidationError(
                f"Cannot calculate {timeframe.value} timeframe from "
                f"{self.base_timeframe.value} base timeframe"
            )

    @redis_cached[List[KlineData]](ttl=60)  # Cache for 1 minute since klines update frequently
    async def get_klines(self,
                        symbol: SymbolInfo,
                        timeframe: Timeframe,
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None,
                        limit: Optional[int] = 200) -> List[KlineData]:
        """
        Get kline data for specified timeframe.
        Uses continuous aggregates for common timeframes, calculates others on demand.

        Args:
            symbol: Symbol to get data for
            timeframe: Target timeframe
            start_time: Optional start time (None for latest)
            end_time: Optional end time (None for latest)
            limit: Optional limit on number of klines (default 200)

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
            aligned_start = TimeUtils.align_timestamp_to_interval(start_time, timeframe)
            aligned_end = TimeUtils.align_timestamp_to_interval(end_time, timeframe, round_up=True)

            # At this point, aligned_start and aligned_end cannot be None
            assert aligned_start is not None and aligned_end is not None

            # Check for data gaps
            gaps = await self._check_data_gaps(symbol, aligned_start, aligned_end)

            if gaps:
                await self._fill_data_gaps(symbol, gaps)

            # Choose appropriate repository method based on timeframe
            if timeframe == self.base_timeframe:
                # Get base timeframe data directly
                klines = await self.kline_repository.get_base_klines(
                    symbol,
                    timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )
            elif timeframe in self._stored_timeframes:
                # Get data from continuous aggregates
                klines = await self.kline_repository.get_stored_klines(
                    symbol,
                    timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )
            else:
                # Calculate timeframe on demand
                klines = await self.kline_repository.get_calculated_klines(
                    symbol,
                    timeframe,
                    self.base_timeframe,
                    aligned_start,
                    aligned_end,
                    limit
                )

            return klines or []

        except Exception as e:
            logger.error(f"Error getting {timeframe.value} klines for {symbol}: {str(e)}")
            raise

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

        await self.message_broker.publish(
            MessageType.GAP_DETECTED,
            GapMessage(
                service="market_data",
                type=MessageType.GAP_DETECTED,
                timestamp=TimeUtils.get_current_timestamp(),
                symbol=symbol.name,
                exchange=symbol.exchange,
                gaps=gaps,
                timeframe=self.base_timeframe.value
            ).model_dump())

    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.cache.close()