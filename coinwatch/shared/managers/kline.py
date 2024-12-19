from shared.core.enums import Interval
from shared.core.exceptions import ValidationError
from shared.core.models import KlineModel, SymbolModel
from shared.database.repositories.kline import KlineRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, GapMessage
from shared.utils.logger import LoggerSetup
import shared.utils.time as TimeUtils
from shared.utils.cache import RedisCache, redis_cached


class KlineManager:
    """
    Manages candlestick calculations and data access.

    Responsibilities:
    - Provides access to kline data across different intervals
    - Manages continuous aggregates for common intervals
    - Handles interval calculations and validations
    - Caches frequently accessed kline data in Redis
    """

    MAX_LIMIT = 1000  # Maximum number of klines that can be requested

    def __init__(self,
                 message_broker: MessageBroker,
                 kline_repository: KlineRepository,
                 redis_url: str,
                 base_interval: Interval = Interval.MINUTE_5):
        self.message_broker = message_broker
        self.kline_repository = kline_repository
        self.base_interval = base_interval
        self.cache = RedisCache(redis_url, namespace="kline")

        # Common intervals stored as continuous aggregates
        self._stored_intervals = {
            tf for tf in Interval if tf.is_stored_interval()
        }

        # Cache of valid higher intervals
        self._valid_intervals: set[Interval] | None = None

        self.logger = LoggerSetup.setup(__class__.__name__)

    @property
    def valid_intervals(self) -> set[Interval]:
        """Get all valid intervals that can be calculated from base interval"""
        if self._valid_intervals is None:
            self._valid_intervals = self._calculate_valid_intervals()
        return self._valid_intervals

    def _calculate_valid_intervals(self) -> set[Interval]:
        """
        Calculate which intervals can be derived from base interval.
        An interval is valid if it's a multiple of the base interval.
        """
        base_ms = self.base_interval.to_milliseconds()
        valid = set()

        for interval in Interval:
            # Only include intervals larger than base
            if interval.to_milliseconds() >= base_ms:
                # Check if interval is cleanly divisible by base
                if interval.to_milliseconds() % base_ms == 0:
                    valid.add(interval)

        return valid

    def validate_interval(self, interval: Interval) -> None:
        """
        Validate that a interval can be calculated from base interval.

        Raises:
            ValidationError: If interval is invalid
        """
        if not isinstance(interval, Interval):
            raise ValidationError(f"Invalid interval type: {type(interval)}")

        if interval not in self.valid_intervals:
            raise ValidationError(
                f"Cannot calculate {interval.value} interval from "
                f"{self.base_interval.value} base interval"
            )

    @redis_cached[list[KlineModel]](ttl=60)
    async def get_klines(self,
                        symbol: SymbolModel,
                        interval: Interval,
                        start_time: int | None = None,
                        end_time: int | None = None,
                        limit: int | None = None) -> list[KlineModel]:
        """
        Get kline data for specified interval.
        Uses continuous aggregates for common intervals, calculates others on demand.

        Args:
            symbol: Symbol to get data for
            interval: Target interval
            start_time: Optional start time (None for latest)
            end_time: Optional end time (None for latest)
            limit: Optional limit on number of klines (max 1000, defaults to 1)

        Returns:
            List[KlineModel]: Kline data in ascending order

        Raises:
            ValidationError: If interval is invalid
            ServiceError: If data retrieval fails
        """
        try:
            # Validate interval
            self.validate_interval(interval)

            # Enforce max limit
            if limit is not None:
                limit = min(limit, self.MAX_LIMIT)

            # Set default end time to current interval boundary
            if end_time is None:
                end_time = TimeUtils.align_timestamp_to_interval(
                    TimeUtils.get_current_timestamp(),
                    interval
                )

            # Set start time based on end time and limit
            if start_time is None:
                # Calculate how many intervals to look back
                lookback = limit if limit is not None else 1
                start_time = end_time - (lookback * interval.to_milliseconds())

            # Align timestamps to interval boundaries
            aligned_start = TimeUtils.align_timestamp_to_interval(start_time, interval)
            # Only use round_up for user-provided end_time to include partial candles
            aligned_end = TimeUtils.align_timestamp_to_interval(
                end_time,
                interval,
                round_up=(end_time != TimeUtils.align_timestamp_to_interval(TimeUtils.get_current_timestamp(), interval))
            )

            # At this point, aligned_start and aligned_end cannot be None
            assert aligned_start is not None and aligned_end is not None

            # Check for data gaps
            gaps = await self._check_data_gaps(symbol, aligned_start, aligned_end)

            if gaps:
                await self._fill_data_gaps(symbol, gaps)

            # Choose appropriate repository method based on interval
            if interval == self.base_interval:
                # Get base interval data directly
                klines = await self.kline_repository.get_base_klines(
                    symbol,
                    interval,
                    aligned_start,
                    aligned_end,
                    limit
                )
            elif interval in self._stored_intervals:
                # Get data from continuous aggregates
                klines = await self.kline_repository.get_stored_klines(
                    symbol,
                    interval,
                    aligned_start,
                    aligned_end,
                    limit
                )
            else:
                # Calculate interval on demand
                klines = await self.kline_repository.get_calculated_klines(
                    symbol,
                    interval,
                    self.base_interval,
                    aligned_start,
                    aligned_end,
                    limit
                )

            return klines or []

        except Exception as e:
            self.logger.error(f"Error getting {interval.value} klines for {symbol}: {e}")
            raise

    async def _check_data_gaps(self,
                              symbol: SymbolModel,
                              start_time: int,
                              end_time: int) -> list[tuple[int, int]]:
        """Check for gaps in base interval data"""
        return await self.kline_repository.get_data_gaps(
            symbol,
            self.base_interval,
            start_time,
            end_time
        )

    async def _fill_data_gaps(self, symbol: SymbolModel, gaps: list[tuple[int, int]]) -> None:
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
                interval=self.base_interval.value
            ).model_dump())

    async def cleanup(self) -> None:
        """Cleanup resources"""
        await self.cache.close()