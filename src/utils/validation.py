# src/utils/validation.py

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Union, Tuple

from .domain_types import Timestamp
from ..core.exceptions import ValidationError
from ..utils.time import TimeUtils
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MarketDataValidator:
    """
    Validates market data to ensure data integrity and consistency.

    This validator checks timestamps, numeric values, and price relationships
    in kline (candlestick) data. It includes checks for:
    - Valid timestamp ranges (not before exchange launch, not too far in future)
    - Numeric value validity (non-negative, correct types)
    - OHLC price relationship consistency

    Class Attributes:
        BYBIT_LAUNCH_DATE (int): Bybit exchange launch timestamp in milliseconds
        MAX_FUTURE_TOLERANCE (int): Maximum allowed seconds into future for timestamps
    """

    BYBIT_LAUNCH_DATE = TimeUtils.to_timestamp(datetime(2019, 11, 1, tzinfo=timezone.utc))
    MAX_FUTURE_TOLERANCE = 60  # seconds

    @classmethod
    def _validate_timestamp(cls, timestamp: Timestamp) -> None:
        """
        Validate timestamp is within acceptable range.

        Ensures timestamp:
        - Is correct type and format (integer, 13 digits)
        - Is not before exchange launch date
        - Is not too far in the future (within MAX_FUTURE_TOLERANCE)

        Args:
            timestamp: Epoch timestamp in milliseconds

        Raises:
            ValidationError: If timestamp fails any validation criteria
        """
        if not isinstance(timestamp, int):
            raise ValidationError(f"Invalid timestamp type: {type(timestamp)}, must be int")

        if len(str(abs(timestamp))) != 13:
            raise ValidationError(f"Invalid timestamp length: {len(str(abs(timestamp)))}, must be 13 digits")

        if timestamp < cls.BYBIT_LAUNCH_DATE:
            launch_date = datetime.fromtimestamp(cls.BYBIT_LAUNCH_DATE/1000, tz=timezone.utc)
            raise ValidationError(f"Timestamp before exchange launch ({launch_date})")

        current_time = TimeUtils.get_current_timestamp()
        dt = TimeUtils.from_timestamp(timestamp)
        max_allowed = TimeUtils.from_timestamp(current_time) + timedelta(seconds=cls.MAX_FUTURE_TOLERANCE)

        if dt > max_allowed:
            logger.warning(
                f"Timestamp too far in future: {(dt - TimeUtils.from_timestamp(current_time)).total_seconds()} seconds ahead"
            )
            raise ValidationError(
                f"Future timestamp not allowed: {dt} > {max_allowed}"
            )

    @classmethod
    def _validate_numeric(cls, name: str, value: Union[int, float]) -> Decimal:
        """
        Validate and convert numeric values to Decimal.

        Ensures value:
        - Is correct type (int or float)
        - Can be converted to Decimal
        - Is positive

        Args:
            name: Name of the field being validated (for error messages)
            value: Numeric value to validate

        Returns:
            Decimal: Validated and converted value

        Raises:
            ValidationError: If value fails validation criteria
        """
        if not isinstance(value, (int, float)):
            raise ValidationError(f"Invalid {name} type: {type(value)}")

        try:
            decimal_value = Decimal(str(value))
            if decimal_value < 0:
                raise ValidationError(f"Non-positive {name}: {value}")
            return decimal_value
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid {name} value: {value}")

    @classmethod
    def _validate_price_relationships(cls,
                                    open_price: Decimal,
                                    high_price: Decimal,
                                    low_price: Decimal,
                                    close_price: Decimal) -> None:
        """
        Validate OHLC price relationships follow market logic.

        Ensures:
        - Low price is lowest of all prices
        - High price is highest of all prices
        - Open and close prices are between high and low

        Args:
            open_price: Opening price
            high_price: Highest price
            low_price: Lowest price
            close_price: Closing price

        Raises:
            ValidationError: If price relationships are invalid
        """
        if not (low_price <= min(open_price, close_price) <=
                max(open_price, close_price) <= high_price):
            raise ValidationError(
                f"Invalid OHLC relationships: "
                f"O={open_price}, H={high_price}, L={low_price}, C={close_price}"
            )

    @classmethod
    def validate_kline(cls,
                      timestamp: Timestamp,
                      open_price: float,
                      high_price: float,
                      low_price: float,
                      close_price: float,
                      volume: float,
                      turnover: float) -> Tuple[bool, Tuple[Decimal, ...]]:
        """
        Validate complete kline (candlestick) data.

        Performs comprehensive validation of all kline components and
        converts numeric values to Decimal type for consistent precision.

        Args:
            timestamp: Kline timestamp in milliseconds
            open_price: Opening price
            high_price: Highest price during period
            low_price: Lowest price during period
            close_price: Closing price
            turnover: Trading turnover value
            volume: Trading volume

        Returns:
            Tuple containing:
                - bool: True if validation successful
                - Tuple[Decimal, ...]: Converted values in order:
                  (open, high, low, close, volume, turnover)

        Raises:
            ValidationError: If any validation check fails

        Example:
            >>> validator = MarketDataValidator()
            >>> success, values = validator.validate_kline(
            ...     timestamp=1635724800000,  # 2021-11-01 00:00:00
            ...     open_price=60000.0,
            ...     high_price=61000.0,
            ...     low_price=59000.0,
            ...     close_price=60500.0,
            ...     turnover=1000000.0,
            ...     volume=16.7
            ... )
        """
        try:
            # Timestamp validation
            cls._validate_timestamp(timestamp)

            # Convert and validate prices
            open_dec = cls._validate_numeric('open', open_price)
            high_dec = cls._validate_numeric('high', high_price)
            low_dec = cls._validate_numeric('low', low_price)
            close_dec = cls._validate_numeric('close', close_price)
            volume_dec = cls._validate_numeric('volume', volume)
            turnover_dec = cls._validate_numeric('turnover', turnover)

            # Validate relationships
            cls._validate_price_relationships(open_dec, high_dec, low_dec, close_dec)

            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

            return True, (open_dec, high_dec, low_dec, close_dec, volume_dec, turnover_dec)

        except ValidationError:
            raise
        except Exception as e:
            logger.error("Unexpected validation error", exc_info=e)
            raise ValidationError(f"Validation failed: {str(e)}")


# Code for setting the MAX_FUTURE_TOLERANCE dynamically based on observed latency:
# class MarketDataValidator:
#     def __init__(self):
#         self._latency_samples = []
#         self._max_observed_latency = 0

#     def update_latency(self, latency: float) -> None:
#         self._latency_samples.append(latency)
#         if len(self._latency_samples) > 100:
#             self._latency_samples.pop(0)
#         self._max_observed_latency = max(self._latency_samples) * 2

#     @property
#     def future_tolerance(self) -> int:
#         return max(60, min(300, int(self._max_observed_latency)))