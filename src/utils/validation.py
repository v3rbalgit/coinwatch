# src/utils/validation.py

import logging
from typing import List, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Base exception for validation errors."""
    pass

class PriceValidator:
    """Validates price and volume data."""

    @staticmethod
    def validate_price(price: float) -> bool:
        """
        Validate a price value.

        Args:
            price: Price value to validate

        Returns:
            bool: True if valid

        Raises:
            ValidationError: If price is invalid
        """
        try:
            if not isinstance(price, (int, float)):
                raise ValidationError("Price must be a number")

            # Convert to Decimal for precise comparison
            price_decimal = Decimal(str(price))

            if price_decimal <= 0:
                raise ValidationError("Price must be positive")

            # Check for reasonable number of decimal places
            decimal_places = len(str(price_decimal).split('.')[-1]) if '.' in str(price_decimal) else 0

            if decimal_places > 10:
                raise ValidationError("Price has too many decimal places")

            return True

        except InvalidOperation:
            raise ValidationError("Invalid price format")
        except Exception as e:
            raise ValidationError(f"Price validation failed: {str(e)}")

    @staticmethod
    def validate_volume(volume: float) -> bool:
        """
        Validate a volume value.

        Args:
            volume: Volume value to validate

        Returns:
            bool: True if valid

        Raises:
            ValidationError: If volume is invalid
        """
        try:
            if not isinstance(volume, (int, float)):
                raise ValidationError("Volume must be a number")

            volume_decimal = Decimal(str(volume))

            if volume_decimal < 0:
                raise ValidationError("Volume cannot be negative")

            return True

        except InvalidOperation:
            raise ValidationError("Invalid volume format")
        except Exception as e:
            raise ValidationError(f"Volume validation failed: {str(e)}")

class TimestampValidator:
    """Validates timestamp data."""

    @staticmethod
    def validate_timestamp(timestamp: int) -> bool:
        """
        Validate a timestamp value.

        Args:
            timestamp: Timestamp in milliseconds

        Returns:
            bool: True if valid

        Raises:
            ValidationError: If timestamp is invalid
        """
        try:
            if not isinstance(timestamp, int):
                raise ValidationError("Timestamp must be an integer")

            # Check if timestamp is in milliseconds (13 digits)
            if len(str(timestamp)) != 13:
                raise ValidationError("Timestamp must be in milliseconds (13 digits)")

            # Convert to datetime for range checking
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

            # Check if timestamp is in reasonable range
            now = datetime.now(timezone.utc)
            min_date = datetime(2015, 1, 1, tzinfo=timezone.utc)  # Bybit's launch

            if dt < min_date:
                raise ValidationError("Timestamp is before exchange launch")
            if dt > now + timedelta(minutes=1):  # Allow 1 minute future tolerance
                raise ValidationError("Timestamp is in the future")

            return True

        except Exception as e:
            raise ValidationError(f"Timestamp validation failed: {str(e)}")

class KlineValidator:
    """Validates complete kline records."""

    @staticmethod
    def validate_kline(kline_data: Tuple) -> bool:
        """
        Validate a complete kline record.

        Args:
            kline_data: Tuple of (timestamp, open, high, low, close, volume, turnover)

        Returns:
            bool: True if valid

        Raises:
            ValidationError: If kline data is invalid
        """
        try:
            if len(kline_data) != 7:
                raise ValidationError("Invalid kline data format")

            timestamp, open_price, high, low, close, volume, turnover = kline_data

            # Validate individual components
            TimestampValidator.validate_timestamp(timestamp)
            PriceValidator.validate_price(open_price)
            PriceValidator.validate_price(high)
            PriceValidator.validate_price(low)
            PriceValidator.validate_price(close)
            PriceValidator.validate_volume(volume)
            PriceValidator.validate_volume(turnover)

            # Validate price relationships
            if not (low <= min(open_price, close) <= max(open_price, close) <= high):
                raise ValidationError("Invalid price relationships in OHLC data")

            return True

        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"Kline validation failed: {str(e)}")

    @staticmethod
    def validate_klines(klines: List[Tuple]) -> List[Tuple[int, str]]:
        """
        Validate a list of kline records and return any validation errors.

        Args:
            klines: List of kline data tuples

        Returns:
            List of (index, error_message) tuples for invalid records
        """
        validation_errors = []

        for i, kline in enumerate(klines):
            try:
                KlineValidator.validate_kline(kline)
            except ValidationError as e:
                validation_errors.append((i, str(e)))
            except Exception as e:
                validation_errors.append((i, f"Unexpected error: {str(e)}"))

        return validation_errors