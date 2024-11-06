# src/utils/retry.py
import random
from dataclasses import dataclass
from typing import Type, Tuple, Set

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    base_delay: float = 1.0
    max_delay: float = 300.0
    max_retries: int = 3
    jitter_factor: float = 0.5

class RetryStrategy:
    """Handles retry logic with exponential backoff"""

    def __init__(self, config: RetryConfig = RetryConfig()):
        self.config = config
        self._retryable_errors: Set[Type[Exception]] = set()
        self._non_retryable_errors: Set[Type[Exception]] = set()

    def add_retryable_error(self, *error_types: Type[Exception]) -> None:
        """Add error types that should be retried"""
        self._retryable_errors.update(error_types)

    def add_non_retryable_error(self, *error_types: Type[Exception]) -> None:
        """Add error types that should not be retried"""
        self._non_retryable_errors.update(error_types)

    def get_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter"""
        delay = min(
            self.config.max_delay,
            self.config.base_delay * (2 ** attempt)
        )
        jitter = delay * self.config.jitter_factor
        return delay + random.uniform(-jitter, jitter)

    def should_retry(self, attempt: int, error: Exception) -> Tuple[bool, str]:
        """
        Determine if retry should be attempted

        Returns:
            Tuple[bool, str]: (should_retry, reason)
        """
        if attempt >= self.config.max_retries:
            return False, "max_retries_exceeded"

        # Check non-retryable errors first
        if any(isinstance(error, e) for e in self._non_retryable_errors):
            return False, "non_retryable_error"

        # Check explicitly retryable errors
        if any(isinstance(error, e) for e in self._retryable_errors):
            return True, "retryable_error"

        # Default behavior for uncategorized errors
        if isinstance(error, (ConnectionError, TimeoutError)):
            return True, "transient_error"

        return False, "unknown_error"