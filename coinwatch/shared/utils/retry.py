# src/utils/retry.py

from dataclasses import dataclass, field
from typing import Dict, Type, Tuple, Set, Optional
import random

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    base_delay: float = 1.0
    max_delay: float = 300.0
    max_retries: int = 3
    jitter_factor: float = 0.5

@dataclass
class RetryStrategy:
    """Enhanced retry strategy with error-specific handling"""
    default_config: RetryConfig = field(default_factory=RetryConfig)
    _retryable_errors: Set[Type[Exception]] = field(default_factory=set)
    _non_retryable_errors: Set[Type[Exception]] = field(default_factory=set)
    _error_configs: Dict[Type[Exception], RetryConfig] = field(default_factory=dict)

    def add_retryable_error(self, *error_types: Type[Exception]) -> None:
        """Add error types that should be retried"""
        self._retryable_errors.update(error_types)

    def add_non_retryable_error(self, *error_types: Type[Exception]) -> None:
        """Add error types that should not be retried"""
        self._non_retryable_errors.update(error_types)

    def configure_error_delays(self, configs: Dict[Type[Exception], RetryConfig]) -> None:
        """Configure specific retry behaviors for different error types"""
        self._error_configs.update(configs)

    def get_config_for_error(self, error: Exception) -> RetryConfig:
        """Get retry configuration for specific error type"""
        for error_type, config in self._error_configs.items():
            if isinstance(error, error_type):
                return config
        return self.default_config

    def get_delay(self, attempt: int, error: Optional[Exception] = None) -> float:
        """Calculate delay with exponential backoff and jitter"""
        config = self.default_config if error is None else self.get_config_for_error(error)

        delay = min(
            config.max_delay,
            config.base_delay * (2 ** attempt)
        )
        jitter = delay * config.jitter_factor
        return delay + random.uniform(-jitter, jitter)

    def should_retry(self, attempt: int, error: Exception) -> Tuple[bool, str]:
        """
        Determine if retry should be attempted

        Returns:
            Tuple[bool, str]: (should_retry, reason)
        """
        # Get config for this error type
        config = self.get_config_for_error(error)

        if attempt >= config.max_retries:
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