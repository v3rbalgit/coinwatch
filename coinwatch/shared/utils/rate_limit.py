import time
import asyncio
from typing import Tuple


class RateLimiter:
    """
    Token bucket algorithm for rate limiting.

    Can handle both window-based (e.g., calls per minute) and
    monthly total limits simultaneously.
    """

    def __init__(self, calls_per_window: int, window_size: int, max_monthly_calls: int | None = None):
        self._calls_per_window = calls_per_window
        self._window_size = window_size
        self._tokens = calls_per_window
        self._last_refill = time.time()
        self._max_monthly_calls = max_monthly_calls
        self._monthly_calls = 0
        self._month_start = time.time()
        self._lock = asyncio.Lock()

    def _check_monthly_limit(self, current_time: float) -> bool:
        """Check and update monthly limit status."""
        if not self._max_monthly_calls:
            return True

        # Reset monthly counter if new month started
        if (current_time - self._month_start) >= 30 * 24 * 3600:  # 30 days
            self._monthly_calls = 0
            self._month_start = current_time

        return self._monthly_calls < self._max_monthly_calls

    def _refill_tokens(self, current_time: float) -> Tuple[int, float]:
        """Calculate token refill based on elapsed time."""
        elapsed = current_time - self._last_refill

        if elapsed >= self._window_size:
            # Full window has passed, reset tokens
            return self._calls_per_window, current_time
        elif elapsed > 0:
            # Partial window refill
            new_tokens = int((elapsed / self._window_size) * self._calls_per_window)
            if new_tokens > 0:
                return min(self._calls_per_window, self._tokens + new_tokens), current_time

        return self._tokens, self._last_refill

    def _calculate_wait_time(self, elapsed: float) -> float:
        """Calculate wait time until next token is available."""
        time_per_token = self._window_size / self._calls_per_window
        return time_per_token - (elapsed % time_per_token)

    async def acquire(self) -> None:
        """
        Acquire a token, waiting if necessary.

        This method will wait until a token becomes available.
        Raises:
            RuntimeError: If monthly API limit is exceeded
        """
        async with self._lock:
            current_time = time.time()

            # Check monthly limit first
            if not self._check_monthly_limit(current_time):
                raise RuntimeError("Monthly API limit exceeded")

            # Refill tokens based on elapsed time
            self._tokens, self._last_refill = self._refill_tokens(current_time)

            if self._tokens == 0:
                # Calculate and wait for next token
                elapsed = current_time - self._last_refill
                wait_time = self._calculate_wait_time(elapsed)
                await asyncio.sleep(wait_time)
                self._tokens = 1
                self._last_refill = time.time()

            self._tokens -= 1
            if self._max_monthly_calls:
                self._monthly_calls += 1
