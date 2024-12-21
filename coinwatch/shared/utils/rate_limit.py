import time
import asyncio


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

    async def _acquire_token(self) -> bool:
        """
        Attempt to acquire a rate limit token.

        Returns:
            bool: True if token acquired, False if no tokens available
        """
        async with self._lock:
            current_time = time.time()

            # Check monthly limit if configured
            if self._max_monthly_calls:
                # Reset monthly counter if new month started
                if (current_time - self._month_start) >= 30 * 24 * 3600:  # 30 days
                    self._monthly_calls = 0
                    self._month_start = current_time

                # Check if monthly limit exceeded
                if self._monthly_calls >= self._max_monthly_calls:
                    return False

            # Handle window-based limit
            elapsed = current_time - self._last_refill

            # Calculate token refill based on elapsed time
            if elapsed >= self._window_size:
                # Full window has passed, reset tokens
                self._tokens = self._calls_per_window
                self._last_refill = current_time
            elif elapsed > 0:
                # Partial window refill
                new_tokens = int((elapsed / self._window_size) * self._calls_per_window)
                if new_tokens > 0:
                    self._tokens = min(self._calls_per_window, self._tokens + new_tokens)
                    self._last_refill = current_time

            if self._tokens > 0:
                self._tokens -= 1
                if self._max_monthly_calls:
                    self._monthly_calls += 1
                return True

            return False

    async def acquire(self) -> None:
        """
        Acquire a token, waiting if necessary.

        This method will wait until a token becomes available.
        """
        async with self._lock:
            current_time = time.time()
            elapsed = current_time - self._last_refill

            if self._tokens == 0:
                # Calculate time until next token is available
                if elapsed >= self._window_size:
                    # Window has passed, reset tokens
                    self._tokens = self._calls_per_window
                    self._last_refill = current_time
                else:
                    # Calculate exact wait time needed for next token
                    time_per_token = self._window_size / self._calls_per_window
                    wait_time = time_per_token - (elapsed % time_per_token)
                    await asyncio.sleep(wait_time)
                    self._tokens = 1  # We get at least one token after waiting
                    self._last_refill = time.time()

            self._tokens -= 1
            if self._max_monthly_calls:
                self._monthly_calls += 1
