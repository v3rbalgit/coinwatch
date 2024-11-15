# src/utils/rate_limit.py

import time
import asyncio
from dataclasses import dataclass
from typing import Optional

@dataclass
class RateLimitConfig:
    """Configuration for rate limiting"""
    calls_per_window: int
    window_size: int  # seconds
    max_monthly_calls: Optional[int] = None

class TokenBucket:
    """
    Token bucket algorithm for rate limiting.

    Can handle both window-based (e.g., calls per minute) and
    monthly total limits simultaneously.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._tokens = config.calls_per_window
        self._last_refill = time.time()
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
            if self.config.max_monthly_calls:
                # Reset monthly counter if new month started
                if (current_time - self._month_start) >= 30 * 24 * 3600:  # 30 days
                    self._monthly_calls = 0
                    self._month_start = current_time

                # Check if monthly limit exceeded
                if self._monthly_calls >= self.config.max_monthly_calls:
                    return False

            # Handle window-based limit
            elapsed = current_time - self._last_refill
            if elapsed >= self.config.window_size:
                self._tokens = self.config.calls_per_window
                self._last_refill = current_time

            if self._tokens > 0:
                self._tokens -= 1
                if self.config.max_monthly_calls:
                    self._monthly_calls += 1
                return True

            return False

    async def acquire(self) -> None:
        """
        Acquire a token, waiting if necessary.

        Raises:
            asyncio.TimeoutError: If wait time would exceed window size
        """
        while not await self._acquire_token():
            # Calculate wait time
            wait_time = self.config.window_size - (time.time() - self._last_refill)

            if wait_time > 0:
                await asyncio.sleep(wait_time)