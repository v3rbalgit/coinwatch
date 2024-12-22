import time
from typing import Dict, Tuple
from fastapi import Request, HTTPException
from redis.asyncio import Redis
from shared.utils.logger import LoggerSetup


class RateLimiter:
    """
    Rate limiting middleware using Redis and sliding window algorithm.

    Features:
    - Per-endpoint rate limits
    - IP-based limiting
    - Sliding window counter
    - Configurable limits
    """

    def __init__(self, redis_url: str = "redis://redis:6379"):
        self.redis = Redis.from_url(redis_url, decode_responses=True)

        # Default rate limits (requests per minute)
        self._rate_limits: Dict[str, int] = {
            # Market data endpoints
            "market_data:symbols": 60,
            "market_data:klines": 120,
            "market_data:indicators": 60,

            # Fundamental data endpoints
            "fundamental_data:metadata": 30,
            "fundamental_data:metrics": 30,
            "fundamental_data:sentiment": 30,

            # Monitor endpoints
            "monitor:metrics": 60,
            "monitor:status": 30,

            # Default limit for unspecified endpoints
            "default": 60
        }

        # Window size in seconds
        self._window_size = 60

        self.logger = LoggerSetup.setup(__class__.__name__)

    async def check_rate_limit(
        self,
        request: Request,
        endpoint: str | None = None
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Check if request is within rate limits.

        Args:
            request: FastAPI request
            endpoint: Optional endpoint identifier

        Returns:
            Tuple[bool, Dict[str, int]]: (allowed, limit info)
            - allowed: Whether request is allowed
            - limit info: Current limit status
        """
        try:
            # Get client IP
            client_ip = request.client.host if request.client else "unknown"

            # Get endpoint identifier
            if not endpoint:
                path = request.url.path.strip("/")
                parts = path.split("/")
                if len(parts) >= 2:
                    endpoint = f"{parts[1]}:{parts[-1]}"
                else:
                    endpoint = "default"

            # Get rate limit for endpoint
            limit = self._rate_limits.get(endpoint, self._rate_limits["default"])

            # Create Redis key
            current_time = int(time.time())
            window_start = current_time - self._window_size
            key = f"ratelimit:{endpoint}:{client_ip}:{window_start}"

            # Get current count
            count = await self.redis.get(key)
            count = int(count) if count else 0

            if count >= limit:
                # Rate limit exceeded
                remaining_time = await self.redis.ttl(key)
                return False, {
                    "limit": limit,
                    "remaining": 0,
                    "reset": remaining_time,
                    "window": self._window_size
                }

            # Increment counter
            pipe = self.redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, self._window_size)
            await pipe.execute()

            return True, {
                "limit": limit,
                "remaining": limit - (count + 1),
                "reset": self._window_size,
                "window": self._window_size
            }

        except Exception as e:
            self.logger.error(f"Rate limit check error: {e}")
            # Allow request on error
            return True, {
                "limit": 0,
                "remaining": 0,
                "reset": 0,
                "window": self._window_size
            }

    async def close(self) -> None:
        """Close Redis connection"""
        await self.redis.close()

async def rate_limit_middleware(request: Request, call_next):
    """FastAPI middleware for rate limiting"""
    limiter = RateLimiter()
    try:
        allowed, info = await limiter.check_rate_limit(request)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Rate limit exceeded",
                    "limit_info": info
                }
            )

        # Add rate limit info to response headers
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(info["limit"])
        response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
        response.headers["X-RateLimit-Reset"] = str(info["reset"])
        return response

    finally:
        await limiter.close()
