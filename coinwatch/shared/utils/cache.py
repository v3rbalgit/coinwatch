# src/utils/cache.py

from typing import Any, Callable, TypeVar, ParamSpec, Coroutine, cast, Optional
from functools import wraps
from cachetools import TTLCache
from hashlib import md5
import logging
from dataclasses import dataclass

from shared.core.enums import Timeframe
from shared.core.models import SymbolInfo

logger = logging.getLogger(__name__)

T = TypeVar('T')
P = ParamSpec('P')

@dataclass
class CacheStats:
    """Statistics for cache monitoring"""
    hits: int = 0
    misses: int = 0
    size: int = 0
    maxsize: int = 0
    ttl: int = 0

    def __str__(self) -> str:
        hit_ratio = (self.hits / (self.hits + self.misses)) * 100 if (self.hits + self.misses) > 0 else 0
        return (
            f"Cache Stats: hits={self.hits}, misses={self.misses}, "
            f"hit_ratio={hit_ratio:.1f}%, size={self.size}/{self.maxsize}, "
            f"ttl={self.ttl}s"
        )

    @property
    def hit_ratio(self) -> float:
        """Calculate cache hit ratio"""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

class AsyncTTLCache:
    """Thread-safe TTL cache for async functions"""

    def __init__(self, maxsize: int = 1000, ttl: int = 300):
        """
        Initialize cache with size and TTL limits

        Args:
            maxsize: Maximum number of items in cache
            ttl: Time to live in seconds
        """
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.stats = CacheStats(maxsize=maxsize, ttl=ttl)
        self._wrapped_function: Optional[str] = None

    def _make_key(self, *args: Any, **kwargs: Any) -> str:
        """
        Generate a cache key from function arguments

        Handles special cases for domain types like SymbolInfo and Timeframe
        """
        processed_args = []

        for arg in args:
            if isinstance(arg, SymbolInfo):
                processed_args.append(f"{arg.name}_{arg.exchange}")
            elif isinstance(arg, Timeframe):
                processed_args.append(arg.value)
            elif isinstance(arg, (int, float, str)):
                processed_args.append(str(arg))
            elif arg is None:
                processed_args.append('None')
            else:
                processed_args.append(str(arg))

        # Process kwargs in sorted order for consistent keys
        processed_kwargs = [
            f"{k}_{v}" for k, v in sorted(kwargs.items())
        ]

        # Combine all parts and hash
        key_parts = processed_args + processed_kwargs
        return md5('_'.join(key_parts).encode()).hexdigest()

    def clear(self) -> None:
        """Clear the cache and reset statistics"""
        self.cache.clear()
        self.stats = CacheStats(maxsize=int(self.cache.maxsize), ttl=int(self.cache.ttl))

    def get_stats(self) -> CacheStats:
        """Get current cache statistics"""
        self.stats.size = len(self.cache)
        self.stats.maxsize = int(self.cache.maxsize)
        self.stats.ttl = int(self.cache.ttl)
        return self.stats

    def reconfigure(self, maxsize: Optional[int] = None, ttl: Optional[int] = None) -> None:
        """
        Reconfigure cache parameters

        Args:
            maxsize: New maximum size (optional)
            ttl: New TTL in seconds (optional)
        """
        new_maxsize = maxsize if maxsize is not None else self.cache.maxsize
        new_ttl = ttl if ttl is not None else self.cache.ttl

        # Create new cache with updated parameters
        new_cache = TTLCache(maxsize=new_maxsize, ttl=new_ttl)

        # Transfer still-valid entries
        for key, value in self.cache.items():
            new_cache[key] = value

        # Update cache and stats
        self.cache = new_cache
        self.stats.maxsize = int(new_maxsize)
        self.stats.ttl = int(new_ttl)

    def __call__(
        self,
        func: Callable[P, Coroutine[Any, Any, T]]
    ) -> Callable[P, Coroutine[Any, Any, T]]:
        """Make class instance callable as a decorator"""
        self._wrapped_function = func.__name__

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            key = self._make_key(*args, **kwargs)

            try:
                # Try to get from cache
                result = self.cache[key]
                self.stats.hits += 1
                logger.debug(f"Cache hit for {self._wrapped_function}")
                return cast(T, result)
            except KeyError:
                # Calculate and cache result
                result = await func(*args, **kwargs)
                self.cache[key] = result
                self.stats.misses += 1
                self.stats.size = len(self.cache)
                logger.debug(f"Cache miss for {self._wrapped_function}, stored new result")
                return result

        # Add cache management methods directly to wrapper
        wrapper.cache_clear = self.clear  # type: ignore
        wrapper.cache_info = self.get_stats  # type: ignore
        wrapper.reconfigure = self.reconfigure  # type: ignore
        return wrapper

def async_ttl_cache(
    maxsize: int = 1000,
    ttl: int = 300
) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], Callable[P, Coroutine[Any, Any, T]]]:
    """
    Create an async TTL cache decorator

    Args:
        maxsize: Maximum cache size (default: 1000)
        ttl: Time to live in seconds (default: 300)

    Returns:
        Callable: Cache decorator for async functions

    Example:
        @async_ttl_cache(maxsize=100, ttl=60)
        async def fetch_data() -> List[DataType]:
            ...
    """
    return AsyncTTLCache(maxsize=maxsize, ttl=ttl)