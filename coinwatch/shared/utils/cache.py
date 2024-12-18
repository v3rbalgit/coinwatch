from decimal import Decimal
import json
import functools
from typing import Any, Dict, Optional, Callable, Awaitable, Type, cast, get_origin, get_args
from pydantic import BaseModel
from redis.asyncio import Redis

from shared.core.enums import Interval
from shared.core.models import (
    KlineData,
    RSIResult, BollingerBandsResult, MACDResult, MAResult, OBVResult
)
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class RedisCache:
    """
    Redis-based caching implementation.

    Features:
    - Async Redis operations
    - Automatic serialization/deserialization
    - Configurable TTL
    - Namespace support for different services
    """

    def __init__(self, redis_url: str, namespace: str = "market_data"):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace

    def _make_key(self, key: str) -> str:
        """Create namespaced key"""
        return f"{self.namespace}:{key}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            value = await self.redis.get(self._make_key(key))
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set value in cache with TTL in seconds"""
        try:
            if isinstance(value, list) and value and isinstance(value[0], BaseModel):
                serialized = json.dumps([
                    json.loads(v.model_dump_json()) for v in value
                ])
            elif isinstance(value, BaseModel):
                serialized = value.model_dump_json()
            else:
                serialized = json.dumps(value)

            return await self.redis.set(
                self._make_key(key),
                serialized,
                ex=ttl
            )
        except Exception as e:
            logger.error(f"Redis set error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from cache"""
        try:
            return bool(await self.redis.delete(self._make_key(key)))
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
            return False

    async def clear_namespace(self) -> bool:
        """Clear all keys in namespace"""
        try:
            keys = await self.redis.keys(f"{self.namespace}:*")
            if keys:
                return bool(await self.redis.delete(*keys))
            return True
        except Exception as e:
            logger.error(f"Redis clear error: {e}")
            return False

    async def close(self) -> None:
        """Close Redis connection"""
        try:
            await self.redis.close()
        except Exception as e:
            logger.error(f"Redis close error: {e}")

class redis_cached[T]:
    """
    Decorator class for caching async function results in Redis.

    Example:
        @redis_cached[list[KlineData]]()
        async def get_klines(...) -> list[KlineData]:
            ...
    """
    def __init__(self, ttl: int = 300):
        self.ttl = ttl

    def _convert_to_decimal(self, value: Any) -> Any:
        """Convert string values back to Decimal where needed"""
        if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
            return Decimal(value)
        return value

    def _convert_to_enum(self, value: Any, field_type: Any) -> Any:
        """Convert string values back to Enum where needed"""
        if isinstance(value, str) and field_type == Interval:
            return Interval(value)
        return value

    def _deserialize_model(self, data: Dict[str, Any], model_class: Type[BaseModel]) -> BaseModel:
        """Deserialize a single model instance with proper type handling"""
        # Get field types from model
        field_types = {
            field_name: field.annotation
            for field_name, field in model_class.model_fields.items()
        }

        # Convert values based on field types
        converted_data = {}
        for k, v in data.items():
            field_type = field_types.get(k)
            if field_type == Decimal:
                converted_data[k] = self._convert_to_decimal(v)
            elif field_type == Interval:
                converted_data[k] = self._convert_to_enum(v, Interval)
            else:
                converted_data[k] = v

        return model_class.model_validate(converted_data)

    def _deserialize_cached_value(self, cached_value: Any, return_type: Type[T]) -> T:
        """Deserialize cached value back to appropriate type"""
        origin = get_origin(return_type)
        args = get_args(return_type)

        if origin is list and args and issubclass(args[0], BaseModel):
            model_class = args[0]

            # Handle different model types
            if model_class in (KlineData, RSIResult, MACDResult,
                             BollingerBandsResult, MAResult, OBVResult):
                return cast(T, [
                    self._deserialize_model(item, model_class)
                    for item in cached_value
                ])

            return cast(T, [model_class.model_validate(item) for item in cached_value])

        elif isinstance(return_type, type) and issubclass(return_type, BaseModel):
            return cast(T, self._deserialize_model(cached_value, return_type))

        return cast(T, cached_value)

    def __call__(self, func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(self_obj: Any, *args: Any, **kwargs: Any) -> T:
            if not hasattr(self_obj, 'cache') or not isinstance(self_obj.cache, RedisCache):
                return await func(self_obj, *args, **kwargs)

            key_parts = [
                func.__name__,
                *[str(arg) for arg in args],
                *[f"{k}:{v}" for k, v in sorted(kwargs.items())]
            ]
            cache_key = ":".join(key_parts)

            if cached_value := await self_obj.cache.get(cache_key):
                return_type = func.__annotations__.get('return', Any)
                return self._deserialize_cached_value(cached_value, return_type)

            result = await func(self_obj, *args, **kwargs)

            if result is not None:
                await self_obj.cache.set(cache_key, result, self.ttl)

            return result

        return wrapper