# src/adapters/base.py

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar, Any, Protocol
import aiohttp
import asyncio

class APIConfig(Protocol):
    """Protocol for API configs to ensure type safety"""
    rate_limit: int
    rate_limit_window: int

TConfig = TypeVar('TConfig', bound=APIConfig)

class APIAdapter(ABC, Generic[TConfig]):
    """
    Base class for API adapters providing common functionality

    Features:
    - Session management
    - Rate limiting
    - Retry logic
    - Request handling
    """

    def __init__(self, config: TConfig):
        self._config = config
        self._initialized = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None:
            async with self._session_lock:
                if self._session is None:
                    self._session = await self._create_session()
        return self._session

    @abstractmethod
    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with adapter-specific configuration"""
        pass

    @abstractmethod
    async def _request(self,
                      method: str,
                      endpoint: str,
                      **kwargs: Any) -> Any:
        """Make API request with retry logic and rate limiting"""
        pass

    async def cleanup(self) -> None:
        """Cleanup resources"""
        if self._session:
            await self._session.close()
            self._session = None