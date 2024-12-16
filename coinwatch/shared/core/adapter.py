# src/adapters/base.py

from typing import Optional, Any, Protocol
import aiohttp
import asyncio

class APIAdapter(Protocol):
    """
    Base class for API adapters providing common functionality

    Features:
    - Session management
    - Rate limiting
    - Retry logic
    - Request handling
    """

    _session: Optional[aiohttp.ClientSession] = None
    _session_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None:
            async with self._session_lock:
                if self._session is None:
                    self._session = await self._create_session()
        return self._session

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create new session with adapter-specific configuration"""
        ...

    async def _request(self,
                      method: str,
                      endpoint: str,
                      **kwargs: Any) -> Any:
        """Make API request with retry logic and rate limiting"""
        ...

    async def cleanup(self) -> None:
        """Cleanup resources"""
        if self._session:
            async with self._session_lock:
                await self._session.close()
                self._session = None