from typing import Any, Protocol
import aiohttp
import asyncio

class Service(Protocol):
    """
    Base class for all services.

    Features:
    - Configuration management
    - Service lifecycle (start/stop)
    - Status reporting
    - Message-based communication
    """
    async def start(self) -> None:
        """
        Start the service.

        Each service must implement its startup logic:
        - Initialize resources
        - Connect to message broker
        - Start background tasks
        """
        ...

    async def stop(self) -> None:
        """
        Stop the service.

        Each service must implement its cleanup logic:
        - Close connections
        - Cancel background tasks
        - Release resources
        """
        ...

    def get_service_status(self) -> str:
        """
        Generate detailed service status report.

        Returns:
            str: Multi-line status report including:
                - Service state
                - Resource usage
                - Error counts
                - Component health
        """
        ...


class APIAdapter(Protocol):
    """
    Base class for API adapters providing common functionality

    Features:
    - Session management
    - Rate limiting
    - Retry logic
    - Request handling
    """

    _session: aiohttp.ClientSession | None = None
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