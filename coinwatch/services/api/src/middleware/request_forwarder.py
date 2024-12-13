import aiohttp
from typing import Dict, Any, Optional
from fastapi import Request, HTTPException
from shared.utils.logger import LoggerSetup
from ..service_registry import ServiceRegistry

logger = LoggerSetup.setup(__name__)

class RequestForwarder:
    """
    Handles forwarding requests to appropriate microservices.

    Features:
    - Request routing
    - Header forwarding
    - Error handling
    - Response transformation
    """

    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def forward_request(
        self,
        service: str,
        request: Request,
        endpoint: str,
        strip_prefix: bool = True
    ) -> Dict[str, Any]:
        """
        Forward an HTTP request to a service.

        Args:
            service: Target service name
            request: Original FastAPI request
            endpoint: Target endpoint
            strip_prefix: Whether to strip service prefix from path

        Returns:
            Dict[str, Any]: Response data from service

        Raises:
            HTTPException: If forwarding fails
        """
        try:
            # Get service URL
            base_url = await self.registry.get_service_url(service)

            # Build target URL
            if strip_prefix:
                # Remove /api/v1/service-name from path
                path_parts = request.url.path.split("/")
                service_path = "/".join(path_parts[4:])
                url = f"{base_url}/{service_path}"
            else:
                url = f"{base_url}/{endpoint.lstrip('/')}"

            # Forward headers (excluding host)
            headers = {
                k: v for k, v in request.headers.items()
                if k.lower() != "host"
            }

            # Get request body if present
            body = await request.body() if request.method in ["POST", "PUT", "PATCH"] else None

            # Forward request
            session = await self._get_session()
            async with session.request(
                method=request.method,
                url=url,
                headers=headers,
                params=request.query_params,
                data=body
            ) as response:
                # Check response
                if response.status >= 400:
                    error_data = await response.json()
                    raise HTTPException(
                        status_code=response.status,
                        detail=error_data.get("detail", str(error_data))
                    )

                # Return response data
                return await response.json()

        except HTTPException:
            raise  # Re-raise HTTP exceptions
        except Exception as e:
            logger.error(f"Error forwarding request to {service}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Error forwarding request to {service}: {str(e)}"
            )

    async def close(self) -> None:
        """Close aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()
