from typing import Dict
import aiohttp
from fastapi import HTTPException
from shared.utils.logger import LoggerSetup


class ServiceRegistry:
    """
    Service discovery and health tracking for microservices.

    Features:
    - Service URL management
    - Health checking
    - Dynamic service discovery
    - Circuit breaking
    """

    def __init__(self):
        # Service URLs with default ports
        self._services: Dict[str, str] = {
            'market_data': 'http://market_data:8001',
            'fundamental_data': 'http://fundamental_data:8002',
            'monitor': 'http://monitor:8003'
        }

        # Service health status
        self._health_status: Dict[str, bool] = {
            'market_data': False,
            'fundamental_data': False,
            'monitor': False
        }

        # Circuit breaker state
        self._failure_counts: Dict[str, int] = {
            'market_data': 0,
            'fundamental_data': 0,
            'monitor': 0
        }
        self._failure_threshold = 3  # Number of failures before circuit breaks
        self._circuit_broken: Dict[str, bool] = {
            'market_data': False,
            'fundamental_data': False,
            'monitor': False
        }
        self.logger = LoggerSetup.setup(__class__.__name__)

    async def get_service_url(self, service_name: str) -> str:
        """
        Get URL for a service, checking health and circuit breaker state.

        Args:
            service_name: Name of the service

        Returns:
            str: Service URL

        Raises:
            HTTPException: If service is unavailable or circuit is broken
        """
        if service_name not in self._services:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown service: {service_name}"
            )

        if self._circuit_broken[service_name]:
            raise HTTPException(
                status_code=503,
                detail=f"Service {service_name} is temporarily unavailable (circuit broken)"
            )

        if not self._health_status[service_name]:
            # Try to check health before failing
            healthy = await self._check_service_health(service_name)
            if not healthy:
                self._failure_counts[service_name] += 1
                if self._failure_counts[service_name] >= self._failure_threshold:
                    self._circuit_broken[service_name] = True
                raise HTTPException(
                    status_code=503,
                    detail=f"Service {service_name} is unhealthy"
                )

        return self._services[service_name]

    async def _check_service_health(self, service_name: str) -> bool:
        """
        Check health of a service.

        Args:
            service_name: Name of the service to check

        Returns:
            bool: True if service is healthy, False otherwise
        """
        url = f"{self._services[service_name]}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        self._health_status[service_name] = True
                        self._failure_counts[service_name] = 0  # Reset failure count
                        return True

                    self.logger.warning(
                        f"Health check failed for {service_name}: "
                        f"Status {response.status}"
                    )
                    return False

        except Exception as e:
            self.logger.error(f"Error checking {service_name} health: {e}")
            return False

    async def check_all_services(self) -> Dict[str, bool]:
        """
        Check health of all services.

        Returns:
            Dict[str, bool]: Health status for each service
        """
        for service in self._services:
            self._health_status[service] = await self._check_service_health(service)
        return self._health_status

    def reset_circuit(self, service_name: str) -> None:
        """
        Reset circuit breaker for a service.

        Args:
            service_name: Name of the service
        """
        if service_name in self._circuit_broken:
            self._circuit_broken[service_name] = False
            self._failure_counts[service_name] = 0
            self.logger.info(f"Circuit reset for {service_name}")

    def get_service_status(self) -> Dict[str, Dict]:
        """
        Get detailed status of all services.

        Returns:
            Dict[str, Dict]: Status information for each service
        """
        return {
            service: {
                'url': url,
                'healthy': self._health_status[service],
                'circuit_broken': self._circuit_broken[service],
                'failure_count': self._failure_counts[service]
            }
            for service, url in self._services.items()
        }
