from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Generic

from .config import MarketDataConfig, MonitoringConfig, DatabaseConfig

ConfigType = TypeVar('ConfigType', MarketDataConfig, MonitoringConfig, DatabaseConfig, Dict[str, Any])

class ServiceBase(ABC, Generic[ConfigType]):
    """
    Base class for all services.

    Features:
    - Configuration management
    - Service lifecycle (start/stop)
    - Status reporting
    - Message-based communication
    """
    def __init__(self, config: ConfigType):
        """Initialize service with configuration"""
        self._config: ConfigType = config or {}

    @abstractmethod
    async def start(self) -> None:
        """
        Start the service.

        Each service must implement its startup logic:
        - Initialize resources
        - Connect to message broker
        - Start background tasks
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the service.

        Each service must implement its cleanup logic:
        - Close connections
        - Cancel background tasks
        - Release resources
        """
        pass

    @abstractmethod
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
        pass
