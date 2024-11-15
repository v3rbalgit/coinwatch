# src/services/fundamental_data/service.py

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from ...core.coordination import ServiceCoordinator
from ...core.exceptions import ServiceError
from ...utils.domain_types import ServiceStatus
from ...services.base import ServiceBase
from ...utils.logger import LoggerSetup
from ...core.models import SymbolInfo
from ...utils.error import ErrorTracker

logger = LoggerSetup.setup(__name__)

@dataclass
class FundamentalDataConfig:
    """Configuration for fundamental data collection"""
    # Default collection intervals (in seconds)
    collection_intervals: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 86400 * 30,  # Monthly
        'market': 86400,         # Daily
        'blockchain': 86400,     # Daily
        'sentiment': 86400       # Daily
    })
    batch_size: int = 100
    max_concurrent_collections: int = 10

class FundamentalDataService(ServiceBase):
    """
    Service for collecting and managing fundamental cryptocurrency data.

    Responsibilities:
    - Coordinates different metric collectors
    - Manages collection schedules
    - Handles resource allocation
    - Provides metrics aggregation
    - Monitors collection health
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 config: FundamentalDataConfig):
        super().__init__(config)

        self.coordinator = coordinator
        self._status = ServiceStatus.STOPPED
        self._active_symbols: Set[SymbolInfo] = set()

        # Error tracking
        self._error_tracker = ErrorTracker()
        self._last_error: Optional[Exception] = None

        # Collection management
        self._collection_lock = asyncio.Lock()
        self._processing: Set[SymbolInfo] = set()

        # Initialize collectors (will add as we implement them)
        self._collectors = {
            # 'metadata': MetadataCollector(...),
            # 'market': MarketMetricsCollector(...),
            # 'blockchain': BlockchainMetricsCollector(...),
            # 'sentiment': SentimentMetricsCollector(...)
        }

    async def start(self) -> None:
        """Start the fundamental data service"""
        try:
            self._status = ServiceStatus.STARTING
            # await self._register_command_handlers()

            # Start collectors
            for collector in self._collectors.values():
                await collector.start()

            self._status = ServiceStatus.RUNNING
            logger.info("Fundamental data service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self._last_error = e
            logger.error(f"Failed to start fundamental data service: {e}")
            raise ServiceError(f"Service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop the fundamental data service"""
        try:
            self._status = ServiceStatus.STOPPING

            # Stop collectors
            for collector in self._collectors.values():
                await collector.stop()

            # await self._unregister_command_handlers()
            self._status = ServiceStatus.STOPPED

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping fundamental data service: {e}")
            raise ServiceError(f"Service stop failed: {str(e)}")

    def get_service_status(self) -> str:
        """Get comprehensive service status"""
        status_lines = [
            "Fundamental Data Service Status:",
            f"Status: {self._status.value}",
            f"Active Symbols: {len(self._active_symbols)}",
            "",
            "Collection Status:"
        ]

        # Add collector statuses
        for name, collector in self._collectors.items():
            status_lines.append(f"\n{name.title()} Collector:")
            status_lines.append(collector.get_collection_status())

        return "\n".join(status_lines)