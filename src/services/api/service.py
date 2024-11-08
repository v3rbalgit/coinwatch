# src/services/api/service.py

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

from ...core.coordination import ServiceCoordinator
from ...core.exceptions import ServiceError
from ...services.base import ServiceBase
from ...config import APIConfig
from ...utils.logger import LoggerSetup
from ...utils.domain_types import ServiceStatus
from ..market_data.service import MarketDataService
from ..database import DatabaseService
from .routers import market_data, indicators, system

logger = LoggerSetup.setup(__name__)

@dataclass
class APIServiceContext:
    """Context containing core service instances needed by API endpoints"""
    market_data_service: MarketDataService
    database_service: DatabaseService
    coordinator: ServiceCoordinator

class APIService(ServiceBase):
    """
    REST API service for the Coinwatch application.

    Provides HTTP endpoints for:
    - Market data retrieval
    - Technical indicator calculations
    - System status and monitoring
    - Service configuration

    The service is designed to be extensible for future WebSocket support
    and additional features like authentication and trade execution.
    """

    def __init__(self,
                 market_data_service: MarketDataService,
                 database_service: DatabaseService,
                 coordinator: ServiceCoordinator,
                 config: APIConfig):
        super().__init__(config)

        self.app = FastAPI(
            title="Coinwatch API",
            description="Market data and technical analysis API",
            version="1.0.0"
        )

        # Store service instances
        self.context = APIServiceContext(
            market_data_service=market_data_service,
            database_service=database_service,
            coordinator=coordinator
        )

        # Configure CORS
        self._configure_cors()

        # Register error handlers
        self._configure_error_handlers()

        # Initialize routers
        self._configure_routers()

        self._status = ServiceStatus.STOPPED

    def _configure_cors(self) -> None:
        """Configure CORS middleware"""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self._config.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )

    def _configure_error_handlers(self) -> None:
        """Configure global error handlers"""
        @self.app.exception_handler(ServiceError)
        async def service_error_handler(request, exc):
            return JSONResponse(
                status_code=500,
                content={"error": str(exc)}
            )

        @self.app.exception_handler(ValidationError)
        async def validation_error_handler(request, exc):
            return JSONResponse(
                status_code=400,
                content={"error": str(exc)}
            )

    def _configure_routers(self) -> None:
        """Configure API routers"""
        # Market data endpoints
        self.app.include_router(
            market_data.router,
            prefix="/api/v1/market-data",
            tags=["Market Data"]
        )

        # Technical indicators endpoints
        self.app.include_router(
            indicators.router,
            prefix="/api/v1/indicators",
            tags=["Technical Indicators"]
        )

        # System/monitoring endpoints
        self.app.include_router(
            system.router,
            prefix="/api/v1/system",
            tags=["System"]
        )

    async def start(self) -> None:
        """Start the API service"""
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting API service")

            # Additional startup logic can be added here
            # (e.g., background tasks, resource initialization)

            self._status = ServiceStatus.RUNNING
            logger.info("API service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Failed to start API service: {e}")
            raise ServiceError(f"API service start failed: {str(e)}")

    async def stop(self) -> None:
        """Stop the API service"""
        try:
            self._status = ServiceStatus.STOPPING
            logger.info("Stopping API service")

            # Cleanup logic can be added here

            self._status = ServiceStatus.STOPPED
            logger.info("API service stopped")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Error stopping API service: {e}")
            raise ServiceError(f"API service stop failed: {str(e)}")

# Dependencies for endpoint functions
async def get_context() -> APIServiceContext:
    """Dependency to get API context"""
    return APIService.context