import os
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .service import MarketDataService
from shared.core.config import Config
from shared.database.connection import DatabaseConnection
from shared.database.repositories import SymbolRepository, KlineRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, ServiceStatusMessage
from shared.clients.exchanges import BybitAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils
from shared.utils.domain_types import ServiceStatus

logger = LoggerSetup.setup(__name__)

# Service instance
service: Optional[MarketDataService] = None
metrics_task: Optional[asyncio.Task] = None

async def publish_metrics():
    """Periodically publish service metrics"""
    while True:
        try:
            if service and service._status == ServiceStatus.RUNNING:
                # Collect current metrics
                recent_errors = service._error_tracker.get_recent_errors(60)
                collection_errors = len([e for e in recent_errors if "collection" in str(e).lower()])
                streaming_errors = len([e for e in recent_errors if "streaming" in str(e).lower()])
                gap_errors = len([e for e in recent_errors if "gap" in str(e).lower()])

                uptime = 0.0
                if service._start_time is not None:
                    uptime = (TimeUtils.get_current_timestamp() - service._start_time) / 1000

                # Publish service status with metrics
                await service.message_broker.publish(
                    MessageType.SERVICE_STATUS,
                    ServiceStatusMessage(
                        service="market_data",
                        type=MessageType.SERVICE_STATUS,
                        timestamp=TimeUtils.get_current_timestamp(),
                        status=service._status,
                        uptime=uptime,
                        error_count=len(recent_errors),
                        warning_count=len([e for e in recent_errors if "warning" in str(e).lower()]),
                        metrics={
                            "active_symbols": len(service._active_symbols),
                            "active_collections": len(service.data_collector._processing_symbols),
                            "streaming_symbols": len(service.data_collector._streaming_symbols),
                            "streaming_errors": streaming_errors,
                            "collection_errors": collection_errors,
                            "batch_size": service.data_collector._batch_size,
                            "data_gaps": gap_errors,
                            "last_error": str(service._last_error) if service._last_error else None
                        }
                    ).model_dump()
                )

        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")

        # Sleep for metrics interval
        await asyncio.sleep(30)  # Configurable interval

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Service lifecycle manager"""
    global service, metrics_task

    try:
        config = Config()

        # Initialize database connection
        db = DatabaseConnection(config.database, schema="market_data")
        await db.initialize()

        # Initialize repositories
        symbol_repository = SymbolRepository(db)
        kline_repository = KlineRepository(db)

        # Initialize exchange registry with adapters
        exchange_registry = ExchangeAdapterRegistry()
        await exchange_registry.register("bybit", BybitAdapter(config.adapters.bybit))

        # Initialize message broker
        message_broker = MessageBroker("market_data")
        await message_broker.connect(config.message_broker.url)

        service = MarketDataService(
            symbol_repository=symbol_repository,
            kline_repository=kline_repository,
            exchange_registry=exchange_registry,
            message_broker=message_broker,
            config=config.market_data
        )

        # Start service and metrics publishing
        await service.start()
        metrics_task = asyncio.create_task(publish_metrics())

        yield  # Service is running

    except Exception as e:
        logger.error(f"Service initialization failed: {e}")
        raise HTTPException(status_code=500, detail=f"Service initialization failed: {str(e)}")

    finally:
        # Cleanup
        if metrics_task:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass

        if service:
            await service.stop()

# Initialize FastAPI app
app = FastAPI(
    title="Market Data Service",
    description="Market data collection and management service",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    return {
        "status": "healthy",
        "service_status": service._status,
        "active_symbols": len(service._active_symbols)
    }

@app.get("/metrics")
async def get_metrics():
    """Get service metrics (for local debugging)"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        uptime = 0.0
        if service._start_time is not None:
            uptime = (TimeUtils.get_current_timestamp() - service._start_time) / 1000

        recent_errors = service._error_tracker.get_recent_errors(60)
        collection_errors = len([e for e in recent_errors if "collection" in str(e).lower()])
        streaming_errors = len([e for e in recent_errors if "streaming" in str(e).lower()])
        gap_errors = len([e for e in recent_errors if "gap" in str(e).lower()])

        return {
            "status": service._status,
            "uptime_seconds": uptime,
            "last_error": str(service._last_error) if service._last_error else None,
            "error_count": len(recent_errors),
            "warning_count": len([e for e in recent_errors if "warning" in str(e).lower()]),
            "active_symbols": len(service._active_symbols),
            "active_collections": len(service.data_collector._processing_symbols),
            "streaming_symbols": len(service.data_collector._streaming_symbols),
            "streaming_errors": streaming_errors,
            "collection_errors": collection_errors,
            "batch_size": service.data_collector._batch_size,
            "data_gaps": gap_errors
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect metrics: {str(e)}")

@app.get("/status")
async def get_status():
    """Get detailed service status"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    return {
        "status": service.get_service_status(),
        "collector_status": service.data_collector.get_collection_status()
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=bool(os.getenv("DEBUG", False))
    )
