import os
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .service import FundamentalDataService
from shared.clients.coingecko import CoinGeckoAdapter
from shared.clients.exchanges import BybitAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import Config
from shared.database.connection import DatabaseConnection
from shared.database.repositories import MetadataRepository, MarketMetricsRepository, SentimentRepository
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, ServiceStatusMessage
from shared.utils.logger import LoggerSetup
from shared.utils.time import TimeUtils

logger = LoggerSetup.setup(__name__)

# Service instance
service: Optional[FundamentalDataService] = None
metrics_task: Optional[asyncio.Task] = None

async def publish_metrics():
    """Periodically publish service metrics"""
    while True:
        try:
            if service and service._status == "running":
                # Collect current metrics
                collector_metrics = {
                    name: collector.get_collection_status()
                    for name, collector in service._collectors.items()
                }

                uptime = 0.0
                if service._start_time is not None:
                    uptime = (TimeUtils.get_current_timestamp() - service._start_time) / 1000

                # Get recent errors
                recent_errors = service._error_tracker.get_recent_errors(60)
                metadata_errors = len([e for e in recent_errors if "metadata" in str(e).lower()])
                market_errors = len([e for e in recent_errors if "market" in str(e).lower()])
                sentiment_errors = len([e for e in recent_errors if "sentiment" in str(e).lower()])

                # Publish service status with metrics
                await service.message_broker.publish(
                    MessageType.SERVICE_STATUS,
                    ServiceStatusMessage(
                        service="fundamental_data",
                        type=MessageType.SERVICE_STATUS,
                        timestamp=TimeUtils.get_current_timestamp(),
                        status=service._status,
                        uptime=uptime,
                        error_count=len(recent_errors),
                        warning_count=len([e for e in recent_errors if "warning" in str(e).lower()]),
                        metrics={
                            "active_tokens": len(service._active_tokens),
                            "metadata_errors": metadata_errors,
                            "market_errors": market_errors,
                            "sentiment_errors": sentiment_errors,
                            "collectors": collector_metrics,
                            "last_error": str(service._last_error) if service._last_error else None
                        }
                    ).dict()
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
        db = DatabaseConnection(config.database, schema='fundamental_data')
        await db.initialize()

        # Initialize repositories
        metadata_repository = MetadataRepository(db)
        market_metrics_repository = MarketMetricsRepository(db)
        sentiment_repository = SentimentRepository(db)

        # Initialize message broker
        message_broker = MessageBroker("fundamental_data")
        await message_broker.connect(config.message_broker.url)

        # Initialize exchange registry
        exchange_registry = ExchangeAdapterRegistry()
        await exchange_registry.register("bybit", BybitAdapter(config.adapters.bybit))

        # Initialize CoinGecko adapter
        coingecko_adapter = CoinGeckoAdapter(config.adapters.coingecko)

        service = FundamentalDataService(
            metadata_repository=metadata_repository,
            market_metrics_repository=market_metrics_repository,
            sentiment_repository=sentiment_repository,
            exchange_registry=exchange_registry,
            coingecko_adapter=coingecko_adapter,
            message_broker=message_broker,
            config=config.fundamental_data
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
    title="Fundamental Data Service",
    description="Token metadata and fundamental analysis service",
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
        "active_tokens": len(service._active_tokens)
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
        metadata_errors = len([e for e in recent_errors if "metadata" in str(e).lower()])
        market_errors = len([e for e in recent_errors if "market" in str(e).lower()])
        sentiment_errors = len([e for e in recent_errors if "sentiment" in str(e).lower()])

        return {
            "status": service._status,
            "uptime_seconds": uptime,
            "last_error": str(service._last_error) if service._last_error else None,
            "error_count": len(recent_errors),
            "warning_count": len([e for e in recent_errors if "warning" in str(e).lower()]),
            "active_tokens": len(service._active_tokens),
            "metadata_errors": metadata_errors,
            "market_errors": market_errors,
            "sentiment_errors": sentiment_errors,
            "collectors": {
                name: collector.get_collection_status()
                for name, collector in service._collectors.items()
            }
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
        "collectors": {
            name: collector.get_collection_status()
            for name, collector in service._collectors.items()
        }
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
        port=8002,
        reload=bool(os.getenv("DEBUG", False))
    )
