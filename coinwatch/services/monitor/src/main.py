import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from shared.core.config import MonitorConfig
from shared.messaging.broker import MessageBroker
from shared.utils.logger import LoggerSetup
from .service import MonitoringService

logger = LoggerSetup.setup(__name__)

# Service instance
service: MonitoringService | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Service lifecycle manager"""
    global service

    try:
        # Initialize message broker
        message_broker = MessageBroker("monitor")
        await message_broker.connect(os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/"))

        # Initialize service with config
        service_config = MonitorConfig(
            check_intervals={
                'system': int(os.getenv("SYSTEM_CHECK_INTERVAL", "60")),
                'market': int(os.getenv("MARKET_CHECK_INTERVAL", "30")),
                'database': int(os.getenv("DATABASE_CHECK_INTERVAL", "30"))
            }
        )

        service = MonitoringService(
            message_broker=message_broker,
            config=service_config
        )

        # Start service
        await service.start()

        yield  # Service is running

    except Exception as e:
        logger.error(f"Service initialization failed: {e}")
        raise HTTPException(status_code=500, detail=f"Service initialization failed: {str(e)}")

    finally:
        # Cleanup
        if service:
            await service.stop()

# Initialize FastAPI app
app = FastAPI(
    title="Monitor Service",
    description="System monitoring and metrics collection service",
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
        "service_status": service._status
    }

@app.get("/metrics")
async def get_metrics():
    """Get Prometheus metrics"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        metrics = await service.get_prometheus_metrics()
        return Response(
            content=metrics,
            media_type="text/plain"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect metrics: {str(e)}")

@app.get("/status")
async def get_status():
    """Get detailed service status"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    return {
        "status": service.get_service_status()
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8003,
        reload=bool(os.getenv("DEBUG", False))
    )
