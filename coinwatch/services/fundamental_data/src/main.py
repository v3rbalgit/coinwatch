import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.core.models import MarketMetricsModel, MetadataModel

from .service import FundamentalDataService
from shared.clients.coingecko import CoinGeckoAdapter
from shared.clients.exchanges import BybitAdapter
from shared.clients.registry import ExchangeAdapterRegistry
from shared.core.config import Config
from shared.database.connection import DatabaseConnection
from shared.database.repositories import MetadataRepository, MarketMetricsRepository, SentimentRepository
from shared.utils.logger import LoggerSetup
from shared.utils.time import get_current_timestamp

logger = LoggerSetup.setup(__name__)

# Service instance
service: FundamentalDataService | None = None
metrics_task: asyncio.Task | None = None


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

        # Initialize exchange registry
        exchange_registry = ExchangeAdapterRegistry()
        exchange_registry.register("bybit", BybitAdapter(config.adapters.bybit))

        # Initialize CoinGecko adapter
        coingecko_adapter = CoinGeckoAdapter(config.adapters.coingecko)

        service = FundamentalDataService(
            metadata_repository=metadata_repository,
            market_metrics_repository=market_metrics_repository,
            sentiment_repository=sentiment_repository,
            exchange_registry=exchange_registry,
            coingecko_adapter=coingecko_adapter,
            config=config.fundamental_data
        )

        # Start service and metrics publishing
        await service.start()

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

# Business endpoints
@app.get("/tokens/metadata", response_model=list[MetadataModel])
async def get_tokens_metadata(
    symbols: list[str] = Query(..., description="List of token symbols to fetch metadata for")
):
    """Get metadata for specified tokens"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        metadata = await service.metadata_repository.get_metadata(set(symbols))
        if not metadata:
            raise HTTPException(status_code=404, detail="No metadata found for specified tokens")
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch metadata: {str(e)}")

@app.get("/tokens/market", response_model=list[MarketMetricsModel])
async def get_tokens_market_metrics(
    symbols: list[str] = Query(..., description="List of token symbols to fetch metadata for")
):
    """Get market metrics for specified tokens"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        metadata = await service.market_metrics_repository.get_market_metrics(set(symbols))
        if not metadata:
            raise HTTPException(status_code=404, detail="No market metrics found for specified tokens")
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch market metrics : {str(e)}")

@app.get("/tokens/{symbol}/metadata", response_model=MetadataModel)
async def get_token_metadata(symbol: str):
    """Get metadata for a specific token"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        metadata = await service.metadata_repository.get_metadata({symbol})
        if not metadata:
            raise HTTPException(status_code=404, detail=f"No metadata found for token {symbol}")
        return metadata[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch metadata: {str(e)}")

@app.get("/tokens/{symbol}/market", response_model=MarketMetricsModel)
async def get_token_market_metrics(symbol: str):
    """Get market metrics for a specific token"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        metrics = await service.market_metrics_repository.get_market_metrics({symbol})
        if not metrics:
            raise HTTPException(status_code=404, detail=f"No market metrics found for token {symbol}")
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch market metrics: {str(e)}")

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

# Service endpoints
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
    """Get service metrics"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Calculate uptime
        uptime = 0.0
        if service._start_time is not None:
            uptime = (get_current_timestamp() - service._start_time) / 1000

        # Get error metrics
        recent_errors = service._error_tracker.get_recent_errors(60)
        error_types = {
            "metadata": len([e for e in recent_errors if "metadata" in str(e).lower()]),
            "market": len([e for e in recent_errors if "market" in str(e).lower()]),
            "warning": len([e for e in recent_errors if "warning" in str(e).lower()])
        }

        # Get collector metrics
        collectors = {}
        for name, collector in service._collectors.items():
            # Convert progress dataclass to dict if it exists
            current_progress = None
            if collector._current_progress:
                current_progress = {
                    "collector_type": collector._current_progress.collector_type,
                    "start_time": collector._current_progress.start_time.isoformat(),
                    "total_tokens": collector._current_progress.total_tokens,
                    "processed_tokens": collector._current_progress.processed_tokens,
                    "last_processed_token": collector._current_progress.last_processed_token
                }

            collectors[name] = {
                "running": collector._running,
                "active_tokens": len(collector._processing),
                "collection_interval": collector._collection_interval,
                "last_collection": collector._last_collection,
                "current_progress": current_progress
            }

        return {
            # Service metrics
            "service": {
                "status": service._status.value,
                "uptime_seconds": uptime,
                "last_error": str(service._last_error) if service._last_error else None,
                "active_tokens": len(service._active_tokens)
            },
            # Error metrics
            "errors": {
                "total": len(recent_errors),
                "by_type": error_types
            },
            # Collector metrics
            "collectors": collectors
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
