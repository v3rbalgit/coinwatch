from contextlib import asynccontextmanager
from fastapi import APIRouter, Request
from ..service_registry import ServiceRegistry
from ..middleware.request_forwarder import RequestForwarder

# Shared instances
registry = ServiceRegistry()
forwarder = RequestForwarder(registry)

# Router with lifespan
@asynccontextmanager
async def lifespan(app: APIRouter):
    yield
    await forwarder.close()

router = APIRouter(
    prefix="/fundamental",
    tags=["Fundamental Data"],
    lifespan=lifespan
)

# Metadata endpoints
@router.get("/metadata/{symbol}")
async def get_metadata(symbol: str, request: Request):
    """Get token metadata"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"metadata/{symbol}"
    )

@router.get("/metadata/search")
async def search_metadata(request: Request):
    """Search tokens by metadata"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="metadata/search"
    )

# Market metrics endpoints
@router.get("/metrics/{symbol}")
async def get_market_metrics(symbol: str, request: Request):
    """Get market metrics for a token"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"metrics/{symbol}"
    )

@router.get("/metrics/{symbol}/history")
async def get_metrics_history(symbol: str, request: Request):
    """Get historical market metrics"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"metrics/{symbol}/history"
    )

# Sentiment endpoints
@router.get("/sentiment/{symbol}")
async def get_sentiment(symbol: str, request: Request):
    """Get current sentiment data"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"sentiment/{symbol}"
    )

@router.get("/sentiment/{symbol}/history")
async def get_sentiment_history(symbol: str, request: Request):
    """Get historical sentiment data"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"sentiment/{symbol}/history"
    )

@router.get("/sentiment/sources")
async def get_sentiment_sources(request: Request):
    """Get available sentiment data sources"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="sentiment/sources"
    )

# Service status endpoints
@router.get("/status")
async def get_service_status(request: Request):
    """Get fundamental data service status"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="status"
    )

@router.get("/metrics")
async def get_service_metrics(request: Request):
    """Get fundamental data service metrics"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="metrics"
    )
