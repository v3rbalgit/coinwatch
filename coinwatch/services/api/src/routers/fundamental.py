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

# Token metadata endpoints
@router.get("/metadata")
async def get_tokens_metadata(request: Request):
    """Get metadata for multiple tokens"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="tokens/metadata"
    )

@router.get("/metadata/{symbol}")
async def get_token_metadata(symbol: str, request: Request):
    """Get metadata for a specific token"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"tokens/{symbol}/metadata"
    )

# Market metrics endpoints
@router.get("/market")
async def get_tokens_market_metrics(request: Request):
    """Get market metrics for multiple tokens"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint="tokens/market"
    )

@router.get("/market/{symbol}")
async def get_token_market_metrics(symbol: str, request: Request):
    """Get market metrics for a specific token"""
    return await forwarder.forward_request(
        service="fundamental_data",
        request=request,
        endpoint=f"tokens/{symbol}/market"
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
