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
    prefix="/market",
    tags=["Market Data"],
    lifespan=lifespan
)

@router.get("/symbols")
async def get_symbols(request: Request):
    """Get all trading symbols"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint="symbols"
    )

@router.get("/symbols/{symbol}")
async def get_symbol(symbol: str, request: Request):
    """Get details for a specific symbol"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint=f"symbols/{symbol}"
    )

@router.get("/klines/{symbol}")
async def get_klines(symbol: str, request: Request):
    """Get candlestick data for a symbol"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint=f"klines/{symbol}"
    )

@router.get("/indicators/{symbol}/{indicator}")
async def get_indicator(
    symbol: str,
    indicator: str,
    request: Request
):
    """Get technical indicator values for a symbol"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint=f"indicators/{symbol}/{indicator}"
    )

@router.get("/status")
async def get_market_status(request: Request):
    """Get market data service status"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint="status"
    )

@router.get("/metrics")
async def get_market_metrics(request: Request):
    """Get market data service metrics"""
    return await forwarder.forward_request(
        service="market_data",
        request=request,
        endpoint="metrics"
    )
