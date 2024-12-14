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
    prefix="/monitor",
    tags=["System Monitor"],
    lifespan=lifespan
)

@router.get("/metrics")
async def get_system_metrics(request: Request):
    """Get system-wide metrics in Prometheus format"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint="metrics"
    )

@router.get("/status")
async def get_system_status(request: Request):
    """Get comprehensive system status"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint="status"
    )

@router.get("/health")
async def get_health_status(request: Request):
    """Get health status of all services"""
    # Get status from registry directly
    service_status = registry.get_service_status()
    return {
        "status": "healthy" if all(s["healthy"] for s in service_status.values()) else "unhealthy",
        "services": service_status
    }

@router.get("/services/{service}/status")
async def get_service_status(service: str, request: Request):
    """Get detailed status for a specific service"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint=f"services/{service}/status"
    )

@router.get("/services/{service}/metrics")
async def get_service_metrics(service: str, request: Request):
    """Get metrics for a specific service"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint=f"services/{service}/metrics"
    )

@router.get("/errors")
async def get_error_summary(request: Request):
    """Get system-wide error summary"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint="errors"
    )

@router.get("/errors/{service}")
async def get_service_errors(service: str, request: Request):
    """Get errors for a specific service"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint=f"errors/{service}"
    )

@router.get("/resources")
async def get_resource_usage(request: Request):
    """Get system resource usage"""
    return await forwarder.forward_request(
        service="monitor",
        request=request,
        endpoint="resources"
    )
