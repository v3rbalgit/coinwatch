from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from shared.utils.logger import LoggerSetup
from .middleware.rate_limit import rate_limit_middleware
from .routers import market, fundamental, monitor
from .service_registry import ServiceRegistry

logger = LoggerSetup.setup(__name__)

# Shared instances
registry = ServiceRegistry()
redis: Redis | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Service lifecycle manager"""
    global redis

    try:
        # Initialize Redis connection
        redis = Redis.from_url(
            "redis://redis:6379",
            decode_responses=True
        )

        # Check service health
        service_status = await registry.check_all_services()
        unhealthy = [
            service for service, healthy in service_status.items()
            if not healthy
        ]
        if unhealthy:
            logger.warning(f"Unhealthy services detected: {unhealthy}")

        yield  # Service is running

    except Exception as e:
        logger.error(f"Service initialization failed: {e}")
        raise

    finally:
        # Cleanup
        if redis:
            await redis.close()

# Initialize FastAPI app
app = FastAPI(
    title="Coinwatch API",
    description="Market data and technical analysis API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting middleware
app.middleware("http")(rate_limit_middleware)

# Configure routers with API versioning
app.include_router(market.router, prefix="/api/v1")
app.include_router(fundamental.router, prefix="/api/v1")
app.include_router(monitor.router, prefix="/api/v1")

@app.get("/health")
async def health_check():
    """API Gateway health check"""
    service_status = registry.get_service_status()

    # Check Redis health
    redis_status = "unhealthy"
    if redis:
        try:
            await redis.ping()
            redis_status = "healthy"
        except Exception:
            pass

    return {
        "status": "healthy",
        "components": {
            "redis": redis_status,
            "services": service_status
        }
    }

@app.get("/")
async def root():
    """API root - redirect to docs"""
    return {
        "message": "Welcome to Coinwatch API",
        "docs": "/api/docs",
        "redoc": "/api/redoc"
    }

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )
