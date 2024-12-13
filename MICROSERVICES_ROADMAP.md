# Coinwatch Microservices Migration Roadmap

This document outlines the step-by-step process for migrating the Coinwatch application from its current monolithic architecture to a microservices architecture. Each phase is designed to maintain application functionality throughout the transition.

## Current Architecture Overview

The application currently consists of:
- Services:
  * Market Data Service: Handles market data collection and processing
  * Fundamental Data Service: Manages metadata, sentiment, and market metrics
  * Monitor Service: Tracks system performance
  * API Service: Serves data to frontend (partially implemented)
- Core Components:
  * ServiceCoordinator: Manages inter-service communication
  * DatabaseService: Handles database operations
  * Repositories: Interface with the database
  * Managers: Handle specific computations (e.g., IndicatorManager)
  * Collectors: Gather data from various sources

## Migration Phases

### Phase 1: Project Restructuring (3-4 days)

1. Create New Directory Structure
```
coinwatch/
├── shared/                    # Shared code package
│   ├── models/               # Domain models
│   │   ├── __init__.py
│   │   ├── base.py          # Move from src/models/base.py
│   │   ├── kline.py         # Move from src/models/kline.py
│   │   ├── market.py        # Move from src/models/market.py
│   │   ├── metadata.py      # Move from src/models/metadata.py
│   │   ├── platform.py      # Move from src/models/platform.py
│   │   ├── sentiment.py     # Move from src/models/sentiment.py
│   │   └── symbol.py        # Move from src/models/symbol.py
│   │
│   ├── core/                # Core functionality
│   │   ├── __init__.py
│   │   ├── exceptions.py    # Move from src/core/exceptions.py
│   │   ├── models.py        # Move from src/core/models.py
│   │   └── monitoring.py    # Move from src/core/monitoring.py
│   │
│   ├── utils/              # Shared utilities
│   │   ├── __init__.py
│   │   ├── cache.py        # Move from src/utils/cache.py
│   │   ├── domain_types.py # Move from src/utils/domain_types.py
│   │   ├── error.py        # Move from src/utils/error.py
│   │   ├── logger.py       # Move from src/utils/logger.py
│   │   ├── progress.py     # Move from src/utils/progress.py
│   │   ├── rate_limit.py   # Move from src/utils/rate_limit.py
│   │   ├── retry.py        # Move from src/utils/retry.py
│   │   ├── time.py         # Move from src/utils/time.py
│   │   └── validation.py   # Move from src/utils/validation.py
│   │
│   └── database/           # Database components
│       ├── __init__.py
│       ├── migrations/     # New Alembic migrations directory
│       └── repositories/   # All repositories
│           ├── __init__.py
│           ├── kline.py    # Move from src/repositories/kline.py
│           ├── market.py   # Move from src/repositories/market.py
│           ├── metadata.py # Move from src/repositories/metadata.py
│           ├── platform.py # Move from src/repositories/platform.py
│           ├── sentiment.py # Move from src/repositories/sentiment.py
│           └── symbol.py   # Move from src/repositories/symbol.py
│
├── services/               # Individual services
│   ├── market_data/       # Market data microservice
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── main.py   # Service entry point
│   │   │   ├── service.py # Move from src/services/market_data/service.py
│   │   │   ├── collector.py # Move from src/services/market_data/collector.py
│   │   │   ├── synchronizer.py # Move from src/services/market_data/synchronizer.py
│   │   │   └── managers/  # Related managers
│   │   │       ├── __init__.py
│   │   │       └── indicator.py # Move from src/managers/indicator.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── fundamental_data/  # Fundamental data microservice
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── main.py   # Service entry point
│   │   │   ├── service.py # Move from src/services/fundamental_data/service.py
│   │   │   └── collectors/ # All collectors
│   │   │       ├── __init__.py
│   │   │       ├── base.py # Move from src/services/fundamental_data/collector.py
│   │   │       ├── metadata.py # Move from src/services/fundamental_data/metadata_collector.py
│   │   │       ├── market_metrics.py # Move from src/services/fundamental_data/market_metrics_collector.py
│   │   │       └── sentiment.py # Move from src/services/fundamental_data/sentiment_metrics_collector.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── monitor/          # Monitoring microservice
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── main.py  # Service entry point
│   │   │   ├── service.py # Move from src/services/monitor/service.py
│   │   │   ├── metrics_collector.py # Move from src/services/monitor/metrics_collector.py
│   │   │   └── monitor_metrics.py # Move from src/services/monitor/monitor_metrics.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── api/             # API Gateway service
│       ├── src/
│       │   ├── __init__.py
│       │   ├── main.py # Service entry point
│       │   ├── service.py # Move from src/services/api/service.py
│       │   └── routers/ # API routes (to be implemented)
│       │       ├── __init__.py
│       │       ├── market.py
│       │       ├── fundamental.py
│       │       └── monitor.py
│       ├── Dockerfile
│       └── requirements.txt
```

2. Setup Package Management
   - Create requirements.txt for each service with specific dependencies
   - Create setup.py for shared package
   - Update main requirements.txt to include all dependencies

3. Create Docker Configuration
   - Create Dockerfile for each service
   - Create docker-compose.yml for local development
   - Update docker-compose.prod.yml for production

### Phase 2: Database Migration (2-3 days)

1. Setup Alembic
```bash
cd shared/database
alembic init migrations
```

2. Create Initial Migration
   - Configure alembic.ini
   - Create migration script from existing models
   - Test migration up/down

3. Update Database Service
   - Move connection management to shared package
   - Implement connection pooling
   - Add health checks

### Phase 3: Message Broker Integration (2-3 days)

1. Setup RabbitMQ Infrastructure
```yaml
# Add to docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
```

2. Create Message Broker Service
```python
# shared/core/messaging.py
class MessageBroker:
    async def connect(self):
        # Setup RabbitMQ connection

    async def publish(self, topic: str, message: dict):
        # Publish message to topic

    async def subscribe(self, topic: str, handler: Callable):
        # Subscribe to topic
```

3. Define Message Schemas
   - Create message types for each event
   - Implement serialization/deserialization
   - Add validation

### Phase 4: Service Migration (2-3 weeks)

1. Market Data Service (4-5 days)
   - Create FastAPI application structure
   - Move market data collection logic
   - Implement service endpoints
   - Add health checks
   - Update dependencies
   - Create tests

2. Fundamental Data Service (4-5 days)
   - Create FastAPI application structure
   - Move collectors
   - Implement service endpoints
   - Add health checks
   - Update dependencies
   - Create tests

3. Monitor Service (2-3 days)
   - Create FastAPI application structure
   - Move monitoring logic
   - Implement service endpoints
   - Add health checks
   - Update dependencies
   - Create tests

4. API Gateway Service (4-5 days)
   - Create FastAPI application structure
   - Implement routing logic:
     ```python
     # services/api/src/main.py
     from fastapi import FastAPI
     from .routers import market_data, fundamental_data, monitor

     app = FastAPI(
         title="Coinwatch API",
         description="Market data and technical analysis API",
         version="1.0.0"
     )

     # Configure routers
     app.include_router(market_data.router, prefix="/api/v1/market-data")
     app.include_router(fundamental_data.router, prefix="/api/v1/fundamental")
     app.include_router(monitor.router, prefix="/api/v1/system")
     ```

   - Implement service discovery:
     ```python
     # services/api/src/service_registry.py
     class ServiceRegistry:
         def __init__(self):
             self.services = {
                 'market_data': 'http://market-data:8001',
                 'fundamental_data': 'http://fundamental-data:8002',
                 'monitor': 'http://monitor:8003'
             }

         async def get_service_url(self, service_name: str) -> str:
             return self.services.get(service_name)
     ```

   - Setup request forwarding:
     ```python
     # services/api/src/routers/market_data.py
     from fastapi import APIRouter, HTTPException
     from ..service_registry import ServiceRegistry

     router = APIRouter()
     registry = ServiceRegistry()

     @router.get("/symbols")
     async def get_symbols():
         service_url = await registry.get_service_url('market_data')
         # Forward request to market data service
         response = await forward_request(f"{service_url}/symbols")
         return response
     ```

   - Implement error handling and rate limiting:
     ```python
     # services/api/src/middleware/rate_limit.py
     from fastapi import Request
     from fastapi.responses import JSONResponse
     from ..utils.rate_limit import RateLimiter

     async def rate_limit_middleware(request: Request, call_next):
         limiter = RateLimiter()
         if not await limiter.check_rate_limit(request):
             return JSONResponse(
                 status_code=429,
                 content={"error": "Rate limit exceeded"}
             )
         return await call_next(request)
     ```

5. Service Communication (2-3 days)
   - Setup message patterns:
     ```python
     # shared/core/messaging.py
     from enum import Enum

     class MessageType(Enum):
         SYMBOL_ADDED = "symbol.added"
         SYMBOL_UPDATED = "symbol.updated"
         SYMBOL_DELETED = "symbol.deleted"
         MARKET_DATA_UPDATED = "market.updated"
         FUNDAMENTAL_DATA_UPDATED = "fundamental.updated"

     class Message:
         def __init__(self, type: MessageType, data: dict):
             self.type = type
             self.data = data
     ```

   - Implement service-specific handlers:
     ```python
     # services/market_data/src/messaging.py
     class MarketDataMessageHandler:
         async def handle_symbol_added(self, message: Message):
             symbol = message.data['symbol']
             await self.service.add_symbol(symbol)

         async def handle_symbol_deleted(self, message: Message):
             symbol = message.data['symbol']
             await self.service.remove_symbol(symbol)
     ```

### Phase 5: Local Development Setup (3-4 days)

1. Create Development Scripts
```bash
# scripts/dev/
├── start.sh    # Start all services
├── stop.sh     # Stop all services
├── logs.sh     # View service logs
└── test.sh     # Run tests
```

2. Setup Local Environment
```yaml
# docker-compose.override.yml
services:
  market_data:
    volumes:
      - ./:/app
    environment:
      - DEBUG=1
```

3. Create Development Documentation
   - Local setup instructions
   - Testing procedures
   - Debugging guide

4. Development Environment Configuration
   ```yaml
   # docker-compose.override.yml
   services:
     market_data:
       build:
         context: .
         dockerfile: services/market_data/Dockerfile
       volumes:
         - ./services/market_data:/app/services/market_data
         - ./shared:/app/shared
       environment:
         - PYTHONPATH=/app
         - DEBUG=1
         - RELOAD=1

     fundamental_data:
       build:
         context: .
         dockerfile: services/fundamental_data/Dockerfile
       volumes:
         - ./services/fundamental_data:/app/services/fundamental_data
         - ./shared:/app/shared
       environment:
         - PYTHONPATH=/app
         - DEBUG=1
         - RELOAD=1

     api:
       build:
         context: .
         dockerfile: services/api/Dockerfile
       volumes:
         - ./services/api:/app/services/api
         - ./shared:/app/shared
       environment:
         - PYTHONPATH=/app
         - DEBUG=1
         - RELOAD=1
       ports:
         - "8000:8000"
   ```

5. Development Scripts
   ```bash
   # scripts/dev/start.sh
   #!/bin/bash

   # Start infrastructure services
   docker-compose up -d rabbitmq postgres

   # Wait for infrastructure to be ready
   ./scripts/dev/wait-for-it.sh localhost:5672 -t 60
   ./scripts/dev/wait-for-it.sh localhost:5432 -t 60

   # Run database migrations
   docker-compose run --rm market_data alembic upgrade head

   # Start application services
   docker-compose up -d market_data fundamental_data monitor api
   ```

### Phase 6: Testing and Validation (1 week)

1. Create Integration Tests
   - Test service communication
   - Verify data flow
   - Check error handling

2. Performance Testing
   - Measure service latency
   - Check resource usage
   - Verify scalability

3. Documentation
   - Update API documentation
   - Create service documentation
   - Update deployment guides

## Development Workflow

1. Local Development
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f service_name

# Run tests
docker-compose run service_name pytest
```

2. Making Changes
   - Edit code in your IDE
   - Services hot-reload changes
   - Run relevant tests
   - Commit changes

3. Debugging
   - Check service logs
   - Use FastAPI debug endpoints
   - Monitor RabbitMQ dashboard

## Service Dependencies

Here's how the services depend on each other:

1. Market Data Service
   - Dependencies:
     * Database Service (PostgreSQL/TimescaleDB)
     * Message Broker (RabbitMQ)
   - Provides:
     * Market data endpoints
     * Technical indicators
     * Symbol management

2. Fundamental Data Service
   - Dependencies:
     * Database Service
     * Message Broker
     * External APIs (CoinGecko, social media)
   - Provides:
     * Metadata endpoints
     * Sentiment analysis
     * Market metrics

3. Monitor Service
   - Dependencies:
     * Message Broker
     * Database Service (metrics storage)
   - Provides:
     * System metrics
     * Service health status
     * Performance monitoring

4. API Gateway
   - Dependencies:
     * All other services
     * Redis (rate limiting, caching)
   - Provides:
     * REST API endpoints
     * Request routing
     * Rate limiting
     * Error handling

## Data Flow

1. Market Data Flow:
   ```
   Exchange APIs -> Market Data Service -> Message Broker ->
   [Fundamental Data Service, Monitor Service] -> Database
   ```

2. Fundamental Data Flow:
   ```
   External APIs -> Fundamental Data Service -> Message Broker ->
   [Market Data Service, Monitor Service] -> Database
   ```

3. API Request Flow:
   ```
   Client -> API Gateway -> Service Discovery ->
   Appropriate Service -> Database -> Response
   ```

## Prerequisites

1. Software Requirements
   - Docker & Docker Compose
   - Python 3.9+
   - PostgreSQL client
   - RabbitMQ client

2. Environment Setup
   - Copy .env.example to .env
   - Configure database credentials
   - Setup API keys

## Notes

- Each phase should be completed and tested before moving to the next
- Maintain backwards compatibility during migration
- Keep services running in parallel during transition
- Document all changes and update tests
- Monitor performance throughout migration

## Rollback Plan

1. Keep old code structure until migration is complete
2. Maintain database backups
3. Document all changes for potential rollback
4. Test rollback procedures

This roadmap provides a structured approach to migrating the Coinwatch application to a microservices architecture while maintaining functionality and minimizing risks.
