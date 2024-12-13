# Coinwatch

A cryptocurrency data collection and analysis system that aggregates market data, fundamental metrics, and social sentiment information from multiple sources, now implemented as a microservices architecture.

## System Architecture

### Message-Based Communication
The system uses RabbitMQ for inter-service communication, replacing the previous ServiceCoordinator with a more scalable, loosely-coupled approach.

Message Types:
```python
class MarketDataMessage(str, Enum):
    SYMBOL_ADDED = "market.symbol.added"
    SYMBOL_UPDATED = "market.symbol.updated"
    SYMBOL_DELISTED = "market.symbol.delisted"
    DATA_SYNCHRONIZED = "market.data.synchronized"

class FundamentalDataMessage(str, Enum):
    METADATA_UPDATED = "fundamental.metadata.updated"
    METRICS_UPDATED = "fundamental.metrics.updated"
    SENTIMENT_UPDATED = "fundamental.sentiment.updated"

class MonitorMessage(str, Enum):
    SERVICE_ERROR = "monitor.service.error"
    METRICS_COLLECTED = "monitor.metrics.collected"
```

Services can:
- Publish messages to topics
- Subscribe to message topics
- Process messages asynchronously
- Handle message failures with DLQ

Example message flow:
```python
# Publishing a message
await message_broker.publish(
    "market.symbols",
    MarketDataMessage(
        type=MarketDataMessage.SYMBOL_ADDED,
        data={"symbol": symbol_info}
    )
)

# Subscribing to messages
@message_broker.subscribe("market.symbols")
async def handle_symbol_update(message: MarketDataMessage):
    if message.type == MarketDataMessage.SYMBOL_ADDED:
        await process_new_symbol(message.data["symbol"])
```

## Architecture Evolution

### Key Improvements

1. Service Independence
```
Old:
- Services shared same process
- Direct method calls
- Tight coupling
- Single point of failure

New:
- Independent processes
- Message-based communication
- Loose coupling
- Fault isolation
```

2. Scalability
```
Old:
- Vertical scaling only
- Shared resources
- Memory constraints
- Single database connection pool

New:
- Horizontal & vertical scaling
- Isolated resources
- Independent scaling
- Service-specific pools
```

3. Reliability
```
Old:
- Service failures affect entire system
- Complex error recovery
- Monolithic deployment
- All-or-nothing updates

New:
- Isolated failures
- Built-in retry mechanisms
- Independent deployments
- Rolling updates
```

4. Development
```
Old:
- Large codebase
- Complex dependencies
- Difficult testing
- Long deployment cycles

New:
- Smaller, focused codebases
- Clear boundaries
- Easier testing
- Quick deployments
```

### Migration Benefits

1. Performance
- Independent service scaling
- Optimized resource usage
- Better cache utilization
- Reduced memory pressure

2. Maintainability
- Clear service boundaries
- Easier debugging
- Simpler deployments
- Better monitoring

3. Development
- Parallel development
- Focused testing
- Clear ownership
- Faster iterations

4. Operations
- Granular scaling
- Better resource utilization
- Simplified troubleshooting
- Flexible deployment options

### Architectural Patterns

1. Message Patterns
```python
# Old: Direct service calls
await market_data_service.add_symbol(symbol)

# New: Message-based communication
await message_broker.publish(
    "market.symbols",
    MarketDataMessage(
        type=MarketDataMessage.SYMBOL_ADDED,
        data={"symbol": symbol}
    )
)
```

2. Service Discovery
```python
# Old: Direct service references
service = coordinator.get_service("market_data")

# New: Dynamic service discovery
service_url = await registry.get_service_url("market_data")
```

3. Error Handling
```python
# Old: Direct error propagation
try:
    await service.process()
except ServiceError:
    await coordinator.handle_error()

# New: Message-based error handling
@message_broker.subscribe("monitor.errors")
async def handle_error(error_message):
    await error_handler.process(error_message)
```

4. Configuration
```python
# Old: Shared configuration
config = Config()
market_data = config.market_data

# New: Service-specific configuration
market_data_config = MarketDataConfig()
fundamental_data_config = FundamentalDataConfig()
```

### Data Flow Examples

1. Adding New Symbol
```
Old:
Client -> API -> ServiceCoordinator -> MarketDataService
                                  -> FundamentalDataService

New:
Client -> API Gateway -> Market Data Service
                     -> Message: SYMBOL_ADDED
                        -> Fundamental Data Service
                        -> Monitor Service
```

2. Collecting Market Data
```
Old:
MarketDataService -> ServiceCoordinator -> Database
                                      -> MonitorService

New:
Market Data Service -> Database
                   -> Message: DATA_SYNCHRONIZED
                      -> Monitor Service
                      -> Fundamental Service
```

3. Error Handling
```
Old:
Service -> ServiceCoordinator -> MonitorService
                             -> Log Files

New:
Service -> Error Message -> Monitor Service
                        -> Metrics
                        -> Alerts
```

This evolution represents a significant improvement in system architecture, providing better scalability, reliability, and maintainability while maintaining all the functionality of the original system.

### Services

#### 1. Market Data Service
Handles collection and processing of exchange data:
- Maintains list of active trading pairs
- Collects OHLCV data at configured intervals
- Verifies data integrity and fills gaps
- Processes technical indicators

Key components:
```python
# Service structure
services/market_data/
├── src/
│   ├── main.py           # FastAPI application
│   ├── service.py        # Core service logic
│   ├── collector.py      # Data collection
│   └── synchronizer.py   # Data synchronization
├── Dockerfile
└── requirements.txt
```

Configuration:
```python
@dataclass
class MarketDataConfig:
    """Market data service configuration"""
    sync_interval: int = 300  # 5 minutes
    retry_interval: int = 60  # 1 minute
    max_retries: int = 3
    default_timeframe: str = '5'
    batch_size: int = 1000
```

#### 2. Fundamental Data Service
Collects and processes fundamental and social data through multiple collectors:

Service Structure:
```python
services/fundamental_data/
├── src/
│   ├── main.py          # FastAPI application
│   ├── service.py       # Service coordination
│   └── collectors/      # Data collectors
│       ├── metadata.py
│       ├── market.py
│       └── sentiment.py
├── Dockerfile
└── requirements.txt
```

Configuration:
```python
@dataclass
class FundamentalDataConfig:
    """Configuration for fundamental data collection"""
    collection_intervals: Dict[str, int] = field(
        default_factory=lambda: {
            'metadata': 86400 * 7,   # Weekly
            'market': 3600,          # Hourly
            'sentiment': 86400       # Daily
        }
    )
    batch_sizes: Dict[str, int] = field(
        default_factory=lambda: {
            'metadata': 10,
            'market': 100,
            'sentiment': 50
        }
    )
```

#### 3. Monitor Service
Tracks system health and performance:
- Service status monitoring
- Performance metrics collection
- Error tracking and reporting

Service Structure:
```python
services/monitor/
├── src/
│   ├── main.py              # FastAPI application
│   ├── service.py           # Core monitoring
│   ├── metrics_collector.py # Metrics collection
│   └── monitor_metrics.py   # Metrics definitions
├── Dockerfile
└── requirements.txt
```

Metrics Collection:
```python
@dataclass
class ServiceMetrics:
    """Service health metrics"""
    service_name: str
    status: ServiceStatus
    uptime: int
    memory_usage: float
    cpu_usage: float
    error_count: int
    last_error: Optional[str]
    collection_stats: Dict[str, Any]
```

#### 4. API Gateway
Serves as the unified entry point for all services:
- Request routing
- Rate limiting
- Service discovery
- Health monitoring

Service Structure:
```python
services/api/
├── src/
│   ├── main.py                 # FastAPI application
│   ├── service_registry.py     # Service discovery
│   ├── middleware/
│   │   ├── rate_limit.py      # Rate limiting
│   │   └── request_forwarder.py # Request routing
│   └── routers/
│       ├── market.py          # Market data routes
│       ├── fundamental.py     # Fundamental data routes
│       └── monitor.py         # System monitoring routes
├── Dockerfile
└── requirements.txt
```

Configuration:
```python
@dataclass
class APIConfig:
    """API Gateway configuration"""
    port: int = 8000
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    redis_url: str = "redis://localhost:6379"
```

### Shared Components

#### 1. Database Layer
Located in shared/database/:
```python
shared/database/
├── models/              # Database models
│   ├── market_data/
│   └── fundamental_data/
├── repositories/        # Data access
└── connection.py       # DB connection
```

#### 2. Message Broker
Located in shared/messaging/:
```python
shared/messaging/
├── broker.py          # Message broker client
├── schemas.py         # Message schemas
└── handlers.py        # Message handlers
```

#### 3. Core Utilities
Located in shared/core/:
```python
shared/core/
├── config.py         # Configuration
├── exceptions.py     # Custom exceptions
└── models.py         # Shared models
```

### Infrastructure

#### 1. Database (PostgreSQL/TimescaleDB)
- Time-series optimized storage
- Automatic data partitioning
- Efficient time-range queries

#### 2. Message Broker (RabbitMQ)
- Topic-based routing
- Message persistence
- Dead letter queues
- Publisher confirms

#### 3. Cache (Redis)
- API rate limiting
- Response caching
- Session storage

## Development Setup

### Prerequisites
1. Software Requirements:
   - Docker & Docker Compose
   - Python 3.9+
   - PostgreSQL client
   - Node.js (for development scripts)

2. API Keys Required:
   - Bybit API credentials
   - CoinGecko API key
   - Social media API keys

### Installation

1. Clone and Setup:
```bash
git clone https://github.com/yourusername/coinwatch.git
cd coinwatch
cp .env.example .env
# Edit .env with your configuration
```

2. Start Services:
```bash
# Start infrastructure
docker-compose up -d postgres rabbitmq redis

# Start application services
docker-compose up -d market_data fundamental_data monitor api
```

3. View Logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f market_data
```

### Development Workflow

1. Service Development:
```bash
# Create new service
mkdir -p services/new_service/{src,tests}
touch services/new_service/{Dockerfile,requirements.txt}

# Run service tests
cd services/new_service
pytest tests/
```

2. Shared Code Changes:
```bash
# Update shared package
cd shared
pip install -e .

# Run shared tests
pytest tests/
```

3. API Development:
```bash
# Start API in development mode
cd services/api
uvicorn src.main:app --reload

# View API docs
open http://localhost:8000/api/docs
```

## Current Status

### Completed Features
- Basic service separation
- Message broker integration
- API Gateway implementation
- Service discovery
- Rate limiting
- Health monitoring

### In Progress
- Service-specific tests
- Documentation updates
- Monitoring dashboards
- Development tooling

### Pending Tasks
- Remove old src/ directory
- Complete service migrations
- Add integration tests
- Setup CI/CD pipeline

## Migration Guide

### Phase 1: Infrastructure (Completed)
- Setup message broker
- Configure databases
- Setup monitoring

### Phase 2: Services (In Progress)
- Implement new services
- Add health checks
- Setup logging

### Phase 3: Testing (Pending)
- Unit tests
- Integration tests
- Performance tests

### Phase 4: Cleanup (Pending)
- Remove old code
- Update documentation
- Optimize configurations

## Contributing

1. Code Structure:
```
coinwatch/
├── services/          # Microservices
├── shared/           # Shared code
├── scripts/          # Development scripts
└── docs/            # Documentation
```

2. Development Guidelines:
- Follow service structure
- Add tests for changes
- Update documentation
- Use shared components

## License

[Your License Here]

## Contact

[Your Contact Information]
