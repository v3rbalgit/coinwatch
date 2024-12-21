# Coinwatch

Coinwatch is a comprehensive cryptocurrency data collection and analysis system designed to provide real-time and historical insights into the crypto market. It aggregates data from various sources, including cryptocurrency exchanges, on-chain data providers, and social media platforms, to offer a holistic view of market activity, fundamental metrics, and social sentiment. This system is built using a microservices architecture for scalability and maintainability.

## Features

- **Market Data Aggregation:** Collects and stores real-time and historical price data from various cryptocurrency exchanges.
- **Fundamental Data Collection:** Gathers fundamental metrics such as market capitalization, supply, and other relevant data.
- **Social Sentiment Analysis:** Aggregates and analyzes social sentiment data from platforms like Reddit, Twitter, and Telegram.
- **REST API:** Provides a comprehensive RESTful API for accessing collected and analyzed data.
- **Microservices Architecture:** Built using a microservices architecture for scalability, resilience, and independent deployment.
- **Containerized with Docker:** Easy to deploy and manage using Docker and Docker Compose.
- **Comprehensive Monitoring:** Monitors the health and performance of all services.

## System Architecture

Coinwatch is implemented using a microservices architecture. Each service is designed to handle specific aspects of the system, such as collecting market data, processing fundamental data, monitoring system health, and providing a unified API. The services communicate with each other primarily through synchronous HTTP requests.

The core components of the architecture are:

- **API Gateway (api):** The entry point for all external requests. It routes requests to the appropriate backend services, handles authentication and authorization, and can implement rate limiting.
- **Market Data Service (market_data):** Responsible for collecting, processing, and storing real-time market data from various cryptocurrency exchanges.
- **Fundamental Data Service (fundamental_data):** Collects and processes fundamental data, such as token metadata, market metrics, and social sentiment, from various sources.
- **Monitor Service (monitor):** Monitors the health and performance of the system and its components, collecting metrics and providing alerts.

### Inter-service Communication

Services communicate with each other using **synchronous HTTP requests**. The API Gateway acts as a reverse proxy, routing external requests to the appropriate internal services. Internal services may also make direct HTTP requests to other services when necessary.

For example, when a client requests market data for a specific symbol, the API Gateway routes the request to the Market Data Service. The Market Data Service then retrieves the requested data from its database and returns it to the API Gateway, which in turn sends it back to the client.

### Data Flow

1. **Collecting Market Data:**
   - The Market Data Service periodically fetches market data from cryptocurrency exchanges.
   - It processes and stores this data in its PostgreSQL database.
   - Other services can retrieve this data by sending HTTP requests to the Market Data Service's API endpoints.

2. **Collecting Fundamental Data:**
   - The Fundamental Data Service collects metadata, market metrics, and sentiment data from various sources (e.g., CoinGecko API, social media platforms).
   - This data is processed and stored in its PostgreSQL database.
   - Other services can access this data via HTTP requests to the Fundamental Data Service.

3. **API Request:**
   - A client sends a request to the API Gateway.
   - The API Gateway routes the request to the appropriate backend service based on the request path.
   - The backend service processes the request and retrieves data from its database or other services.
   - The backend service sends a response back to the API Gateway.
   - The API Gateway sends the response back to the client.

### Technology Stack

- **FastAPI:** A modern, fast (high-performance), web framework for building APIs with Python. Used by all services.
- **Uvicorn:** An ASGI server, used to run the FastAPI applications.
- **PostgreSQL:** The primary database used by the Market Data and Fundamental Data services to store collected data.
- **Redis:** Used for caching and rate limiting within the API Gateway.
- **Docker:** Used for containerizing the services, ensuring consistent deployment and scalability.
- **Docker Compose:** Used to define and manage multi-container Docker applications.

This microservices architecture allows for independent scaling and deployment of individual services, improving the system's resilience and maintainability.

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

## Usage

To use the Coinwatch API, follow these steps:

1. **Start the application** as described in the "Development Setup" section.
2. **Access the API documentation** at `http://localhost:8000/api/docs` to explore available endpoints and their parameters.
3. **Send requests** to the API endpoints using your preferred HTTP client.

**Example API Endpoints:**

- `GET /market/klines/{symbol}`: Get kline data for a specific symbol.
- `GET /fundamental/metadata/{symbol}`: Get metadata for a specific symbol.
- `GET /sentiment/{symbol}`: Get social sentiment data for a specific symbol.

## Contributing

Contributions to Coinwatch are welcome! To contribute, please follow these guidelines:

1. **Fork the repository** on GitHub.
2. **Create a new branch** for your feature or bug fix.
3. **Implement your changes** and ensure they are well-tested.
4. **Follow the project's coding standards.**
5. **Submit a pull request** with a clear description of your changes.

## License

This project is licensed under the MIT License.

## Contact

For questions or issues, please visit the project repository at [https://github.com/v3rbalgit/coinwatch](https://github.com/v3rbalgit/coinwatch).
