# src/config.py

from typing import Dict, List, Optional
from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

from .core.exceptions import ConfigurationError
from .utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class APIConfig:
    """API service configuration"""
    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    workers: int = 4
    reload: bool = False
    log_level: str = "info"
    root_path: str = ""
    docs_url: str = "/docs"
    openapi_url: str = "/openapi.json"

    # Rate limiting settings
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: int = 60  # seconds

@dataclass
class TimescaleConfig:
    """
    TimescaleDB specific configuration settings

    Attributes:
        chunk_interval: Time interval for each chunk (partition)
        compress_after: When to compress chunks for better storage efficiency
        drop_after: When to drop old chunks (never for full retention)
        retention_days: Number of days to keep historical data
        replication_factor: Number of replicas for high availability
    """
    chunk_interval: str = "7 days"
    compress_after: str = "30 days"
    drop_after: Optional[str] = None  # None means never drop
    retention_days: Optional[int] = None  # None means keep all history
    replication_factor: int = 1

    def __post_init__(self):
        # Convert retention days from env if provided
        retention_str = os.getenv('TIMESCALE_RETENTION_DAYS', '0')
        if int(retention_str):
            try:
                self.retention_days = int(retention_str)
            except ValueError:
                raise ConfigurationError(f"Invalid retention days value: {retention_str}")

@dataclass
class DatabaseConfig:
    """
    Database configuration settings for PostgreSQL with TimescaleDB

    Attributes:
        host: Database server hostname
        port: Database server port
        user: Database username
        password: Database password
        database: Database name
        pool_size: Size of the connection pool
        max_overflow: Maximum number of connections above pool_size
        pool_timeout: Seconds to wait for a connection from pool
        pool_recycle: Seconds before connections are recycled
        echo: Enable SQL query logging
        dialect: Database dialect (postgresql)
        driver: Database driver (asyncpg)
        timescale: TimescaleDB specific configuration
    """
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 1800
    echo: bool = False
    dialect: str = "postgresql"
    driver: str = "asyncpg"
    timescale: TimescaleConfig = field(default_factory=TimescaleConfig)

    @property
    def url(self) -> str:
        """Get database URL"""
        return f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def dsn(self) -> str:
        """Get database DSN (for asyncpg)"""
        return f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def __post_init__(self) -> None:
        """Validate database configuration"""
        if not self.host:
            raise ConfigurationError("Database host must be specified")
        if self.port <= 0:
            raise ConfigurationError("Invalid port number")
        if not self.user or not self.password:
            raise ConfigurationError("Database credentials must be specified")
        if not self.database:
            raise ConfigurationError("Database name must be specified")
        if self.pool_size <= 0:
            raise ConfigurationError("Pool size must be positive")
        if self.max_overflow < 0:
            raise ConfigurationError("Max overflow cannot be negative")
        if self.pool_timeout <= 0:
            raise ConfigurationError("Pool timeout must be positive")
        if self.pool_recycle <= 0:
            raise ConfigurationError("Pool recycle interval must be positive")

@dataclass
class BybitConfig:
    """Bybit-specific configuration"""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    testnet: bool = False
    kline_limit: int = 1000
    rate_limit: int = 600       # requests per window
    rate_limit_window: int = 5  # seconds

    def __post_init__(self) -> None:
        """Validate Bybit configuration"""
        if self.kline_limit <= 0 or self.kline_limit > 1000:
            raise ConfigurationError("Kline limit must be between 1 and 1000")
        if self.rate_limit <= 0:
            raise ConfigurationError("Rate limit must be positive")
        if self.rate_limit_window <= 0:
            raise ConfigurationError("Rate limit window must be positive")

@dataclass
class ExchangeConfig:
    """Exchange configuration"""
    bybit: BybitConfig

@dataclass
class MarketDataConfig:
    """
    Market data service configuration

    Attributes:
        sync_interval: How often to sync data (seconds)
        retry_interval: Time between retries on failure
        max_retries: Maximum number of retry attempts
        default_timeframes: List of timeframes to collect
        batch_size: Number of records to process in one batch
    """
    sync_interval: int = 300  # 5 minutes
    retry_interval: int = 60  # 1 minute
    max_retries: int = 3
    default_timeframe: str = '5'
    batch_size: int = 1000

    def __post_init__(self) -> None:
        """Validate market data configuration"""
        if self.sync_interval < 60:
            raise ConfigurationError("Sync interval must be at least 60 seconds")
        if self.retry_interval <= 0:
            raise ConfigurationError("Retry interval must be positive")
        if self.max_retries <= 0:
            raise ConfigurationError("Max retries must be positive")
        if self.batch_size <= 0:
            raise ConfigurationError("Batch size must be positive")
        if self.default_timeframe not in {'1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W'}:
            raise ConfigurationError("Invalid default timeframe")

@dataclass
class MonitoringConfig:
    """Configuration for monitoring service"""
    check_intervals: Dict[str, int] = field(
        default_factory=lambda: {
            'system': 30,      # System metrics need frequent updates
            'market': 120,      # Market data metrics can be less frequent
            'database': 60,    # Database metrics can be less frequent
        }
    )

    def __post_init__(self) -> None:
        """Validate configuration values"""
        if any([v < 10 for k, v in self.check_intervals.items()]):
            raise ConfigurationError("Collection interval must be at least 10 seconds")

@dataclass
class LogConfig:
    """Logging configuration"""
    level: str = "INFO"
    file_path: Optional[str] = None
    max_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

class Config:
    """
    Application configuration

    Provides centralized configuration management for all components
    of the application, including database, exchanges, market data,
    monitoring, and logging.
    """

    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Initialize components
        self.database = self._init_database_config()
        self.exchanges = self._init_exchange_config()
        self.market_data = self._init_market_data_config()
        self.monitoring = self._init_monitoring_config()
        self.logging = self._init_log_config()
        self.api = self._init_api_config()

    def _init_database_config(self) -> DatabaseConfig:
        """Initialize database configuration"""
        try:
            timescale_config = TimescaleConfig(
                chunk_interval=os.getenv('TIMESCALE_CHUNK_INTERVAL', '7 days'),
                compress_after=os.getenv('TIMESCALE_COMPRESS_AFTER', '30 days'),
                drop_after=os.getenv('TIMESCALE_DROP_AFTER', None),
                replication_factor=int(os.getenv('TIMESCALE_REPLICATION_FACTOR', '1'))
            )

            return DatabaseConfig(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                user=os.getenv('DB_USER', 'user'),
                password=os.getenv('DB_PASSWORD', 'password'),
                database=os.getenv('DB_NAME', 'coinwatch'),
                pool_size=int(os.getenv('DB_POOL_SIZE', '20')),
                max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '30')),
                pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30')),
                pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
                echo=bool(os.getenv('DB_ECHO', 'False').lower() == 'true'),
                dialect=os.getenv('DB_DIALECT', 'postgresql'),
                driver=os.getenv('DB_DRIVER', 'asyncpg'),
                timescale=timescale_config
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid database configuration: {e}")

    def _init_exchange_config(self) -> ExchangeConfig:
        """Initialize exchange configuration"""
        try:
            bybit_config = BybitConfig(
                api_key=os.getenv('BYBIT_API_KEY'),
                api_secret=os.getenv('BYBIT_API_SECRET'),
                testnet=bool(os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'),
                kline_limit=int(os.getenv('BYBIT_KLINE_LIMIT', '1000')),
                rate_limit=int(os.getenv('BYBIT_RATE_LIMIT', '600')),
                rate_limit_window=int(os.getenv('BYBIT_RATE_LIMIT_WINDOW', '300'))
            )
            return ExchangeConfig(bybit=bybit_config)
        except Exception as e:
            raise ConfigurationError(f"Invalid exchange configuration: {e}")

    def _init_market_data_config(self) -> MarketDataConfig:
        """Initialize market data configuration"""
        try:
            return MarketDataConfig(
                sync_interval=int(os.getenv('SYNC_INTERVAL', 300)),
                retry_interval=int(os.getenv('RETRY_INTERVAL', 60)),
                max_retries=int(os.getenv('MAX_RETRIES', 3)),
                default_timeframe=os.getenv('DEFAULT_TIMEFRAME', '5'),
                batch_size=int(os.getenv('MARKET_DATA_BATCH_SIZE', '1000'))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid market data configuration: {e}")

    def _init_monitoring_config(self) -> MonitoringConfig:
        """Initialize monitoring configuration"""
        try:
            return MonitoringConfig()
        except Exception as e:
            raise ConfigurationError(f"Invalid monitoring configuration: {e}")

    def _init_log_config(self) -> LogConfig:
        """Initialize logging configuration"""
        try:
            return LogConfig(
                level=os.getenv('LOG_LEVEL', 'INFO'),
                file_path=os.getenv('LOG_FILE'),
                max_size=int(os.getenv('LOG_MAX_SIZE', 10 * 1024 * 1024)),
                backup_count=int(os.getenv('LOG_BACKUP_COUNT', 5))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid logging configuration: {e}")

    def _init_api_config(self) -> APIConfig:
        """Initialize API configuration"""
        try:
            return APIConfig(
                host=os.getenv('API_HOST', '0.0.0.0'),
                port=int(os.getenv('API_PORT', '8000')),
                cors_origins=os.getenv('API_CORS_ORIGINS', '*').split(','),
                workers=int(os.getenv('API_WORKERS', '4')),
                reload=bool(os.getenv('API_RELOAD', 'false').lower() == 'true'),
                log_level=os.getenv('API_LOG_LEVEL', 'info'),
                rate_limit_enabled=bool(os.getenv('API_RATE_LIMIT_ENABLED', 'true').lower() == 'true'),
                rate_limit_requests=int(os.getenv('API_RATE_LIMIT_REQUESTS', '100')),
                rate_limit_window=int(os.getenv('API_RATE_LIMIT_WINDOW', '60'))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid API configuration: {e}")