# src/config.py

from typing import Dict, Optional
from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

from .core.exceptions import ConfigurationError
from .utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class TimescaleConfig:
    """
    TimescaleDB specific configuration settings

    Attributes:
        chunk_interval: Time interval for each chunk (partition)
        compress_after: When to compress chunks for better storage efficiency
        drop_after: When to drop old chunks (never for full retention)
        replication_factor: Number of replicas for high availability
    """
    chunk_interval: str = "7 days"
    compress_after: str = "30 days"
    drop_after: Optional[str] = None  # None means never drop
    replication_factor: int = 1

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

@dataclass
class BybitConfig:
    """Bybit-specific configuration"""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    testnet: bool = False
    kline_limit: int = 200
    rate_limit: int = 600       # requests per window
    rate_limit_window: int = 5  # seconds

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

@dataclass
class MonitoringConfig:
    """Configuration for monitoring service"""
    retention_hours: int = 24
    health_check_interval: int = 60
    max_backoff: int = 300
    base_backoff: int = 5
    max_error_history: int = 1000
    lock_timeout: float = 5.0
    check_intervals: Dict[str, int] = field(
        default_factory=lambda: {
            'resource': 30,
            'database': 60,
            'network': 60
        }
    )

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
                kline_limit=int(os.getenv('BYBIT_KLINE_LIMIT', '200')),
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
            return MonitoringConfig(
                retention_hours=int(os.getenv('MONITORING_RETENTION_HOURS', '24')),
                health_check_interval=int(os.getenv('MONITORING_HEALTH_CHECK_INTERVAL', '60')),
                max_backoff=int(os.getenv('MONITORING_MAX_BACKOFF', '300')),
                base_backoff=int(os.getenv('MONITORING_BASE_BACKOFF', '5')),
                max_error_history=int(os.getenv('MONITORING_MAX_ERROR_HISTORY', '1000')),
                lock_timeout=float(os.getenv('MONITORING_LOCK_TIMEOUT', '5.0')),
                check_intervals={
                    'resource': int(os.getenv('MONITORING_RESOURCE_INTERVAL', '30')),
                    'database': int(os.getenv('MONITORING_DATABASE_INTERVAL', '60')),
                    'network': int(os.getenv('MONITORING_NETWORK_INTERVAL', '60'))
                }
            )
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