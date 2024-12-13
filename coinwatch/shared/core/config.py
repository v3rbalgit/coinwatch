from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import os
from dotenv import load_dotenv
from sqlalchemy import AsyncAdaptedQueuePool

from .exceptions import ConfigurationError
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class MessageBrokerConfig:
    """Message broker configuration for inter-service communication"""
    url: str = "amqp://guest:guest@localhost/"
    connection_timeout: int = 30
    heartbeat: int = 60
    blocked_connection_timeout: int = 30

    def __post_init__(self) -> None:
        """Validate message broker configuration"""
        if not self.url:
            raise ConfigurationError("Message broker URL must be specified")
        if self.connection_timeout <= 0:
            raise ConfigurationError("Connection timeout must be positive")
        if self.heartbeat <= 0:
            raise ConfigurationError("Heartbeat must be positive")

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
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

    @property
    def url(self) -> str:
        """Get database URL"""
        return f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine_options(self) -> Dict[str, Any]:
        """Get SQLAlchemy engine options"""
        return {
            'poolclass': AsyncAdaptedQueuePool,
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'echo': self.echo
        }

@dataclass
class MarketDataConfig:
    """Market data service configuration"""
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

@dataclass
class SentimentConfig:
    """Configuration for sentiment analysis APIs"""
    twitter_api_key: Optional[str] = None
    twitter_api_host: Optional[str] = None
    twitter_rate_limit: int = 950
    twitter_rate_limit_window: int = 86400

    reddit_client_id: Optional[str] = None
    reddit_client_secret: Optional[str] = None
    reddit_rate_limit: int = 95
    reddit_rate_limit_window: int = 60
    telegram_api_id: Optional[int] = None
    telegram_api_hash: Optional[str] = None
    telegram_session_name: str = "coinwatch_bot"

@dataclass
class FundamentalDataConfig:
    """Configuration for fundamental data collection"""
    collection_intervals: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 86400 * 7,   # Weekly
        'market': 3600,          # Hourly
        'sentiment': 86400       # Daily
    })
    batch_sizes: Dict[str, int] = field(default_factory=lambda: {
        'metadata': 10,
        'market': 100,
        'sentiment': 50
    })
    sentiment: SentimentConfig = field(default_factory=SentimentConfig)

@dataclass
class MonitorConfig:
    """Configuration for monitoring service"""
    check_intervals: Dict[str, int] = field(
        default_factory=lambda: {
            'system': 30,      # System metrics
            'market': 120,     # Market data metrics
            'database': 60,    # Database metrics
        }
    )

@dataclass
class APIConfig:
    """API Gateway configuration"""
    port: int = 8000
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    redis_url: str = "redis://localhost:6379"

@dataclass
class LogConfig:
    """Logging configuration"""
    level: str = "INFO"
    file_path: Optional[str] = None
    max_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

class Config:
    """Application configuration"""

    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Initialize components
        self.database = self._init_database_config()
        self.message_broker = self._init_message_broker_config()
        self.market_data = self._init_market_data_config()
        self.fundamental_data = self._init_fundamental_data_config()
        self.monitoring = self._init_monitor_config()
        self.api = self._init_api_config()

    def _init_database_config(self) -> DatabaseConfig:
        """Initialize database configuration"""
        try:
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
                echo=bool(os.getenv('DB_ECHO', 'False').lower() == 'true')
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid database configuration: {e}")

    def _init_message_broker_config(self) -> MessageBrokerConfig:
        """Initialize message broker configuration"""
        try:
            return MessageBrokerConfig(
                url=os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost/'),
                connection_timeout=int(os.getenv('RABBITMQ_CONNECTION_TIMEOUT', '30')),
                heartbeat=int(os.getenv('RABBITMQ_HEARTBEAT', '60')),
                blocked_connection_timeout=int(os.getenv('RABBITMQ_BLOCKED_CONNECTION_TIMEOUT', '30'))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid message broker configuration: {e}")

    def _init_market_data_config(self) -> MarketDataConfig:
        """Initialize market data configuration"""
        try:
            return MarketDataConfig(
                sync_interval=int(os.getenv('SYNC_INTERVAL', '300')),
                retry_interval=int(os.getenv('RETRY_INTERVAL', '60')),
                max_retries=int(os.getenv('MAX_RETRIES', '3')),
                default_timeframe=os.getenv('DEFAULT_TIMEFRAME', '5'),
                batch_size=int(os.getenv('MARKET_DATA_BATCH_SIZE', '1000'))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid market data configuration: {e}")

    def _init_fundamental_data_config(self) -> FundamentalDataConfig:
        """Initialize fundamental data configuration"""
        try:
            sentiment_config = SentimentConfig(
                twitter_api_key=os.getenv('TWITTER_RAPIDAPI_KEY'),
                twitter_api_host=os.getenv('TWITTER_RAPIDAPI_HOST'),
                twitter_rate_limit=int(os.getenv('TWITTER_RATE_LIMIT', '950')),
                twitter_rate_limit_window=int(os.getenv('TWITTER_RATE_LIMIT_WINDOW', '86400')),
                reddit_client_id=os.getenv('REDDIT_CLIENT_ID'),
                reddit_client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                reddit_rate_limit=int(os.getenv('REDDIT_RATE_LIMIT', '95')),
                reddit_rate_limit_window=int(os.getenv('REDDIT_RATE_LIMIT_WINDOW', '60')),
                telegram_api_id=int(os.getenv('TELEGRAM_API_ID', '0')) or None,
                telegram_api_hash=os.getenv('TELEGRAM_API_HASH'),
                telegram_session_name=os.getenv('TELEGRAM_SESSION_NAME', 'coinwatch_bot')
            )
            return FundamentalDataConfig(sentiment=sentiment_config)
        except Exception as e:
            raise ConfigurationError(f"Invalid fundamental data configuration: {e}")

    def _init_monitor_config(self) -> MonitorConfig:
        """Initialize monitoring configuration"""
        try:
            return MonitorConfig(
                check_intervals={
                    'system': int(os.getenv('MONITOR_CHECK_INTERVAL_SYSTEM', '30')),
                    'market': int(os.getenv('MONITOR_CHECK_INTERVAL_MARKET', '120')),
                    'database': int(os.getenv('MONITOR_CHECK_INTERVAL_DATABASE', '60'))
                }
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid monitoring configuration: {e}")

    def _init_api_config(self) -> APIConfig:
        """Initialize API configuration"""
        try:
            return APIConfig(
                port=int(os.getenv('API_PORT', '8000')),
                cors_origins=os.getenv('API_CORS_ORIGINS', '*').split(','),
                rate_limit_enabled=bool(os.getenv('API_RATE_LIMIT_ENABLED', 'true').lower() == 'true'),
                rate_limit_requests=int(os.getenv('API_RATE_LIMIT_REQUESTS', '100')),
                rate_limit_window=int(os.getenv('API_RATE_LIMIT_WINDOW', '60')),
                redis_url=os.getenv('REDIS_URL', 'redis://localhost:6379')
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid API configuration: {e}")
