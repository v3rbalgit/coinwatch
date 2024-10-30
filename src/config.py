# src/config.py

from typing import Optional
from dataclasses import dataclass
import os
from dotenv import load_dotenv

from .domain_types import Timeframe
from .core.exceptions import ConfigurationError
from .utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 1800
    echo: bool = False

    @property
    def url(self) -> str:
        """Get database URL"""
        return f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class BybitConfig:
    """Bybit-specific configuration"""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    testnet: bool = False
    rate_limit: int = 600  # requests per window
    rate_limit_window: int = 300  # 5 minutes in seconds

@dataclass
class ExchangeConfig:
    """Exchange configuration"""
    bybit: BybitConfig

@dataclass
class MarketDataConfig:
    """Market data service configuration"""
    sync_interval: int = 300  # 5 minutes
    retry_interval: int = 60  # 1 minute
    max_retries: int = 3
    batch_size: int = 100
    default_timeframes: list[Timeframe] = [Timeframe.MINUTE_5]

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
        self.exchanges = self._init_exchange_config()
        self.market_data = self._init_market_data_config()
        self.logging = self._init_log_config()

    def _init_database_config(self) -> DatabaseConfig:
        """Initialize database configuration"""
        try:
            return DatabaseConfig(
                host=os.getenv('DB_HOST', 'db'),
                port=int(os.getenv('DB_PORT', '5432')),
                user=os.getenv('DB_USER', 'user'),
                password=os.getenv('DB_PASSWORD', 'password'),
                database=os.getenv('DB_NAME', 'coinwatch'),
                pool_size=int(os.getenv('DB_POOL_SIZE', '10')),
                max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '20')),
                pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30')),
                pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
                echo=bool(os.getenv('DB_ECHO', 'False').lower() == 'true')
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
                rate_limit=int(os.getenv('BYBIT_RATE_LIMIT', '600')),
                rate_limit_window=int(os.getenv('BYBIT_RATE_LIMIT_WINDOW', '300'))
            )
            return ExchangeConfig(bybit=bybit_config)
        except Exception as e:
            raise ConfigurationError(f"Invalid exchange configuration: {e}")

    def _init_market_data_config(self) -> MarketDataConfig:
        """Initialize market data configuration"""
        try:
            timeframes = [
                Timeframe(tf) for tf in
                os.getenv('DEFAULT_TIMEFRAMES', '5m').split(',')
            ]

            return MarketDataConfig(
                sync_interval=int(os.getenv('SYNC_INTERVAL', 300)),
                retry_interval=int(os.getenv('RETRY_INTERVAL', 60)),
                max_retries=int(os.getenv('MAX_RETRIES', 3)),
                batch_size=int(os.getenv('BATCH_SIZE', 100)),
                default_timeframes=timeframes
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid market data configuration: {e}")

    def _init_log_config(self) -> LogConfig:
        """Initialize logging configuration"""
        try:
            return LogConfig(
                level=os.getenv('LOG_LEVEL', 'INFO'),
                file_path=os.getenv('LOG_FILE', 'file_path'),
                max_size=int(os.getenv('LOG_MAX_SIZE', 10 * 1024 * 1024)),
                backup_count=int(os.getenv('LOG_BACKUP_COUNT', 5))
            )
        except Exception as e:
            raise ConfigurationError(f"Invalid logging configuration: {e}")