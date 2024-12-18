from enum import Enum
from pydantic import BaseModel


class MessageType(str, Enum):
    """Message types organized by domain"""
    # Market Data Domain
    SYMBOL_ADDED = "market.symbol.added"
    SYMBOL_DELISTED = "market.symbol.delisted"
    KLINE_UPDATED = "market.kline.updated"
    GAP_DETECTED = "market.gap.detected"
    COLLECTION_COMPLETE = "market.collection.complete"
    SYNC_COMPLETE = "market.sync.complete"  # For real-time sync completion

    # Fundamental Data Domain
    METADATA_UPDATED = "fundamental.metadata.updated"
    SENTIMENT_UPDATED = "fundamental.sentiment.updated"
    MARKET_METRICS_UPDATED = "fundamental.metrics.updated"

    # System Domain
    SERVICE_STATUS = "system.service.status"
    ERROR_REPORTED = "system.error.reported"
    SYSTEM_ALERT = "system.alert"

class BaseMessage(BaseModel):
    """Base message with common fields"""
    service: str  # Source service
    type: MessageType  # Message type
    timestamp: int  # Unix timestamp in milliseconds

class SymbolMessage(BaseMessage):
    """Symbol lifecycle events"""
    symbol: str
    exchange: str
    base_asset: str
    quote_asset: str
    launch_time: int | None = None

class KlineMessage(BaseMessage):
    """Candlestick/kline updates"""
    symbol: str
    exchange: str
    interval: str
    kline_timestamp: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    turnover: float

class GapMessage(BaseMessage):
    """Data gap detection"""
    symbol: str
    exchange: str
    interval: str
    gaps: list[tuple[int, int]]  # List of (start, end) timestamps

class CollectionMessage(BaseMessage):
    """Initial or gap fill collection"""
    symbol: str
    exchange: str
    interval: str
    start_time: int
    end_time: int
    processed: int  # Number of klines processed
    context: dict  # Additional sync info like next_sync, resource usage

class MetadataMessage(BaseMessage):
    """Token metadata updates"""
    symbol: str
    name: str
    description: str | None
    tags: list[str]
    platform: str
    contract_address: str | None

class SentimentMessage(BaseMessage):
    """Token sentiment updates"""
    symbol: str
    source: str  # e.g., "reddit", "twitter"
    score: float
    volume: int
    interval: str

class MarketMetricsMessage(BaseMessage):
    """Market metrics updates"""
    symbol: str
    market_cap: float
    volume_24h: float
    price_change_24h: float
    total_supply: float | None
    circulating_supply: float | None

class ServiceStatusMessage(BaseMessage):
    """Service health status"""
    status: str
    uptime: float
    error_count: int
    warning_count: int
    metrics: dict

class ErrorMessage(BaseMessage):
    """Error reporting"""
    error_type: str
    severity: str  # "warning", "error", "critical"
    message: str
    context: dict

class SystemAlertMessage(BaseMessage):
    """System-wide alerts"""
    alert_type: str  # e.g., "service_degraded", "high_error_rate"
    service_name: str
    message: str
    severity: str
    context: dict | None = None
