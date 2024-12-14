from enum import Enum
from typing import Any, Dict, Optional, List, Tuple
from pydantic import BaseModel

from shared.utils.domain_types import ServiceStatus

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
    first_trade_time: Optional[int] = None

class KlineMessage(BaseMessage):
    """Candlestick/kline updates"""
    symbol: str
    exchange: str
    timeframe: str
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
    timeframe: str
    gaps: List[Tuple[int, int]]  # List of (start, end) timestamps

class CollectionMessage(BaseMessage):
    """Initial or gap fill collection"""
    symbol: str
    exchange: str
    timeframe: str
    start_time: int
    end_time: int
    processed: int  # Number of klines processed
    context: Dict[str, Any]  # Additional sync info like next_sync, resource usage

class MetadataMessage(BaseMessage):
    """Token metadata updates"""
    symbol: str
    name: str
    description: Optional[str]
    tags: List[str]
    platform: str
    contract_address: Optional[str]

class SentimentMessage(BaseMessage):
    """Token sentiment updates"""
    symbol: str
    source: str  # e.g., "reddit", "twitter"
    score: float
    volume: int
    timeframe: str

class MarketMetricsMessage(BaseMessage):
    """Market metrics updates"""
    symbol: str
    market_cap: float
    volume_24h: float
    price_change_24h: float
    total_supply: Optional[float]
    circulating_supply: Optional[float]

class ServiceStatusMessage(BaseMessage):
    """Service health status"""
    status: ServiceStatus
    uptime: float
    error_count: int
    warning_count: int
    metrics: Dict

class ErrorMessage(BaseMessage):
    """Error reporting"""
    error_type: str
    severity: str  # "warning", "error", "critical"
    message: str
    context: Dict

class SystemAlertMessage(BaseMessage):
    """System-wide alerts"""
    alert_type: str  # e.g., "service_degraded", "high_error_rate"
    service_name: str
    message: str
    severity: str
    context: Optional[Dict] = None
