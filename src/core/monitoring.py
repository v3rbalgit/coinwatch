# src/core/monitoring.py

from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime

@dataclass
class ServiceMetrics:
    """Base metrics structure for all services"""
    service_name: str
    status: str
    uptime_seconds: float
    last_error: Optional[str]
    error_count: int
    warning_count: int
    timestamp: datetime
    additional_metrics: Dict[str, Any]

@dataclass
class DatabaseMetrics(ServiceMetrics):
    """Database-specific metrics"""
    active_connections: int
    pool_size: int
    max_overflow: int
    available_connections: int
    deadlocks: int
    long_queries: int
    maintenance_due: bool
    replication_lag_seconds: int

@dataclass
class MarketDataMetrics(ServiceMetrics):
    """Market data service metrics"""
    active_symbols: int
    active_collections: int
    pending_collections: int
    active_syncs: int
    sync_errors: int
    collection_errors: int
    batch_size: int
    data_gaps: int

@dataclass
class SystemMetrics:
    """System resource metrics"""
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    disk_free_gb: float
    network_rx_bytes: int
    network_tx_bytes: int
    process_count: int
    timestamp: datetime