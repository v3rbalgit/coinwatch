# src/services/monitor/monitor_metrics.py

from typing import Optional
from prometheus_client import Counter, Gauge, Info, Summary, CollectorRegistry

class MonitoringMetrics:
    """Prometheus metrics for monitoring service"""
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()

        # Service-level metrics
        self.service_up = Gauge(
            'monitoring_service_up',
            'Current status of the monitoring service',
            ['status'],
            registry=self.registry
        )

        # System metrics
        self.system_memory_usage = Gauge(
            'system_memory_usage_percent',
            'System memory usage percentage',
            registry=self.registry
        )
        self.system_cpu_usage = Gauge(
            'system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self.registry
        )
        self.system_disk_usage = Gauge(
            'system_disk_usage_percent',
            'System disk usage percentage',
            registry=self.registry
        )

        # Market data metrics
        self.market_active_collections = Gauge(
            'market_active_collections',
            'Number of active data collections',
            registry=self.registry
        )
        self.market_pending_collections = Gauge(
            'market_pending_collections',
            'Number of pending collections',
            registry=self.registry
        )
        self.market_active_syncs = Gauge(
            'market_active_syncs',
            'Number of active synchronizations',
            registry=self.registry
        )
        self.market_batch_size = Gauge(
            'market_batch_size',
            'Current batch size for collections',
            registry=self.registry
        )

        # Database metrics
        self.db_active_connections = Gauge(
            'db_active_connections',
            'Number of active database connections',
            registry=self.registry
        )
        self.db_pool_usage = Gauge(
            'db_pool_usage_percent',
            'Database connection pool usage percentage',
            registry=self.registry
        )

        # Cache metrics
        self.cache_hit_ratio = Gauge(
            'cache_hit_ratio',
            'Cache hit ratio',
            ['cache_type'],
            registry=self.registry
        )
        self.cache_size = Gauge(
            'cache_size',
            'Current cache size',
            ['cache_type'],
            registry=self.registry
        )
        self.cache_memory_usage = Gauge(
            'cache_memory_usage_bytes',
            'Cache memory usage in bytes',
            ['cache_type'],
            registry=self.registry
        )

        # Error tracking
        self.error_count = Counter(
            'monitoring_error_count_total',
            'Total number of errors by component',
            ['component', 'severity'],
            registry=self.registry
        )

        # Performance metrics
        self.collection_duration = Summary(
            'market_collection_duration_seconds',
            'Time spent collecting market data',
            registry=self.registry
        )
        self.sync_duration = Summary(
            'market_sync_duration_seconds',
            'Time spent synchronizing market data',
            registry=self.registry
        )

        # Resource pressure events
        self.resource_pressure_events = Counter(
            'resource_pressure_events_total',
            'Number of resource pressure events',
            ['resource_type', 'severity'],
            registry=self.registry
        )

        # Service info
        self.service_info = Info(
            'monitoring_service',
            'Monitoring service information',
            registry=self.registry
        )