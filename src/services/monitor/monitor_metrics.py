# src/services/monitor/monitor_metrics.py

from typing import Optional
from prometheus_client import Counter, Gauge, CollectorRegistry

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

        self.service_uptime = Gauge(
            'service_uptime_seconds',
            'Service uptime in seconds',
            ['service'],
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
        self.system_disk_free = Gauge(
            'system_disk_free_gb',
            'Free disk space in gigabytes',
            registry=self.registry
        )
        self.system_network_rx = Gauge(
            'system_network_rx_bytes',
            'Network bytes received',
            registry=self.registry
        )
        self.system_network_tx = Gauge(
            'system_network_tx_bytes',
            'Network bytes transmitted',
            registry=self.registry
        )
        self.system_process_count = Gauge(
            'system_process_count',
            'Number of running processes',
            registry=self.registry
        )

        # Market data metrics
        self.market_active_symbols = Gauge(
            'market_active_symbols',
            'Number of active trading symbols',
            registry=self.registry
        )
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
        self.market_collection_errors = Gauge(
            'market_collection_errors',
            'Number of collection errors',
            registry=self.registry
        )
        self.market_sync_errors = Gauge(
            'market_sync_errors',
            'Number of synchronization errors',
            registry=self.registry
        )
        self.market_data_gaps = Gauge(
            'market_data_gaps',
            'Number of detected data gaps',
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
        self.db_deadlocks = Gauge(
            'db_deadlocks',
            'Number of detected deadlocks',
            registry=self.registry
        )
        self.db_long_queries = Gauge(
            'db_long_queries',
            'Number of long-running queries',
            registry=self.registry
        )
        self.db_maintenance_due = Gauge(
            'db_maintenance_due',
            'Database maintenance status (1 if due)',
            registry=self.registry
        )
        self.db_replication_lag = Gauge(
            'db_replication_lag_seconds',
            'Database replication lag in seconds',
            registry=self.registry
        )

        self.error_count = Counter(
            'monitoring_error_count_total',
            'Total number of errors by component',
            ['component', 'severity'],
            registry=self.registry
        )