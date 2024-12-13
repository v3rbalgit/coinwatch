from dataclasses import dataclass
from prometheus_client import Counter, Gauge, CollectorRegistry

@dataclass
class MonitoringMetrics:
    """
    Prometheus metrics definitions for monitoring service.

    Metrics:
    - Service status (up/down)
    - Error counts by service and severity
    - System resource usage (CPU, memory, disk, network)
    - Service-specific metrics
    """

    def __init__(self):
        self.registry = CollectorRegistry()

        # Service health metrics
        self.service_up = Gauge(
            'service_up',
            'Service operational status (1 = up, 0 = down)',
            ['service', 'status'],
            registry=self.registry
        )

        # Error metrics
        self.error_count = Counter(
            'error_count_total',
            'Total number of errors by service and severity',
            ['service', 'severity'],
            registry=self.registry
        )

        # System metrics
        self.system_cpu_usage = Gauge(
            'system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self.registry
        )

        self.system_memory_usage = Gauge(
            'system_memory_usage_percent',
            'System memory usage percentage',
            registry=self.registry
        )

        self.system_disk_usage = Gauge(
            'system_disk_usage_percent',
            'System disk usage percentage',
            registry=self.registry
        )

        self.system_disk_free = Gauge(
            'system_disk_free_gb',
            'System free disk space in gigabytes',
            registry=self.registry
        )

        self.system_network_rx = Gauge(
            'system_network_rx_mb',
            'Network bytes received in megabytes',
            registry=self.registry
        )

        self.system_network_tx = Gauge(
            'system_network_tx_mb',
            'Network bytes transmitted in megabytes',
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
            'Number of pending data collections',
            registry=self.registry
        )

        self.market_active_syncs = Gauge(
            'market_active_syncs',
            'Number of active data synchronizations',
            registry=self.registry
        )

        self.market_collection_errors = Gauge(
            'market_collection_errors',
            'Number of data collection errors',
            registry=self.registry
        )

        self.market_sync_errors = Gauge(
            'market_sync_errors',
            'Number of data synchronization errors',
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

        self.db_deadlocks = Counter(
            'db_deadlocks_total',
            'Total number of database deadlocks',
            registry=self.registry
        )

        self.db_long_queries = Counter(
            'db_long_queries_total',
            'Total number of long-running queries',
            registry=self.registry
        )

        self.db_replication_lag = Gauge(
            'db_replication_lag_seconds',
            'Database replication lag in seconds',
            registry=self.registry
        )

    def reset(self) -> None:
        """Reset all metrics to their initial state"""
        # Reset gauges to 0
        self.system_cpu_usage.set(0)
        self.system_memory_usage.set(0)
        self.system_disk_usage.set(0)
        self.system_disk_free.set(0)
        self.system_network_rx.set(0)
        self.system_network_tx.set(0)
        self.system_process_count.set(0)

        self.market_active_symbols.set(0)
        self.market_active_collections.set(0)
        self.market_pending_collections.set(0)
        self.market_active_syncs.set(0)
        self.market_collection_errors.set(0)
        self.market_sync_errors.set(0)
        self.market_data_gaps.set(0)

        self.db_active_connections.set(0)
        self.db_pool_usage.set(0)
        self.db_replication_lag.set(0)

        # Note: Counters cannot be reset as they are cumulative
