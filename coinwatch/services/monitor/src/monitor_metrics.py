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

        # Service performance metrics
        self.service_response_time = Gauge(
            'service_response_time_seconds',
            'Service endpoint response time in seconds',
            ['service', 'endpoint'],
            registry=self.registry
        )

        self.service_request_rate = Counter(
            'service_request_total',
            'Total number of requests by service and endpoint',
            ['service', 'endpoint'],
            registry=self.registry
        )

        self.service_error_rate = Counter(
            'service_error_total',
            'Total number of errors by service, endpoint, and type',
            ['service', 'endpoint', 'error_type'],
            registry=self.registry
        )

        # Market data metrics
        self.market_uptime = Gauge(
            'market_uptime_seconds',
            'Market data service uptime in seconds',
            registry=self.registry
        )

        self.market_batch_size = Gauge(
            'market_batch_size',
            'Market data collection batch size',
            registry=self.registry
        )

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

        self.market_streaming_symbols = Gauge(
            'market_streaming_symbols',
            'Number of symbols with active streaming',
            registry=self.registry
        )

        self.market_collection_progress = Gauge(
            'market_collection_progress',
            'Collection progress percentage by symbol',
            ['symbol'],
            registry=self.registry
        )

        self.market_processed_candles = Gauge(
            'market_processed_candles',
            'Number of processed candles by symbol',
            ['symbol'],
            registry=self.registry
        )

        self.market_total_candles = Gauge(
            'market_total_candles',
            'Total number of candles to process by symbol',
            ['symbol'],
            registry=self.registry
        )

        self.market_errors = Counter(
            'market_errors_total',
            'Total number of errors by type',
            ['type'],
            registry=self.registry
        )

        # Fundamental data metrics
        self.fundamental_active_tokens = Gauge(
            'fundamental_active_tokens',
            'Total number of tokens being tracked',
            registry=self.registry
        )

        self.fundamental_collector_status = Gauge(
            'fundamental_collector_status',
            'Collector running status (1 = running, 0 = stopped)',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_collector_tokens = Gauge(
            'fundamental_collector_tokens',
            'Number of active tokens per collector',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_collector_interval = Gauge(
            'fundamental_collector_interval_seconds',
            'Collection interval in seconds per collector',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_collector_progress = Gauge(
            'fundamental_collector_progress_percent',
            'Current collection progress percentage',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_collector_processed = Gauge(
            'fundamental_collector_processed_tokens',
            'Number of processed tokens in current collection',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_collector_total = Gauge(
            'fundamental_collector_total_tokens',
            'Total number of tokens to process in current collection',
            ['collector'],
            registry=self.registry
        )

        self.fundamental_errors = Counter(
            'fundamental_errors_total',
            'Total number of errors by type',
            ['type'],
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

        # Reset market metrics
        self.market_uptime.set(0)
        self.market_batch_size.set(0)
        self.market_active_symbols.set(0)
        self.market_active_collections.set(0)
        self.market_streaming_symbols.set(0)
        self.market_collection_progress._metrics.clear()
        self.market_processed_candles._metrics.clear()
        self.market_total_candles._metrics.clear()

        # Reset fundamental metrics
        self.fundamental_active_tokens.set(0)
        self.fundamental_collector_status._metrics.clear()
        self.fundamental_collector_tokens._metrics.clear()
        self.fundamental_collector_interval._metrics.clear()
        self.fundamental_collector_progress._metrics.clear()
        self.fundamental_collector_processed._metrics.clear()
        self.fundamental_collector_total._metrics.clear()

        self.db_active_connections.set(0)
        self.db_pool_usage.set(0)
        self.db_replication_lag.set(0)

        # Note: Counters cannot be reset as they are cumulative
