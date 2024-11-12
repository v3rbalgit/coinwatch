# src/services/monitor/service.py

import asyncio
from typing import Dict, Optional, cast
from prometheus_client import generate_latest

from ...core.monitoring import DatabaseMetrics, MarketDataMetrics, SystemMetrics
from .metrics_collector import MetricsCollector, MetricsType
from ...config import MonitoringConfig
from ...core.coordination import ServiceCoordinator
from ..base import ServiceBase
from ...utils.time import TimeUtils
from ...utils.logger import LoggerSetup
from ...utils.domain_types import ServiceStatus
from .monitor_metrics import MonitoringMetrics

logger = LoggerSetup.setup(__name__)

class MonitoringService(ServiceBase):
    """
    Centralized monitoring service that collects and aggregates metrics from all system components.

    Provides real-time monitoring of:
    - System resources (CPU, memory, disk, network)
    - Database performance metrics
    - Market data service metrics

    All metrics are exposed through Prometheus for external monitoring and alerting.
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 config: MonitoringConfig):
        super().__init__(config)

        self.coordinator = coordinator
        self.metrics = MonitoringMetrics()
        self.collector = MetricsCollector(coordinator)

        # Start collection tasks
        self._collection_task: Optional[asyncio.Task] = None
        self._last_collection: Dict[str, int] = {
            'system': 0,
            'market': 0,
            'database': 0
        }

    async def start(self) -> None:
        """
        Start the monitoring service and begin metrics collection.

        Initializes metrics collectors and starts background tasks for periodic collection.
        """
        try:
            self._status = ServiceStatus.STARTING
            logger.info("Starting monitoring service")

            # Start metrics collection
            self._collection_task = asyncio.create_task(self._collect_metrics_loop())

            self._status = ServiceStatus.RUNNING
            logger.info("Monitoring service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            logger.error(f"Failed to start monitoring service: {e}")
            raise

    async def stop(self) -> None:
        """
        Stop the monitoring service and cleanup resources.

        Cancels all background collection tasks and ensures proper shutdown.
        """
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass

    async def _collect_metrics_loop(self) -> None:
        """
        Continuously collect metrics from all services at configured intervals.

        Collection frequencies are customized per metric type:
        - System metrics: 30s
        - Database metrics: 60s
        - Market data metrics: 120s
        """
        while True:
            try:
                current_time = TimeUtils.get_current_timestamp()
                collected_metrics: Dict[str, MetricsType] = {}

                # Check and collect system metrics
                if (current_time - self._last_collection['system'] >=
                    self._config.check_intervals['system'] * 1000):
                    collected_metrics['system'] = await self.collector.collect_system_metrics()
                    self._last_collection['system'] = current_time

                # Check and collect database metrics
                if (current_time - self._last_collection['database'] >=
                    self._config.check_intervals['database'] * 1000):
                    db_metrics = await self.collector._request_service_metrics("database")
                    if db_metrics:
                        collected_metrics['database'] = cast(DatabaseMetrics, db_metrics)
                        self._last_collection['database'] = current_time

                # Check and collect market data metrics
                if (current_time - self._last_collection['market'] >=
                    self._config.check_intervals['market'] * 1000):
                    market_metrics = await self.collector._request_service_metrics("market_data")
                    if market_metrics:
                        collected_metrics['market_data'] = cast(MarketDataMetrics, market_metrics)
                        self._last_collection['market'] = current_time

                # Update any metrics that were collected
                if collected_metrics:
                    self._update_prometheus_metrics(collected_metrics)

                # Sleep for the shortest interval
                min_interval = min(self._config.check_intervals.values())
                await asyncio.sleep(min_interval)

            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(5)

    def _update_prometheus_metrics(self, metrics: Dict[str, MetricsType]) -> None:
        """
        Update Prometheus metrics with latest collected data.

        Args:
            metrics: Dictionary of metrics by service name
        """
        try:
            # Update service status
            self.metrics.service_up.labels(
                status=self._status.value
            ).set(1 if self._status == ServiceStatus.RUNNING else 0)

            # Database metrics
            if db_metrics := metrics.get("database"):
                db_metrics = cast(DatabaseMetrics, db_metrics)
                self.metrics.db_active_connections.set(db_metrics.active_connections)
                self.metrics.db_pool_usage.set(
                    (db_metrics.active_connections / db_metrics.pool_size) * 100
                )
                self.metrics.db_deadlocks.set(db_metrics.deadlocks)
                self.metrics.db_long_queries.set(db_metrics.long_queries)
                self.metrics.db_maintenance_due.set(1 if db_metrics.maintenance_due else 0)
                self.metrics.db_replication_lag.set(db_metrics.replication_lag_seconds)
                self.metrics.service_uptime.labels(service="database").set(db_metrics.uptime_seconds)

                # Error counts
                self.metrics.error_count.labels(
                    component="database",
                    severity="error"
                ).inc(db_metrics.error_count)
                self.metrics.error_count.labels(
                    component="database",
                    severity="warning"
                ).inc(db_metrics.warning_count)

            # Market data metrics
            if market_metrics := metrics.get("market_data"):
                market_metrics = cast(MarketDataMetrics, market_metrics)
                self.metrics.market_active_symbols.set(market_metrics.active_symbols)
                self.metrics.market_active_collections.set(market_metrics.active_collections)
                self.metrics.market_pending_collections.set(market_metrics.pending_collections)
                self.metrics.market_active_syncs.set(market_metrics.active_syncs)
                self.metrics.market_batch_size.set(market_metrics.batch_size)
                self.metrics.market_collection_errors.set(market_metrics.collection_errors)
                self.metrics.market_sync_errors.set(market_metrics.sync_errors)
                self.metrics.market_data_gaps.set(market_metrics.data_gaps)
                self.metrics.service_uptime.labels(service="market_data").set(market_metrics.uptime_seconds)

                # Error counts
                self.metrics.error_count.labels(
                    component="market_data",
                    severity="error"
                ).inc(market_metrics.error_count)
                self.metrics.error_count.labels(
                    component="market_data",
                    severity="warning"
                ).inc(market_metrics.warning_count)

            # System metrics
            if system_metrics := metrics.get("system"):
                system_metrics = cast(SystemMetrics, system_metrics)
                self.metrics.system_cpu_usage.set(system_metrics.cpu_usage * 100)
                self.metrics.system_memory_usage.set(system_metrics.memory_usage * 100)
                self.metrics.system_disk_usage.set(system_metrics.disk_usage * 100)
                self.metrics.system_disk_free.set(system_metrics.disk_free_gb)
                self.metrics.system_network_rx.set(system_metrics.network_rx_bytes)
                self.metrics.system_network_tx.set(system_metrics.network_tx_bytes)
                self.metrics.system_process_count.set(system_metrics.process_count)

        except Exception as e:
            logger.error(f"Error updating Prometheus metrics: {e}")

    async def get_prometheus_metrics(self) -> bytes:
        """
        Generate Prometheus metrics output.

        Returns:
            bytes: Prometheus-formatted metrics data
        """
        return generate_latest(self.metrics.registry)

    def get_service_status(self) -> str:
        """
        Get comprehensive monitoring service status report.

        Returns:
            str: Multi-line status report including:
            - Service state
            - Collection intervals
            - Last collection times
            - Resource metrics
            - Component health status
        """
        metrics = {}  # Will store last collected metrics
        try:
            metrics = self.collector._last_metrics
        except Exception:
            pass

        status_lines = [
            "Monitoring Service Status:",
            f"Status: {self._status.value}",
            "",
            "Collection Intervals:",
            *[f"  {service}: {interval}s"
              for service, interval in self._config.check_intervals.items()],
            "",
            "Last Collection Times:"
        ]

        # Add last collection times
        for service, timestamp in self._last_collection.items():
            if timestamp > 0:
                time_ago = (TimeUtils.get_current_timestamp() - timestamp) / 1000
                status_lines.append(f"  {service}: {time_ago:.1f}s ago")

        if "database" in metrics:
            db_metrics = cast(DatabaseMetrics, metrics["database"])
            status_lines.extend([
                "Database Service:",
                f"  Status: {db_metrics.status}",
                f"  Active Connections: {db_metrics.active_connections}/{db_metrics.pool_size}",
                f"  Deadlocks: {db_metrics.deadlocks}",
                f"  Long Queries: {db_metrics.long_queries}"
            ])

        if "market_data" in metrics:
            market_metrics = cast(MarketDataMetrics, metrics["market_data"])
            status_lines.extend([
                "Market Data Service:",
                f"  Status: {market_metrics.status}",
                f"  Active Symbols: {market_metrics.active_symbols}",
                f"  Active Collections: {market_metrics.active_collections}",
                f"  Active Syncs: {market_metrics.active_syncs}",
                f"  Pending Collections: {market_metrics.pending_collections}",
                f"  Collection Errors: {market_metrics.collection_errors}",
                f"  Sync Errors: {market_metrics.sync_errors}"
            ])

        if "system" in metrics:
            sys_metrics = cast(SystemMetrics, metrics["system"])
            status_lines.extend([
                "System Resources:",
                f"  CPU Usage: {sys_metrics.cpu_usage:.1%}",
                f"  Memory Usage: {sys_metrics.memory_usage:.1%}",
                f"  Disk Usage: {sys_metrics.disk_usage:.1%}",
                f"  Free Disk: {sys_metrics.disk_free_gb:.1f}GB",
                f"  Network RX: {sys_metrics.network_rx_bytes / 1024 / 1024:.1f}MB",
                f"  Network TX: {sys_metrics.network_tx_bytes / 1024 / 1024:.1f}MB",
                f"  Process Count: {sys_metrics.process_count}"
            ])

        return "\n".join(status_lines)