# src/services/monitor/metrics_collector.py

import asyncio
import psutil
from typing import Dict, Union

from ...core.monitoring import ServiceMetrics, DatabaseMetrics, MarketDataMetrics, SystemMetrics
from ...core.coordination import Command, MonitoringCommand, ServiceCoordinator
from ...utils.time import TimeUtils
from ...utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

MetricsType = Union[DatabaseMetrics, MarketDataMetrics, SystemMetrics]

class MetricsCollector:
    """Collects and aggregates metrics from all services"""

    def __init__(self, coordinator: ServiceCoordinator):
        self.coordinator = coordinator
        self._last_metrics: Dict[str, MetricsType] = {}
        self._collection_lock = asyncio.Lock()

    async def collect_metrics(self) -> Dict[str, MetricsType]:
        """Collect metrics from all services"""
        async with self._collection_lock:
            # Request metrics from each service
            db_metrics = await self._request_service_metrics("database")
            market_metrics = await self._request_service_metrics("market_data")
            system_metrics = await self.collect_system_metrics()

            metrics = {
                "database": db_metrics,
                "market_data": market_metrics,
                "system": system_metrics
            }

            self._last_metrics = metrics
            return metrics

    async def _request_service_metrics(self, service: str) -> ServiceMetrics:
        """Request metrics from a specific service"""
        try:
            result = await self.coordinator.execute(Command(
                type=MonitoringCommand.REPORT_METRICS,
                params={
                    "service": service,
                    "timestamp": TimeUtils.get_current_timestamp()
                },
                expects_response=True
            ))

            if result and result.is_success:
                # The handler should return the correct type
                return result.data
            else:
                error_msg = result.error_message if result else "No response"
                raise Exception(f"Failed to collect metrics: {error_msg}")

        except Exception as e:
            # Return error metrics of appropriate type
            if service == "database":
                return DatabaseMetrics(
                    service_name=service,
                    status="error",
                    uptime_seconds=0,
                    last_error=str(e),
                    error_count=1,
                    warning_count=0,
                    timestamp=TimeUtils.get_current_datetime(),
                    additional_metrics={},
                    active_connections=0,
                    pool_size=0,
                    max_overflow=0,
                    available_connections=0,
                    deadlocks=0,
                    long_queries=0,
                    maintenance_due=False,
                    replication_lag_seconds=0
                )
            elif service == "market_data":
                return MarketDataMetrics(
                    service_name=service,
                    status="error",
                    uptime_seconds=0,
                    last_error=str(e),
                    error_count=1,
                    warning_count=0,
                    timestamp=TimeUtils.get_current_datetime(),
                    additional_metrics={},
                    active_symbols=0,
                    active_collections=0,
                    pending_collections=0,
                    active_syncs=0,
                    sync_errors=0,
                    collection_errors=0,
                    batch_size=0,
                    data_gaps=0
                )
            else:
                # Generic service metrics for unknown services
                return ServiceMetrics(
                    service_name=service,
                    status="error",
                    uptime_seconds=0,
                    last_error=str(e),
                    error_count=1,
                    warning_count=0,
                    timestamp=TimeUtils.get_current_datetime(),
                    additional_metrics={}
                )

    async def collect_system_metrics(self) -> SystemMetrics:
        """Collect system resource metrics"""
        try:
            cpu = psutil.cpu_percent(interval=1) / 100.0
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net = psutil.net_io_counters()

            return SystemMetrics(
                cpu_usage=cpu,
                memory_usage=memory.percent / 100.0,
                disk_usage=disk.percent / 100.0,
                disk_free_gb=disk.free / (1024**3),
                network_rx_bytes=net.bytes_recv,
                network_tx_bytes=net.bytes_sent,
                process_count=len(psutil.pids()),
                timestamp=TimeUtils.get_current_datetime()
            )
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return SystemMetrics(
                cpu_usage=0,
                memory_usage=0,
                disk_usage=0,
                disk_free_gb=0,
                network_rx_bytes=0,
                network_tx_bytes=0,
                process_count=0,
                timestamp=TimeUtils.get_current_datetime()
            )