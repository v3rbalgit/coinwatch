import asyncio
from prometheus_client import generate_latest

from shared.core.config import MonitorConfig
from shared.core.enums import ServiceStatus
from shared.core.protocols import Service
from shared.utils.logger import LoggerSetup
from .metrics_collector import MetricsCollector
from .monitor_metrics import MonitoringMetrics
from shared.utils.time import get_current_timestamp



class MonitoringService(Service):
    """
    Centralized monitoring service that collects and aggregates metrics from all system components.

    Features:
    - System resources monitoring (CPU, memory, disk, network)
    - Service health tracking
    - Error aggregation and alerting
    - Prometheus metrics exposure
    """

    def __init__(self,
                 config: MonitorConfig):

        self.metrics_collector = MetricsCollector()
        self.prometheus_metrics = MonitoringMetrics()
        self._check_intervals = config.check_intervals

        # Service state
        self._status = ServiceStatus.STOPPED
        self._start_time = get_current_timestamp()
        self._collection_task: asyncio.Task | None = None
        self._running = False

        self.logger = LoggerSetup.setup(__class__.__name__)


    async def start(self) -> None:
        """Start monitoring service"""
        try:
            self._status = ServiceStatus.STARTING
            self.logger.info("Starting monitoring service")

            # Start metrics collection
            self._running = True
            self._collection_task = asyncio.create_task(self._collect_metrics_loop())

            self._status = ServiceStatus.RUNNING
            self.logger.info("Monitoring service started successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self.logger.error(f"Failed to start monitoring service: {e}")
            raise


    async def stop(self) -> None:
        """Stop monitoring service"""
        try:
            self._status = ServiceStatus.STOPPING
            self.logger.info("Stopping monitoring service")

            # Clean up collector
            await self.metrics_collector.cleanup()

            # Stop metrics collection
            self._running = False
            if self._collection_task:
                self._collection_task.cancel()
                try:
                    await self._collection_task
                except asyncio.CancelledError:
                    pass

            self._status = ServiceStatus.STOPPED
            self.logger.info("Monitoring service stopped successfully")

        except Exception as e:
            self._status = ServiceStatus.ERROR
            self.logger.error(f"Error stopping monitoring service: {e}")
            raise


    async def _collect_service_metrics(self, service: str) -> None:
        """Collect service-specific metrics"""
        try:
            if service == "market_data":
                await self._collect_market_metrics()
            elif service == "fundamental_data":
                await self._collect_fundamental_metrics()
        except Exception as e:
            self.logger.error(f"Error collecting {service} metrics: {e}")


    async def _collect_market_metrics(self) -> None:
        """Collect market data service metrics"""
        metrics = self.metrics_collector.get_service_metrics("market_data")

        # Update service metrics
        service_metrics = metrics.get("service", {})
        self.prometheus_metrics.market_uptime.set(service_metrics.get("uptime_seconds", 0))
        self.prometheus_metrics.market_batch_size.set(service_metrics.get("batch_size", 0))

        # Update collection metrics
        collection_metrics = metrics.get("collection", {})
        self.prometheus_metrics.market_active_symbols.set(collection_metrics.get("active_symbols", 0))
        self.prometheus_metrics.market_active_collections.set(collection_metrics.get("active_collections", 0))
        self.prometheus_metrics.market_streaming_symbols.set(collection_metrics.get("streaming_symbols", 0))

        # Update collection progress
        for symbol, progress in collection_metrics.get("progress", {}).items():
            self.prometheus_metrics.market_collection_progress.labels(symbol=symbol).set(progress.get("percentage", 0))
            self.prometheus_metrics.market_processed_candles.labels(symbol=symbol).set(progress.get("processed_candles", 0))
            self.prometheus_metrics.market_total_candles.labels(symbol=symbol).set(progress.get("total_candles", 0))

        # Update error metrics
        error_metrics = metrics.get("errors", {})
        for error_type, count in error_metrics.get("by_type", {}).items():
            self.prometheus_metrics.market_errors.labels(type=error_type).inc(count)

    async def _collect_fundamental_metrics(self) -> None:
        """Collect fundamental data service metrics"""
        metrics = self.metrics_collector.get_service_metrics("fundamental_data")

        # Update active tokens
        self.prometheus_metrics.fundamental_active_tokens.set(
            metrics.get("active_tokens", 0)
        )

        # Update collector metrics
        for collector_name, data in metrics.get("collectors", {}).items():
            # Update collector status
            self.prometheus_metrics.fundamental_collector_status.labels(
                collector=collector_name
            ).set(1 if data.get("running", False) else 0)

            # Update collector tokens
            self.prometheus_metrics.fundamental_collector_tokens.labels(
                collector=collector_name
            ).set(data.get("active_tokens", 0))

            # Update collection interval
            self.prometheus_metrics.fundamental_collector_interval.labels(
                collector=collector_name
            ).set(data.get("collection_interval", 0))

            # Update collection progress if available
            if progress := data.get("current_progress"):
                self.prometheus_metrics.fundamental_collector_progress.labels(
                    collector=collector_name
                ).set(progress.get("processed_tokens", 0) / progress.get("total_tokens", 1) * 100)

                self.prometheus_metrics.fundamental_collector_processed.labels(
                    collector=collector_name
                ).set(progress.get("processed_tokens", 0))

                self.prometheus_metrics.fundamental_collector_total.labels(
                    collector=collector_name
                ).set(progress.get("total_tokens", 0))

        # Update error metrics
        for error_type, count in metrics.get("errors", {}).get("by_type", {}).items():
            self.prometheus_metrics.fundamental_errors.labels(
                type=error_type
            ).inc(count)


    async def _collect_metrics_loop(self) -> None:
        """Continuously collect and update metrics"""
        while self._running:
            try:
                # Collect system metrics
                metrics = await self.metrics_collector.collect_metrics()
                system = metrics["system"]

                # Update system metrics
                self.prometheus_metrics.system_cpu_usage.set(system["cpu_usage"] * 100)
                self.prometheus_metrics.system_memory_usage.set(system["memory_usage"] * 100)
                self.prometheus_metrics.system_disk_usage.set(system["disk_usage"] * 100)
                self.prometheus_metrics.system_disk_free.set(system["disk_free_gb"])
                self.prometheus_metrics.system_network_rx.set(system["network_rx_mb"])
                self.prometheus_metrics.system_network_tx.set(system["network_tx_mb"])
                self.prometheus_metrics.system_process_count.set(system["process_count"])

                # Collect service-specific metrics
                await self._collect_service_metrics("market_data")
                await self._collect_service_metrics("fundamental_data")

                # Sleep until next collection
                await asyncio.sleep(self._check_intervals.get("metrics", 30))

            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(5)  # Short delay on error


    async def get_prometheus_metrics(self) -> bytes:
        """Generate Prometheus metrics output"""
        return generate_latest(self.prometheus_metrics.registry)


    def get_service_status(self) -> str:
        """
        Generate detailed service status report.

        Returns:
            str: Multi-line status report including:
                - Service state
                - Resource usage
                - Error counts
                - Component health
        """
        status_lines = [
            "Monitoring Service Status:",
            f"Status: {self._status.value}",
            "",
            "Service Health:"
        ]

        # Add service status
        for service, status in self.metrics_collector.get_service_status().items():
            metrics = self.metrics_collector.get_service_metrics(service)
            last_update = metrics.get("timestamp", 0)
            time_ago = (get_current_timestamp() - last_update) / 1000
            status_lines.append(
                f"  {service}: {status} "
                f"(last update: {time_ago:.1f}s ago)"
            )

        # Add resource usage
        usage = self.metrics_collector.get_resource_usage()
        status_lines.extend([
            "",
            "System Resources:",
            f"  CPU Usage: {usage['cpu']:.1f}%",
            f"  Memory Usage: {usage['memory']:.1f}%",
            f"  Disk Usage: {usage['disk']:.1f}%"
        ])

        # Add error summary
        errors = self.metrics_collector.get_error_summary()
        if any(errors.values()):
            status_lines.extend([
                "",
                "Error Summary:"
            ])
            for service, count in errors.items():
                if count > 0:
                    status_lines.append(f"  {service}: {count} errors")

        return "\n".join(status_lines)
