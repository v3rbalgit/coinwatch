import asyncio
from typing import Dict, Any, Optional
from prometheus_client import generate_latest

from shared.core.config import MonitorConfig
from shared.core.service import Service
from shared.messaging.broker import MessageBroker
from shared.messaging.schemas import MessageType, ServiceStatusMessage
import shared.utils.time as TimeUtils
from shared.utils.logger import LoggerSetup
from .metrics_collector import MetricsCollector
from .monitor_metrics import MonitoringMetrics

logger = LoggerSetup.setup(__name__)

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
                 message_broker: MessageBroker,
                 config: MonitorConfig):

        self.message_broker = message_broker
        self.metrics_collector = MetricsCollector()
        self.prometheus_metrics = MonitoringMetrics()
        self._check_intervals = config.check_intervals

        # Service state
        self._status = "stopped"
        self._start_time = TimeUtils.get_current_timestamp()
        self._collection_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start monitoring service"""
        try:
            self._status = "starting"
            logger.info("Starting monitoring service")

            # Connect to message broker
            await self.message_broker.connect()

            # Subscribe to service events
            await self._setup_message_handlers()

            # Start metrics collection
            self._running = True
            self._collection_task = asyncio.create_task(self._collect_metrics_loop())

            self._status = "running"
            logger.info("Monitoring service started successfully")

            # Publish initial service status
            await self.message_broker.publish(
                MessageType.SERVICE_STATUS,
                {
                    "service": "monitor",
                    "type": MessageType.SERVICE_STATUS,
                    "status": self._status,
                    "timestamp": TimeUtils.get_current_timestamp(),
                    "metrics": self.metrics_collector.get_service_metrics("system")
                }
            )

        except Exception as e:
            self._status = "error"
            logger.error(f"Failed to start monitoring service: {e}")
            raise

    async def stop(self) -> None:
        """Stop monitoring service"""
        try:
            self._status = "stopping"
            logger.info("Stopping monitoring service")

            # Stop metrics collection
            self._running = False
            if self._collection_task:
                self._collection_task.cancel()
                try:
                    await self._collection_task
                except asyncio.CancelledError:
                    pass

            # Close message broker connection
            await self.message_broker.close()

            self._status = "stopped"
            logger.info("Monitoring service stopped successfully")

        except Exception as e:
            self._status = "error"
            logger.error(f"Error stopping monitoring service: {e}")
            raise

    async def _setup_message_handlers(self) -> None:
        """Setup message handlers for monitoring events"""
        await self.message_broker.subscribe(
            MessageType.SERVICE_STATUS,
            self._handle_service_status
        )

        await self.message_broker.subscribe(
            MessageType.ERROR_REPORTED,
            self._handle_error
        )

    async def _handle_service_status(self, message: Dict[str, Any]) -> None:
        """Handle service status updates"""
        try:
            service = message["service"]
            self.metrics_collector.update_service_metrics(service, message)

            # Update Prometheus metrics
            status = message.get("status", "unknown")
            self.prometheus_metrics.service_up.labels(
                service=service,
                status=status
            ).set(1 if status == "running" else 0)

            # Update error metrics if present
            error_count = message.get("error_count", 0)
            warning_count = message.get("warning_count", 0)
            if error_count > 0 or warning_count > 0:
                self.prometheus_metrics.error_count.labels(
                    service=service,
                    severity="error"
                ).inc(error_count)
                self.prometheus_metrics.error_count.labels(
                    service=service,
                    severity="warning"
                ).inc(warning_count)

        except Exception as e:
            logger.error(f"Error handling service status: {e}")

    async def _handle_error(self, message: Dict[str, Any]) -> None:
        """Handle error reports"""
        try:
            service = message["service"]
            severity = message.get("severity", "error")

            # Update service metrics
            current = self.metrics_collector.get_service_metrics(service)
            current["error_count"] = current.get("error_count", 0) + 1
            current["last_error"] = message.get("message")
            self.metrics_collector.update_service_metrics(service, current)

            # Update Prometheus metrics
            self.prometheus_metrics.error_count.labels(
                service=service,
                severity=severity
            ).inc()

            # Log critical errors
            if severity == "critical":
                logger.critical(
                    f"Critical error in {service}: {message.get('message')}\n"
                    f"Context: {message.get('context')}"
                )

        except Exception as e:
            logger.error(f"Error handling error report: {e}")

    async def _collect_metrics_loop(self) -> None:
        """Continuously collect and update metrics"""
        while self._running:
            try:
                # Collect all metrics
                metrics = await self.metrics_collector.collect_metrics()

                # Update Prometheus system metrics
                system = metrics["system"]
                self.prometheus_metrics.system_cpu_usage.set(system["cpu_usage"] * 100)
                self.prometheus_metrics.system_memory_usage.set(system["memory_usage"] * 100)
                self.prometheus_metrics.system_disk_usage.set(system["disk_usage"] * 100)
                self.prometheus_metrics.system_disk_free.set(system["disk_free_gb"])
                self.prometheus_metrics.system_network_rx.set(system["network_rx_mb"])
                self.prometheus_metrics.system_network_tx.set(system["network_tx_mb"])
                self.prometheus_metrics.system_process_count.set(system["process_count"])

                # Sleep until next collection
                await asyncio.sleep(self._check_intervals.get("system", 60))

            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(5)  # Short delay on error

    async def get_prometheus_metrics(self) -> bytes:
        """Generate Prometheus metrics output"""
        return generate_latest(self.prometheus_metrics.registry)

    def get_service_status(self) -> str:
        """Get comprehensive monitoring service status"""
        status_lines = [
            "Monitoring Service Status:",
            f"Status: {self._status}",
            "",
            "Service Health:"
        ]

        # Add service status
        for service, status in self.metrics_collector.get_service_status().items():
            metrics = self.metrics_collector.get_service_metrics(service)
            last_update = metrics.get("timestamp", 0)
            time_ago = (TimeUtils.get_current_timestamp() - last_update) / 1000
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
