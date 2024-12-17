import asyncio
import psutil
from typing import Dict, Any

from shared.messaging.broker import MessageBroker
import shared.utils.time as TimeUtils
from shared.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MetricsCollector:
    """
    Unified metrics collection and aggregation.

    Features:
    - Collects system metrics (CPU, memory, disk, network)
    - Maintains latest service metrics
    - Provides unified metrics format
    - Handles collection errors gracefully
    """

    def __init__(self):
        self._metrics: Dict[str, Dict[str, Any]] = {
            'system': {},
            'market_data': {},
            'fundamental_data': {},
            'database': {}
        }
        self._collection_lock = asyncio.Lock()

    def update_service_metrics(self, service: str, metrics: Dict[str, Any]) -> None:
        """Update metrics for a service"""
        if service in self._metrics:
            self._metrics[service].update(metrics)

    async def collect_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Collect all metrics including system resources"""
        async with self._collection_lock:
            # Update system metrics
            self._metrics['system'] = await self._collect_system_metrics()
            return self._metrics.copy()

    async def _collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system resource metrics"""
        try:
            cpu = psutil.cpu_percent(interval=1) / 100.0
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net = psutil.net_io_counters()

            return {
                'status': 'running',
                'timestamp': TimeUtils.get_current_timestamp(),
                'cpu_usage': cpu,
                'memory_usage': memory.percent / 100.0,
                'memory_available_gb': memory.available / (1024**3),
                'disk_usage': disk.percent / 100.0,
                'disk_free_gb': disk.free / (1024**3),
                'network_rx_mb': net.bytes_recv / (1024**2),
                'network_tx_mb': net.bytes_sent / (1024**2),
                'process_count': len(psutil.pids()),
                'error_count': 0
            }
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return {
                'status': 'error',
                'timestamp': TimeUtils.get_current_timestamp(),
                'error': str(e),
                'cpu_usage': 0,
                'memory_usage': 0,
                'memory_available_gb': 0,
                'disk_usage': 0,
                'disk_free_gb': 0,
                'network_rx_mb': 0,
                'network_tx_mb': 0,
                'process_count': 0,
                'error_count': 1
            }

    def get_service_metrics(self, service: str) -> Dict[str, Any]:
        """Get latest metrics for a service"""
        if service not in self._metrics:
            return {
                'status': 'unknown',
                'timestamp': TimeUtils.get_current_timestamp(),
                'error': 'Service not found',
                'error_count': 0
            }
        return self._metrics[service]

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all collected metrics"""
        return self._metrics.copy()

    def get_service_status(self) -> Dict[str, str]:
        """Get current status of all services"""
        return {
            service: metrics.get('status', 'unknown')
            for service, metrics in self._metrics.items()
        }

    def get_error_summary(self) -> Dict[str, int]:
        """Get error counts by service"""
        return {
            service: metrics.get('error_count', 0)
            for service, metrics in self._metrics.items()
        }

    def get_resource_usage(self) -> Dict[str, float]:
        """Get system resource usage summary"""
        system = self._metrics['system']
        return {
            'cpu': system.get('cpu_usage', 0) * 100,
            'memory': system.get('memory_usage', 0) * 100,
            'disk': system.get('disk_usage', 0) * 100
        }
