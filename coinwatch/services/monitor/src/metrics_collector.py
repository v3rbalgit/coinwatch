import asyncio
import psutil
import aiohttp

from shared.utils.logger import LoggerSetup
from shared.utils.time import get_current_timestamp
from shared.core.enums import ServiceStatus



class MetricsCollector:
    """
    Unified metrics collection and aggregation.

    Features:
    - Collects system metrics (CPU, memory, disk, network)
    - Collects service metrics from HTTP endpoints
    - Maintains latest service metrics
    - Provides unified metrics format
    - Handles collection errors gracefully
    """

    def __init__(self):
        self._metrics: dict[str, dict] = {
            'system': {},
            'market_data': {},
            'fundamental_data': {},
            'database': {}
        }
        self._collection_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self.logger = LoggerSetup.setup(__class__.__name__)


    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session


    def update_service_metrics(self, service: str, metrics: dict) -> None:
        """Update metrics for a service"""
        if service in self._metrics:
            self._metrics[service].update(metrics)


    async def collect_metrics(self) -> dict[str, dict]:
        """Collect all metrics including system resources and service metrics"""
        async with self._collection_lock:
            # Update system metrics
            self._metrics['system'] = await self._collect_system_metrics()

            # Collect service metrics
            await self._collect_market_data_metrics()
            await self._collect_fundamental_data_metrics()
            await self._collect_database_metrics()

            return self._metrics.copy()


    async def _collect_system_metrics(self) -> dict:
        """Collect system resource metrics"""
        try:
            cpu = psutil.cpu_percent(interval=1) / 100.0
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net = psutil.net_io_counters()

            return {
                'status': ServiceStatus.RUNNING,
                'timestamp': get_current_timestamp(),
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
            self.logger.error(f"Error collecting system metrics: {e}")
            return {
                'status': ServiceStatus.ERROR,
                'timestamp': get_current_timestamp(),
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


    async def _collect_market_data_metrics(self) -> None:
        """Collect market data service metrics"""
        try:
            session = await self._get_session()
            async with session.get('http://market_data:8001/metrics') as response:
                if response.status == 200:
                    metrics = await response.json()

                    # Extract service metrics
                    service_metrics = metrics.get('service', {})
                    error_metrics = metrics.get('errors', {})
                    collection_metrics = metrics.get('collection', {})

                    self._metrics['market_data'].update({
                        'status': ServiceStatus(service_metrics.get('status', 'error')),
                        'timestamp': get_current_timestamp(),
                        'uptime_seconds': service_metrics.get('uptime_seconds', 0),
                        'last_error': service_metrics.get('last_error'),
                        'batch_size': service_metrics.get('batch_size', 0),
                        'errors': {
                            'total': error_metrics.get('total', 0),
                            'by_type': error_metrics.get('by_type', {})
                        },
                        'collection': {
                            'active_symbols': collection_metrics.get('active_symbols', 0),
                            'active_collections': collection_metrics.get('active_collections', 0),
                            'streaming_symbols': collection_metrics.get('streaming_symbols', 0),
                            'progress': collection_metrics.get('progress', {})
                        }
                    })
                else:
                    self.logger.error(f"Market data metrics collection failed: {response.status}")

        except Exception as e:
            self.logger.error(f"Error collecting market data metrics: {e}")
            self._metrics['market_data'].update({
                'status': ServiceStatus.ERROR,
                'timestamp': get_current_timestamp(),
                'error': str(e)
            })


    async def _collect_fundamental_data_metrics(self) -> None:
        """Collect fundamental data service metrics"""
        try:
            session = await self._get_session()
            async with session.get('http://fundamental_data:8002/metrics') as response:
                if response.status == 200:
                    metrics = await response.json()

                    # Extract service metrics
                    service_metrics = metrics.get('service', {})
                    error_metrics = metrics.get('errors', {})
                    collector_metrics = metrics.get('collectors', {})

                    self._metrics['fundamental_data'].update({
                        'status': ServiceStatus(service_metrics.get('status', 'error')),
                        'timestamp': get_current_timestamp(),
                        'uptime_seconds': service_metrics.get('uptime_seconds', 0),
                        'last_error': service_metrics.get('last_error'),
                        'active_tokens': service_metrics.get('active_tokens', 0),
                        'errors': {
                            'total': error_metrics.get('total', 0),
                            'by_type': error_metrics.get('by_type', {})
                        },
                        'collectors': {
                            name: {
                                'running': data.get('running', False),
                                'active_tokens': data.get('active_tokens', 0),
                                'collection_interval': data.get('collection_interval', 0),
                                'last_collection': data.get('last_collection', {}),
                                'current_progress': data.get('current_progress')
                            }
                            for name, data in collector_metrics.items()
                        }
                    })
                else:
                    self.logger.error(f"Fundamental data metrics collection failed: {response.status}")

        except Exception as e:
            self.logger.error(f"Error collecting fundamental data metrics: {e}")
            self._metrics['fundamental_data'].update({
                'status': ServiceStatus.ERROR,
                'timestamp': get_current_timestamp(),
                'error': str(e)
            })


    async def _collect_database_metrics(self) -> None:
        """Collect database metrics"""
        try:
            session = await self._get_session()
            async with session.get('http://timescaledb:9187/metrics') as response:
                if response.status == 200:
                    metrics = await response.json()

                    self._metrics['database'].update({
                        'status': ServiceStatus.RUNNING,
                        'timestamp': get_current_timestamp(),
                        'active_connections': metrics.get('pg_stat_activity_count', 0),
                        'pool_usage': metrics.get('pg_stat_activity_count', 0) / 100,  # Convert to percentage
                        'deadlocks': metrics.get('pg_stat_database_deadlocks', 0),
                        'long_queries': metrics.get('pg_stat_activity_max_tx_duration', 0),
                        'replication_lag': metrics.get('pg_stat_replication_lag', 0)
                    })
                else:
                    self.logger.error(f"Database metrics collection failed: {response.status}")

        except Exception as e:
            self.logger.error(f"Error collecting database metrics: {e}")
            self._metrics['database'].update({
                'status': ServiceStatus.ERROR,
                'timestamp': get_current_timestamp(),
                'error': str(e)
            })


    def get_service_metrics(self, service: str) -> dict:
        """Get latest metrics for a service"""
        if service not in self._metrics:
            return {
                'status': ServiceStatus.UNKNOWN,
                'timestamp': get_current_timestamp(),
                'error': 'Service not found',
                'error_count': 0
            }
        return self._metrics[service]


    def get_all_metrics(self) -> dict[str, dict]:
        """Get all collected metrics"""
        return self._metrics.copy()


    def get_service_status(self) -> dict[str, str]:
        """Get current status of all services"""
        return {
            service: metrics.get('status', ServiceStatus.UNKNOWN)
            for service, metrics in self._metrics.items()
        }


    def get_error_summary(self) -> dict[str, int]:
        """Get error counts by service"""
        return {
            service: metrics.get('error_count', 0)
            for service, metrics in self._metrics.items()
        }


    def get_resource_usage(self) -> dict[str, float]:
        """Get system resource usage summary"""
        system = self._metrics['system']
        return {
            'cpu': system.get('cpu_usage', 0) * 100,
            'memory': system.get('memory_usage', 0) * 100,
            'disk': system.get('disk_usage', 0) * 100
        }


    async def cleanup(self) -> None:
        """Cleanup resources"""
        if self._session:
            await self._session.close()
            self._session = None
