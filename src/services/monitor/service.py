# src/services/monitor/service.py

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Deque
import psutil
from prometheus_client import generate_latest

from ...config import MonitoringConfig
from ...core.coordination import Command, MarketDataCommand, ServiceCoordinator
from ..base import ServiceBase
from ...utils.logger import LoggerSetup
from ...utils.domain_types import ServiceStatus
from ...utils.time import TimeUtils
from ...utils.error import ErrorTracker
from ...utils.retry import RetryConfig, RetryStrategy
from ...utils.domain_types import Timestamp
from .monitor_types import CPUMetrics, MemoryMetrics, DiskMetrics, SystemMetrics, ResourceThresholds
from .monitor_metrics import MonitoringMetrics

logger = LoggerSetup.setup(__name__)

@dataclass
class MonitoringEntry:
    """Single monitoring data point"""
    timestamp: int
    source: str
    severity: str
    metrics: Dict[str, Any]
    context: Dict[str, Any]

class MonitoringState:
    """Thread-safe state management"""
    def __init__(self, config: MonitoringConfig):
        self._config = config
        self._lock = asyncio.Lock()
        self._status = ServiceStatus.STOPPED
        self._start_time: Optional[Timestamp] = None

        # Calculate history size with memory constraints
        entries_per_hour = sum(3600 / interval for interval in config.check_intervals.values())
        desired_length = int(config.retention_hours * entries_per_hour)
        # Cap at 50,000 entries (approximately 15MB at 300 bytes per entry)
        self._max_entries = min(desired_length, 50_000)

        self._metrics_history: Deque[MonitoringEntry] = deque(maxlen=self._max_entries)
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        self._error_tracker = ErrorTracker()

    async def add_entry(self, entry: MonitoringEntry) -> None:
        """Thread-safe entry addition"""
        async with self._lock:
            self._metrics_history.append(entry)

            # Track error if severity warrants it
            if entry.severity in ('warning', 'critical'):
                await self._error_tracker.record_error(
                    Exception(f"{entry.source} monitoring alert"),
                    entry.source,
                    severity=entry.severity,
                    **entry.metrics
                )

    async def get_recent_entries(self, minutes: int = 60) -> list[MonitoringEntry]:
        """Get recent monitoring entries"""
        current_time = TimeUtils.get_current_timestamp()
        cutoff_time = current_time - (minutes * 60 * 1000)  # Convert to milliseconds

        async with self._lock:
            return [
                entry for entry in self._metrics_history
                if entry.timestamp >= cutoff_time
            ]

    async def set_status(self, status: ServiceStatus) -> None:
        """Thread-safe status update"""
        async with self._lock:
            self._status = status
            if status == ServiceStatus.STARTING:
                self._start_time = TimeUtils.get_current_timestamp()

class MonitoringService(ServiceBase):
    """
    Centralized monitoring service using ServiceCoordinator

    Responsibilities:
    - System resource monitoring (CPU, memory, disk)
    - Database performance monitoring
    - Market data service monitoring
    - Cache performance monitoring
    - Metrics collection and reporting
    - Health check endpoint
    - Prometheus metrics endpoint
    """

    def __init__(self,
                 coordinator: ServiceCoordinator,
                 config: Optional[MonitoringConfig] = None):
        super().__init__(config or MonitoringConfig())

        self.coordinator = coordinator
        self.metrics = MonitoringMetrics()
        self._state = MonitoringState(self._config)

        # Configure retry strategy
        retry_config = RetryConfig(
            base_delay=self._config.base_backoff,
            max_delay=self._config.max_backoff,
            max_retries=3
        )
        self._retry_strategy = RetryStrategy(retry_config)

        # Resource monitoring state
        self._last_cpu_measure: Optional[float] = None
        self._consecutive_warnings = 0
        self._max_warnings = 3
        self._resource_lock = asyncio.Lock()

        # Default resource thresholds
        self._resource_thresholds: ResourceThresholds = {
            'memory': {'warning': 0.75, 'critical': 0.85},  # 75% warning, 85% critical
            'cpu': {'warning': 0.70, 'critical': 0.80},     # 70% warning, 80% critical
            'disk': {'warning': 0.80, 'critical': 0.90}     # 80% warning, 90% critical
        }

        # Track monitored components
        self._monitored_components: Set[str] = {
            'system',     # CPU, memory, disk
            'database',   # Connection pool, query performance
            'market',     # Collection status, gaps, syncs
            'cache'       # Hit rates, memory usage
        }

    async def start(self) -> None:
        """Start monitoring service"""
        try:
            await self._state.set_status(ServiceStatus.STARTING)
            logger.info("Starting monitoring service")

            # Start monitoring tasks
            for component in self._monitored_components:
                task = asyncio.create_task(
                    self._monitor_component(component),
                    name=f"monitor_{component}"
                )
                self._state._monitoring_tasks[component] = task

            await self._state.set_status(ServiceStatus.RUNNING)
            logger.info("Monitoring service started successfully")

        except Exception as e:
            await self._state.set_status(ServiceStatus.ERROR)
            logger.error(f"Failed to start monitoring service: {e}")
            raise

    async def stop(self) -> None:
        """Stop monitoring service with proper cleanup"""
        try:
            await self._state.set_status(ServiceStatus.STOPPING)
            logger.info("Stopping monitoring service")

            # Cancel all monitoring tasks
            for task in self._state._monitoring_tasks.values():
                task.cancel()

            # Wait for tasks to complete with timeout
            if self._state._monitoring_tasks:
                try:
                    await asyncio.gather(
                        *self._state._monitoring_tasks.values(),
                        return_exceptions=True
                    )
                except asyncio.CancelledError:
                    pass

            # Clean up psutil resources
            try:
                # Reset CPU percent measurement
                psutil.cpu_percent(interval=None)

                # Clear process cache
                for proc in psutil.process_iter(['pid'], ad_value=None):
                    try:
                        proc.info
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

            except Exception as e:
                logger.warning(f"Error during psutil cleanup: {e}")

            # Clear monitoring state
            async with self._resource_lock:
                self._last_cpu_measure = None
                self._consecutive_warnings = 0

            self._state._monitoring_tasks.clear()
            await self._state.set_status(ServiceStatus.STOPPED)
            logger.info("Monitoring service stopped successfully")

        except Exception as e:
            await self._state.set_status(ServiceStatus.ERROR)
            logger.error(f"Error stopping monitoring service: {e}")
            raise


    async def _notify_resource_pressure(self, metrics: SystemMetrics, severity: str) -> None:
        """Notify services of specific resource pressure types and recovery"""
        try:
            if severity == 'recovered':
                # Notify services about recovery
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_BATCH_SIZE,
                    params={
                        "size": 1000,  # Default batch size
                        "context": {
                            "recovered": True,
                            "memory_usage": metrics['memory']['usage'],
                            "cpu_usage": metrics['cpu']['usage'],
                            "disk_usage": metrics['disk']['usage'],
                            "available_memory_mb": metrics['memory']['available_mb'],
                            "load_average": metrics['cpu']['load_avg'],
                            "free_disk_gb": metrics['disk']['free_gb']
                        }
                    }
                ))

                # Also notify cache system about recovery
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_CACHE,
                    params={
                        "error_type": "resource_recovered",
                        "severity": "normal",
                        "context": {
                            "memory_available": metrics['memory']['available_mb'],
                            "cpu_usage": metrics['cpu']['usage'],
                            "disk_free": metrics['disk']['free_gb']
                        }
                    }
                ))
                return

            # Memory pressure
            if metrics['memory']['usage'] > self._resource_thresholds['memory'][severity]:
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_CACHE,
                    params={
                        "error_type": "memory_pressure",
                        "severity": severity,
                        "context": {
                            "available_mb": metrics['memory']['available_mb'],
                            "usage": metrics['memory']['usage'],
                            "swap_usage": metrics['memory']['swap_usage']
                        }
                    }
                ))

            # CPU pressure
            if metrics['cpu']['usage'] > self._resource_thresholds['cpu'][severity]:
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_CACHE,
                    params={
                        "error_type": "cpu_pressure",
                        "severity": severity,
                        "context": {
                            "cpu_usage": metrics['cpu']['usage'],
                            "load_avg": metrics['cpu']['load_avg']
                        }
                    }
                ))

            # Disk pressure
            if metrics['disk']['usage'] > self._resource_thresholds['disk'][severity]:
                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_CACHE,
                    params={
                        "error_type": "disk_pressure",
                        "severity": severity,
                        "context": {
                            "disk_usage": metrics['disk']['usage'],
                            "free_gb": metrics['disk']['free_gb']
                        }
                    }
                ))

            # For critical conditions, also adjust batch size
            if severity == 'critical':
                # Calculate reduction based on most severe pressure
                reduction_ratio = min(
                    (self._resource_thresholds['memory']['critical'] - metrics['memory']['usage']) / self._resource_thresholds['memory']['critical'],
                    (self._resource_thresholds['cpu']['critical'] - metrics['cpu']['usage']) / self._resource_thresholds['cpu']['critical'],
                    (self._resource_thresholds['disk']['critical'] - metrics['disk']['usage']) / self._resource_thresholds['disk']['critical'],
                    0.8  # Maximum reduction to 20% of original
                )

                new_size = max(200, int(1000 * reduction_ratio))  # Never go below 200

                await self.coordinator.execute(Command(
                    type=MarketDataCommand.ADJUST_BATCH_SIZE,
                    params={
                        "size": new_size,
                        "context": {
                            "memory_usage": metrics['memory']['usage'],
                            "cpu_usage": metrics['cpu']['usage'],
                            "disk_usage": metrics['disk']['usage'],
                            "reduction_ratio": reduction_ratio
                        }
                    }
                ))

        except Exception as e:
            logger.error(f"Error notifying resource pressure: {e}")

    async def _get_cpu_metrics(self) -> CPUMetrics:
        """Get CPU metrics with smoothing"""
        try:
            # Get CPU usage over 1 second interval
            current_cpu = psutil.cpu_percent(interval=1) / 100.0

            # Apply exponential smoothing
            if self._last_cpu_measure is not None:
                smoothed_cpu = 0.7 * current_cpu + 0.3 * self._last_cpu_measure
            else:
                smoothed_cpu = current_cpu

            self._last_cpu_measure = smoothed_cpu

            return {
                'usage': smoothed_cpu,
                'count': psutil.cpu_count(logical=False),
                'count_logical': psutil.cpu_count(logical=True),
                'load_avg': [x / 100.0 for x in psutil.getloadavg()]
            }
        except Exception as e:
            logger.error(f"Error collecting CPU metrics: {e}")
            return {
                'usage': 0.0,
                'count': 0,
                'count_logical': 0,
                'load_avg': [0.0, 0.0, 0.0]
            }

    async def _get_memory_metrics(self) -> MemoryMetrics:
        """Get memory metrics"""
        try:
            memory = psutil.virtual_memory()
            return {
                'usage': memory.percent / 100.0,
                'available_mb': memory.available / (1024 * 1024),
                'total_mb': memory.total / (1024 * 1024),
                'swap_usage': psutil.swap_memory().percent / 100.0
            }
        except Exception as e:
            logger.error(f"Error collecting memory metrics: {e}")
            return {
                'usage': 0.0,
                'available_mb': 0.0,
                'total_mb': 0.0,
                'swap_usage': 0.0
            }

    async def _get_disk_metrics(self) -> DiskMetrics:
        """Get disk metrics"""
        try:
            disk = psutil.disk_usage('/')
            io_counters = psutil.disk_io_counters()

            return {
                'usage': disk.percent / 100.0,
                'free_gb': disk.free / (1024 * 1024 * 1024),
                'total_gb': disk.total / (1024 * 1024 * 1024),
                'read_mb': io_counters.read_bytes / (1024 * 1024) if io_counters else 0,
                'write_mb': io_counters.write_bytes / (1024 * 1024) if io_counters else 0
            }
        except Exception as e:
            logger.error(f"Error collecting disk metrics: {e}")
            return {
                'usage': 0.0,
                'free_gb': 0.0,
                'total_gb': 0.0,
                'read_mb': 0.0,
                'write_mb': 0.0
            }

    async def _evaluate_resource_severity(self, metrics: SystemMetrics) -> str:
        """Evaluate resource metrics severity with hysteresis"""
        async with self._resource_lock:
            # Check for critical conditions first
            if (metrics['memory']['usage'] > self._resource_thresholds['memory']['critical'] or
                metrics['cpu']['usage'] > self._resource_thresholds['cpu']['critical'] or
                metrics['disk']['usage'] > self._resource_thresholds['disk']['critical']):
                self._consecutive_warnings = self._max_warnings
                return 'critical'

            # Check for warning conditions with hysteresis
            if (metrics['memory']['usage'] > self._resource_thresholds['memory']['warning'] or
                metrics['cpu']['usage'] > self._resource_thresholds['cpu']['warning'] or
                metrics['disk']['usage'] > self._resource_thresholds['disk']['warning']):
                self._consecutive_warnings += 1
                if self._consecutive_warnings >= self._max_warnings:
                    return 'warning'
                return 'normal'

            # Reset warning count if all metrics are normal
            had_warnings = self._consecutive_warnings > 0
            self._consecutive_warnings = 0

            # If we had warnings but now we're back to normal, signal recovery
            return 'recovered' if had_warnings else 'normal'

    async def _monitor_component(self, component: str) -> None:
        """Monitor a specific component with error handling and backoff"""
        monitor_func = getattr(self, f"_monitor_{component}", None)
        if not monitor_func:
            logger.error(f"No monitoring function found for {component}")
            return

        interval = self._config.check_intervals.get(component, 60)

        while True:
            try:
                metrics = await monitor_func()

                entry = MonitoringEntry(
                    timestamp=TimeUtils.get_current_timestamp(),
                    source=component,
                    severity='normal',
                    metrics=metrics,
                    context={"interval": interval}
                )

                await self._state.add_entry(entry)
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info(f"Monitoring cancelled for {component}")
                break
            except Exception as e:
                logger.error(f"Error monitoring {component}: {e}")
                await asyncio.sleep(self._config.base_backoff)

    async def _monitor_system(self) -> SystemMetrics:
        """Monitor system resources"""
        try:
            metrics: SystemMetrics = {
                'cpu': await self._get_cpu_metrics(),
                'memory': await self._get_memory_metrics(),
                'disk': await self._get_disk_metrics(),
                'boot_time': int(psutil.boot_time() * 1000),
                'process_count': len(psutil.pids())
            }

            severity = await self._evaluate_resource_severity(metrics)

            # Notify services if conditions warrant it
            if severity in ('warning', 'critical', 'recovered'):
                await self._notify_resource_pressure(metrics, severity)

            # Create monitoring entry
            entry = MonitoringEntry(
                timestamp=TimeUtils.get_current_timestamp(),
                source='system',
                severity=severity,
                metrics=metrics,  # type: ignore
                context={
                    'thresholds': self._resource_thresholds,
                    'consecutive_warnings': self._consecutive_warnings
                }
            )

            await self._state.add_entry(entry)

            if severity not in ('normal', 'recovered'):
                logger.warning(
                    f"Resource alert [{severity}]:\n"
                    f"Memory: {metrics['memory']['usage']:.1%}\n"
                    f"CPU: {metrics['cpu']['usage']:.1%}\n"
                    f"Disk: {metrics['disk']['usage']:.1%}"
                )
            elif severity == 'recovered':
                logger.info(
                    "System resources recovered to normal levels:\n"
                    f"Memory: {metrics['memory']['usage']:.1%}\n"
                    f"CPU: {metrics['cpu']['usage']:.1%}\n"
                    f"Disk: {metrics['disk']['usage']:.1%}"
                )

            return metrics

        except Exception as e:
            logger.error(f"Error in system monitoring: {e}")
            raise

    async def _monitor_database(self) -> Dict[str, int | float]:
        """Monitor database performance metrics"""
        try:
            # These would come from DatabaseService
            metrics = {
                'active_connections': 0,
                'pool_size': 0,
                'overflow': 0,
                'available': 0,
                'maintenance_due': False
            }

            # Example severity evaluation
            if metrics['overflow'] > 0:
                severity = 'warning'
                if metrics['overflow'] > metrics['pool_size'] * 0.5:
                    severity = 'critical'
            else:
                severity = 'normal'

            # Create monitoring entry
            entry = MonitoringEntry(
                timestamp=TimeUtils.get_current_timestamp(),
                source='database',
                severity=severity,
                metrics=metrics,
                context={
                    'maintenance_status': 'due' if metrics['maintenance_due'] else 'ok'
                }
            )

            await self._state.add_entry(entry)
            return metrics

        except Exception as e:
            logger.error(f"Error monitoring database: {e}")
            return {}

    async def _monitor_market(self) -> Dict[str, int | float]:
        """Monitor market data service metrics"""
        try:
            metrics: Dict[str, int | float] = {
                'active_collections': 0,
                'pending_collections': 0,
                'active_syncs': 0,
                'batch_size': 0.0,  # Changed to float for consistency
                'concurrent_updates': 0.0  # Changed to float for consistency
            }

            # Evaluate service health
            severity = 'normal'
            if metrics['pending_collections'] > 100:  # Example threshold
                severity = 'warning'

            entry = MonitoringEntry(
                timestamp=TimeUtils.get_current_timestamp(),
                source='market',
                severity=severity,
                metrics=metrics,
                context={}
            )

            await self._state.add_entry(entry)
            return metrics

        except Exception as e:
            logger.error(f"Error monitoring market data: {e}")
            return {}

    async def _monitor_cache(self) -> Dict[str, float]:
        """Monitor cache performance metrics"""
        try:
            # Get cache stats from IndicatorManager
            metrics = {
                'total_size': 0,
                'total_capacity': 0,
                'avg_hit_ratio': 0.0,
                'memory_usage': 0.0,
                'ttl_avg': 0.0
            }

            # Example severity evaluation
            utilization = metrics['total_size'] / metrics['total_capacity'] if metrics['total_capacity'] > 0 else 0
            hit_ratio = metrics['avg_hit_ratio']

            severity = 'normal'
            if utilization > 0.9 or hit_ratio < 0.3:  # Example thresholds
                severity = 'warning'

            entry = MonitoringEntry(
                timestamp=TimeUtils.get_current_timestamp(),
                source='cache',
                severity=severity,
                metrics=metrics,
                context={
                    'cache_types': ['rsi', 'macd', 'bollinger', 'sma', 'ema', 'obv']
                }
            )

            await self._state.add_entry(entry)
            return metrics

        except Exception as e:
            logger.error(f"Error monitoring cache: {e}")
            return {}

    async def get_prometheus_metrics(self) -> bytes:
        """Generate Prometheus metrics"""
        try:
            # Update general service metrics
            self.metrics.service_up.labels(
                status=self._state._status.value
            ).set(1 if self._state._status == ServiceStatus.RUNNING else 0)

            # System metrics from last monitoring entry
            system_entries = [e for e in self._state._metrics_history
                            if e.source == 'system']
            if system_entries:
                latest = system_entries[-1]
                self.metrics.system_memory_usage.set(latest.metrics['memory']['usage'] * 100)
                self.metrics.system_cpu_usage.set(latest.metrics['cpu']['usage'] * 100)
                self.metrics.system_disk_usage.set(latest.metrics['disk']['usage'] * 100)

            # Market data metrics
            market_entries = [e for e in self._state._metrics_history
                            if e.source == 'market']
            if market_entries:
                latest = market_entries[-1]
                self.metrics.market_active_collections.set(latest.metrics['active_collections'])
                self.metrics.market_pending_collections.set(latest.metrics['pending_collections'])
                self.metrics.market_active_syncs.set(latest.metrics['active_syncs'])
                self.metrics.market_batch_size.set(latest.metrics['batch_size'])

            # Database metrics
            db_entries = [e for e in self._state._metrics_history
                        if e.source == 'database']
            if db_entries:
                latest = db_entries[-1]
                self.metrics.db_active_connections.set(latest.metrics['active_connections'])
                pool_usage = (latest.metrics['active_connections'] /
                            latest.metrics['pool_size']) * 100 if latest.metrics['pool_size'] else 0
                self.metrics.db_pool_usage.set(pool_usage)

            # Cache metrics
            cache_entries = [e for e in self._state._metrics_history
                            if e.source == 'cache']
            if cache_entries:
                latest = cache_entries[-1]
                for cache_type in ['rsi', 'macd', 'bollinger', 'sma', 'ema', 'obv']:
                    if cache_type in latest.metrics:
                        self.metrics.cache_hit_ratio.labels(
                            cache_type=cache_type
                        ).set(latest.metrics[f'{cache_type}_hit_ratio'])
                        self.metrics.cache_size.labels(
                            cache_type=cache_type
                        ).set(latest.metrics[f'{cache_type}_size'])

            # Error tracking
            for component in ['system', 'market', 'database', 'cache']:
                entries = [e for e in self._state._metrics_history
                        if e.source == component and e.severity in ('warning', 'critical')]
                for severity in ['warning', 'critical']:
                    count = len([e for e in entries if e.severity == severity])
                    self.metrics.error_count.labels(
                        component=component,
                        severity=severity
                    ).inc(count)

            # Generate metrics
            return generate_latest(self.metrics.registry)

        except Exception as e:
            logger.error(f"Error generating Prometheus metrics: {e}")
            # Return basic up metric on error
            self.metrics.service_up.labels(status="error").set(0)
            return generate_latest(self.metrics.registry)

    def get_service_status(self) -> str:
        """Get comprehensive monitoring service status"""
        status_lines = [
            "Monitoring Service Status:",
            f"Active Monitors: {len(self._state._monitoring_tasks)}",
            f"History Size: {len(self._state._metrics_history)}/{self._state._max_entries}",
            f"Resource Thresholds:",
            f"  Memory: {self._resource_thresholds['memory']['warning']:.0%}/{self._resource_thresholds['memory']['critical']:.0%}",
            f"  CPU: {self._resource_thresholds['cpu']['warning']:.0%}/{self._resource_thresholds['cpu']['critical']:.0%}",
            f"  Disk: {self._resource_thresholds['disk']['warning']:.0%}/{self._resource_thresholds['disk']['critical']:.0%}",
            f"Warning Count: {self._consecutive_warnings}/{self._max_warnings}",
            "",
            "Recent Alerts:"
        ]

        # Add recent alerts
        recent_entries = [
            entry for entry in self._state._metrics_history
            if entry.severity in ('warning', 'critical', 'recovered')
        ][-5:]  # Last 5 alerts

        for entry in recent_entries:
            status_lines.append(
                f"  [{entry.severity.upper()}] {entry.source} at "
                f"{TimeUtils.from_timestamp(Timestamp(entry.timestamp)).strftime('%H:%M:%S')}"
            )

        return "\n".join(status_lines)