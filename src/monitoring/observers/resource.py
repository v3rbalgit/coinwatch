# src/monitoring/observers/resource.py

import asyncio
import psutil
from datetime import datetime
from typing import Dict, Optional
from ...core.models import Observation
from src.monitoring.observers.base import SystemObserver
from ...utils.logger import LoggerSetup
from ..monitor_types import CPUMetrics, MemoryMetrics, DiskMetrics, ResourceMetrics

logger = LoggerSetup.setup(__name__)

class ResourceThresholds:
    """Resource monitoring thresholds"""
    # Warning thresholds
    MEMORY_WARNING = 0.75  # 75%
    CPU_WARNING = 0.70     # 70%
    DISK_WARNING = 0.80    # 80%

    # Critical thresholds
    MEMORY_CRITICAL = 0.85  # 85%
    CPU_CRITICAL = 0.80     # 80%
    DISK_CRITICAL = 0.90    # 90%

class ResourceObserver(SystemObserver):
    """Monitor system resources with configurable thresholds"""

    def __init__(self):
        super().__init__()
        self.interval = 30  # 30 seconds between checks

        # Initialize default thresholds
        self.set_threshold('memory', warning=0.75, critical=0.85)
        self.set_threshold('cpu', warning=0.70, critical=0.80)
        self.set_threshold('disk', warning=0.80, critical=0.90)

        self._last_cpu_measure: Optional[float] = None
        self._consecutive_warnings = 0
        self._max_warnings = 3

        self._state_lock = asyncio.Lock()

    async def _get_memory_metrics(self) -> MemoryMetrics:
        """Get detailed memory metrics"""
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

    async def _get_cpu_metrics(self) -> CPUMetrics:
        """Get detailed CPU metrics with smoothing"""
        try:
            # Get CPU usage over 1 second interval
            current_cpu = psutil.cpu_percent(interval=1) / 100.0

            # Simple exponential smoothing
            if self._last_cpu_measure is not None:
                smoothed_cpu = 0.7 * current_cpu + 0.3 * self._last_cpu_measure
            else:
                smoothed_cpu = current_cpu

            self._last_cpu_measure = smoothed_cpu

            return {
                'usage': smoothed_cpu,
                'count': psutil.cpu_count(),
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

    async def _get_disk_metrics(self) -> DiskMetrics:
        """Get detailed disk metrics"""
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

    async def _evaluate_severity(self, metrics: Dict[str, ResourceMetrics]) -> str:
        """
        Evaluate metrics severity with hysteresis
        Returns: 'normal', 'warning', or 'critical'
        """
        memory_thresholds = self.get_threshold('memory')
        cpu_thresholds = self.get_threshold('cpu')
        disk_thresholds = self.get_threshold('disk')

        async with self._state_lock:
            # Check for critical conditions first
            if (metrics['memory']['usage'] > memory_thresholds['critical'] or
                metrics['cpu']['usage'] > cpu_thresholds['critical'] or
                metrics['disk']['usage'] > disk_thresholds['critical']):
                self._consecutive_warnings = self._max_warnings
                return 'critical'

            # Check for warning conditions
            if (metrics['memory']['usage'] > memory_thresholds['warning'] or
                metrics['cpu']['usage'] > cpu_thresholds['warning'] or
                metrics['disk']['usage'] > disk_thresholds['warning']):
                self._consecutive_warnings += 1
                if self._consecutive_warnings >= self._max_warnings:
                    return 'warning'
                return 'normal'

            self._consecutive_warnings = 0
            return 'normal'

    async def observe(self) -> Observation:
        """Perform resource observation"""
        try:
            # Collect all metrics
            metrics: Dict[str, ResourceMetrics] = {
                'memory': await self._get_memory_metrics(),
                'cpu': await self._get_cpu_metrics(),
                'disk': await self._get_disk_metrics()
            }

            # Evaluate severity
            severity = await self._evaluate_severity(metrics)

            # Create observation
            observation = Observation(
                timestamp=datetime.now(),
                source='resource',
                metrics=metrics,
                severity=severity,
                context={
                    'thresholds': self._thresholds,
                    'consecutive_warnings': self._consecutive_warnings,
                    'process_count': len(psutil.pids()),
                    'boot_time': psutil.boot_time()
                }
            )

            # Log if not normal
            if severity != 'normal':
                logger.warning(
                    f"Resource observation: {severity}\n"
                    f"Memory: {metrics['memory']['usage']:.1%}\n"
                    f"CPU: {metrics['cpu']['usage']:.1%}\n"
                    f"Disk: {metrics['disk']['usage']:.1%}"
                )

            return observation

        except Exception as e:
            logger.error(f"Error in resource observation: {e}")
            # Return a minimal observation on error
            return Observation(
                timestamp=datetime.now(),
                source='resource',
                metrics={'error': str(e)},
                severity='critical',
                context={'error': True}
            )

    def update_thresholds(self,
                         resource: str,
                         warning: Optional[float] = None,
                         critical: Optional[float] = None) -> None:
        """Update monitoring thresholds"""
        current = self.get_threshold(resource)

        new_warning = warning if warning is not None else current['warning']
        new_critical = critical if critical is not None else current['critical']

        self.set_threshold(resource, new_warning, new_critical)
        logger.info(f"Updated {resource} thresholds: warning={new_warning}, critical={new_critical}")

    async def cleanup(self) -> None:
        """Clean up resource monitoring and release resources"""
        try:
            logger.info("Cleaning up ResourceObserver")

            # First, call parent cleanup to clear observers
            await super().cleanup()

            async with self._state_lock:
                # Clear monitoring state
                self._consecutive_warnings = 0
                self._last_cpu_measure = None

                # Clear thresholds
                self._thresholds.clear()

                # Reset psutil CPU utilization measurement
                try:
                    # Reset CPU percent measurement
                    psutil.cpu_percent(interval=None)

                    # Clear process cache by forcing a cache refresh
                    for proc in psutil.process_iter(['pid', 'name'], ad_value=None):
                        try:
                            # Force refresh process info
                            proc.info
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue

                except Exception as e:
                    logger.warning(f"Error during psutil cleanup: {e}")

                logger.info("ResourceObserver cleanup completed")

        except Exception as e:
            logger.error(f"Error during ResourceObserver cleanup: {e}")
            raise

