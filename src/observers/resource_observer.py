# src/observers/resource_observer.py

from typing import Dict, Any, Optional, List, NamedTuple
import psutil
import asyncio
import time
from dataclasses import dataclass
from src.core.observers.base import Observer, ObservationContext
from src.core.events import Event, EventType
from src.core.actions import ActionPriority
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class ResourceMetrics:
    """Container for resource metrics."""
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    swap_percent: float
    load_average: List[float]
    io_counters: Dict[str, int]
    network_io: Dict[str, int]
    process_count: int
    thread_count: int
    open_files: int
    open_connections: int

class ResourceThresholds(NamedTuple):
    """Thresholds for resource monitoring."""
    cpu_warning: float = 70.0
    cpu_critical: float = 85.0
    memory_warning: float = 75.0
    memory_critical: float = 90.0
    disk_warning: float = 80.0
    disk_critical: float = 95.0
    swap_warning: float = 60.0
    swap_critical: float = 80.0

class ResourceObserver(Observer):
    """
    Observes system resource usage and triggers actions when thresholds are exceeded.

    Monitors:
    - CPU usage and load
    - Memory usage and swap
    - Disk space and I/O
    - Network I/O
    - Process and thread counts
    - Open files and connections
    """

    def __init__(self,
                 state_manager,
                 action_manager,
                 observation_interval: float = 30.0,
                 thresholds: Optional[ResourceThresholds] = None):
        super().__init__(
            name="ResourceObserver",
            state_manager=state_manager,
            action_manager=action_manager,
            observation_interval=observation_interval
        )
        self.thresholds = thresholds or ResourceThresholds()
        self._previous_metrics: Optional[ResourceMetrics] = None
        self._consecutive_warnings = 0
        self._last_action_time: Dict[str, float] = {}
        self.ACTION_COOLDOWN = 300  # 5 minutes between same actions

    async def observe(self) -> ObservationContext:
        """Collect current resource metrics."""
        try:
            # Collect metrics asynchronously
            loop = asyncio.get_event_loop()
            metrics = await loop.run_in_executor(None, self._collect_metrics)

            context = ObservationContext(
                timestamp=time.time(),
                previous_state={'metrics': self._previous_metrics} if self._previous_metrics else None,
                current_state={'metrics': metrics},
                changes_detected=self._detect_significant_changes(
                    self._previous_metrics,
                    metrics
                ) if self._previous_metrics else False
            )

            self._previous_metrics = metrics
            return context

        except Exception as e:
            logger.error(f"Error collecting metrics: {e}", exc_info=True)
            return ObservationContext(
                timestamp=time.time(),
                error=e
            )

    async def analyze(self, context: ObservationContext) -> None:
        """Analyze resource metrics and trigger actions if needed."""
        if not context.current_state or 'metrics' not in context.current_state:
            return

        metrics: ResourceMetrics = context.current_state['metrics']
        current_time = time.time()

        try:
            # Check critical conditions first
            if await self._check_critical_conditions(metrics, current_time):
                return  # Skip other checks if critical condition handled

            # Check warning conditions
            if await self._check_warning_conditions(metrics, current_time):
                self._consecutive_warnings += 1
            else:
                self._consecutive_warnings = 0

            # Update metrics
            self._update_metrics(metrics)

        except Exception as e:
            logger.error(f"Error analyzing metrics: {e}", exc_info=True)
            await self.handle_error(e)

    async def handle_error(self, error: Exception) -> None:
        """Handle observation errors."""
        logger.error(f"Resource observation error: {error}", exc_info=True)

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_HEALTH_CHANGED,
            {
                'service_name': self.name,
                'status': 'error',
                'error': str(error),
                'timestamp': time.time()
            }
        ))

        # Error handling action
        current_time = time.time()
        if self._can_trigger_action('handle_resource_error', current_time):
            await self.action_manager.submit_action(
                'handle_resource_error',
                {
                    'observer_name': self.name,
                    'error': str(error),
                    'timestamp': current_time,
                    'severity': 'critical'  # Errors are always critical
                },
                priority=ActionPriority.HIGH
            )

    def _collect_metrics(self) -> ResourceMetrics:
        """Collect comprehensive system resource metrics."""
        try:
            virtual_memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            disk = psutil.disk_usage('/')

            # Get process-specific metrics
            current_process = psutil.Process()

            return ResourceMetrics(
                cpu_percent=psutil.cpu_percent(interval=1),
                memory_percent=virtual_memory.percent,
                disk_percent=disk.percent,
                swap_percent=swap.percent,
                load_average=list(psutil.getloadavg()),  # Fixed: converting tuple to list
                io_counters=self._get_io_counters(),
                network_io=self._get_network_io(),
                process_count=len(psutil.pids()),
                thread_count=sum(1 for _ in current_process.threads()),
                open_files=len(current_process.open_files()),
                open_connections=len(current_process.connections())
            )
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}", exc_info=True)
            raise

    async def _check_critical_conditions(self, metrics: ResourceMetrics, current_time: float) -> bool:
        """Check for critical resource conditions."""
        critical_conditions = [
            (metrics.cpu_percent >= self.thresholds.cpu_critical,
             'handle_critical_cpu',
             {
                 'cpu_percent': metrics.cpu_percent,
                 'load_average': metrics.load_average,
                 'severity': 'critical'
             }),

            (metrics.memory_percent >= self.thresholds.memory_critical,
             'handle_critical_memory',
             {
                 'memory_percent': metrics.memory_percent,
                 'swap_percent': metrics.swap_percent,
                 'severity': 'critical'
             }),

            (metrics.disk_percent >= self.thresholds.disk_critical,
             'handle_critical_disk',
             {
                 'disk_percent': metrics.disk_percent,
                 'io_counters': metrics.io_counters,
                 'severity': 'critical'
             })
        ]

        for condition, action, params in critical_conditions:
            if condition and self._can_trigger_action(action, current_time):
                await self._emit_resource_event('critical', params)
                await self.action_manager.submit_action(
                    action,
                    params,
                    priority=ActionPriority.CRITICAL
                )
                self._last_action_time[action] = current_time
                return True

        return False

    async def _check_warning_conditions(self, metrics: ResourceMetrics, current_time: float) -> bool:
        """Check for warning resource conditions."""
        warning_conditions = [
            (metrics.cpu_percent >= self.thresholds.cpu_warning,
             'handle_high_cpu',
             {
                 'cpu_percent': metrics.cpu_percent,
                 'load_average': metrics.load_average,
                 'severity': 'warning'
             }),

            (metrics.memory_percent >= self.thresholds.memory_warning,
             'handle_high_memory',
             {
                 'memory_percent': metrics.memory_percent,
                 'swap_percent': metrics.swap_percent,
                 'severity': 'warning'
             }),

            (metrics.disk_percent >= self.thresholds.disk_warning,
             'handle_high_disk',
             {
                 'disk_percent': metrics.disk_percent,
                 'io_counters': metrics.io_counters,
                 'severity': 'warning'
             })
        ]

        warning_triggered = False
        for condition, action, params in warning_conditions:
            if condition and self._can_trigger_action(action, current_time):
                await self._emit_resource_event('warning', params)
                await self.action_manager.submit_action(
                    action,
                    params,
                    priority=ActionPriority.HIGH
                )
                self._last_action_time[action] = current_time
                warning_triggered = True

        return warning_triggered

    def _detect_significant_changes(self,
                                  previous: Optional[ResourceMetrics],
                                  current: ResourceMetrics) -> bool:
        """Detect significant changes in metrics."""
        if not previous:
            return False

        CPU_CHANGE_THRESHOLD = 15.0
        MEMORY_CHANGE_THRESHOLD = 10.0
        DISK_CHANGE_THRESHOLD = 5.0

        return any([
            abs(current.cpu_percent - previous.cpu_percent) >= CPU_CHANGE_THRESHOLD,
            abs(current.memory_percent - previous.memory_percent) >= MEMORY_CHANGE_THRESHOLD,
            abs(current.disk_percent - previous.disk_percent) >= DISK_CHANGE_THRESHOLD,
            abs(current.thread_count - previous.thread_count) >= 10,
            abs(current.open_files - previous.open_files) >= 20
        ])

    async def _emit_resource_event(self, severity: str, details: Dict[str, Any]) -> None:
        """Emit resource-related event."""
        await self.state_manager.emit_event(Event.create(
            EventType.RESOURCE_EXHAUSTED,
            {
                'severity': severity,
                'timestamp': time.time(),
                'details': details,
                'consecutive_warnings': self._consecutive_warnings
            }
        ))

    def _can_trigger_action(self, action_name: str, current_time: Optional[float] = None) -> bool:
        """Check if enough time has passed to trigger an action again."""
        current_time = current_time or time.time()
        last_time = self._last_action_time.get(action_name, 0)
        return (current_time - last_time) >= self.ACTION_COOLDOWN

    def _update_metrics(self, metrics: ResourceMetrics) -> None:
        """Update internal metrics state."""
        self._metrics.update({
            'cpu_usage': metrics.cpu_percent,
            'memory_usage': metrics.memory_percent,
            'disk_usage': metrics.disk_percent,
            'swap_usage': metrics.swap_percent,
            'load_average': metrics.load_average,
            'process_count': metrics.process_count,
            'thread_count': metrics.thread_count,
            'open_files': metrics.open_files,
            'open_connections': metrics.open_connections,
            'consecutive_warnings': self._consecutive_warnings,
            'last_update': time.time()
        })

    def _get_io_counters(self) -> Dict[str, int]:
        """Get disk I/O counters."""
        try:
            io = psutil.disk_io_counters()
            return {
                'read_bytes': io.read_bytes,
                'write_bytes': io.write_bytes,
                'read_count': io.read_count,
                'write_count': io.write_count
            }
        except Exception as e:
            logger.error(f"Failed to get I/O counters: {e}")
            return {}

    def _get_network_io(self) -> Dict[str, int]:
        """Get network I/O counters."""
        try:
            net = psutil.net_io_counters()
            return {
                'bytes_sent': net.bytes_sent,
                'bytes_recv': net.bytes_recv,
                'packets_sent': net.packets_sent,
                'packets_recv': net.packets_recv
            }
        except Exception as e:
            logger.error(f"Failed to get network I/O counters: {e}")
            return {}