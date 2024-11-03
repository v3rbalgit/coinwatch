# src/services/monitor.py

from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Dict, Any, Optional
import asyncio
from datetime import datetime, timedelta
import psutil
from prometheus_client import generate_latest

from src.config import MonitoringConfig
from src.core.models import Observation
from ..monitoring.metrics import HealthMetrics, MonitoringMetrics
from ..monitoring.observers.base import SystemObserver
from ..monitoring.observers.resource import ResourceObserver
from ..utils.domain_types import ServiceStatus

from ..monitoring.observers.observers import (
    DatabaseObserver,
    NetworkObserver
)
from ..monitoring.actions.dispatcher import ActionDispatcher, ActionMapping
from ..monitoring.actions.resource import (
    MemoryRecoveryAction,
    CPURecoveryAction,
    DiskRecoveryAction
)
from ..services.base import ServiceBase
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class ObserverHealth:
    """Health metrics for individual observers"""
    last_success: Optional[datetime]
    success_rate: float
    error_count: int
    last_error: Optional[str]
    backoff_until: Optional[datetime]

@dataclass
class ObserverState:
    """Track observer state and errors"""
    error_count: int = 0
    last_error: Optional[str] = None
    last_success: Optional[datetime] = None
    backoff_until: Optional[datetime] = None

@dataclass
class MonitoringEntry:
    """Represents a monitoring history entry"""
    timestamp: datetime
    source: str
    severity: str
    metrics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to dictionary"""
        return {
            'timestamp': self.timestamp,
            'source': self.source,
            'severity': self.severity,
            'metrics': self.metrics
        }

class StateManager:
    """Manages monitoring service state"""
    def __init__(self, config: MonitoringConfig):
        self._config = config
        self._lock = asyncio.Lock()
        self._status = ServiceStatus.STOPPED
        self._last_error: Optional[Exception] = None
        self._start_time: Optional[datetime] = None
        self._error_counts: Dict[str, int] = {}
        self._observation_times: Dict[str, datetime] = {}
        self._observer_states: Dict[str, ObserverState] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

        # Calculate metrics history size with hard limit
        observations_per_hour = sum(3600 / interval for interval in self._config.check_intervals.values())
        # Add safety checks
        if observations_per_hour <= 0:
            observations_per_hour = 60  # Default to 1 per minute
        # Calculate desired length
        desired_length = int(config.retention_hours * observations_per_hour)
        # Set hard limit based on container considerations:
        # - 50,000 entries × 300 bytes ≈ 15MB max memory usage
        # - Allows for 24 hours of data at 35 observations/minute
        # - Safe for containers with ≥256MB memory
        maxlen = min(desired_length, 50_000)
        self._metrics_history: Deque[MonitoringEntry] = deque(maxlen=maxlen)

    async def set_status(self, status: ServiceStatus, error: Optional[Exception] = None) -> None:
        """Thread-safe status update"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    self._status = status
                    self._last_error = error
                    if status == ServiceStatus.STARTING:
                        self._start_time = datetime.now()
        except asyncio.TimeoutError:
            logger.error("Status update timed out")
            # TODO: Attempt state recovery
            # await self._recover_state()
            raise

    @property
    def status(self) -> ServiceStatus:
        return self._status

    @property
    def start_time(self) -> Optional[datetime]:
        return self._start_time

    async def add_task(self, name: str, task: asyncio.Task) -> None:
        """Thread-safe task addition"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    self._tasks[name] = task
        except asyncio.TimeoutError:
            logger.error(f"Task addition timed out for {name}")
            raise

    async def get_tasks(self) -> List[asyncio.Task]:
        """Thread-safe task access"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    return list(self._tasks.values())
        except asyncio.TimeoutError:
            logger.error("Task access timed out")
            return []

    async def add_observation(self, source: str, observation: Observation) -> None:
        """Thread-safe observation storage"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    # Cleanup old observations if needed
                    if len(self._observation_times) >= 100:  # Max sources limit
                        oldest = min(self._observation_times.items(), key=lambda x: x[1])
                        del self._observation_times[oldest[0]]

                    entry = MonitoringEntry(
                        timestamp=datetime.now(),
                        source=source,
                        severity=observation.severity,
                        metrics=observation.metrics
                    )

                    self._metrics_history.append(entry)
                    self._observation_times[source] = entry.timestamp
        except asyncio.TimeoutError:
            logger.error(f"Observation storage timed out for {source}")
            raise

    async def update_observer_state(self, name: str, **updates) -> None:
        """Thread-safe observer state update"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    state = self._observer_states.setdefault(name, ObserverState())
                    for key, value in updates.items():
                        setattr(state, key, value)
        except asyncio.TimeoutError:
            logger.error(f"Observer state update timed out for {name}")
            raise

    async def get_observer_state(self, name: str) -> ObserverState:
        """Thread-safe observer state access"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    return self._observer_states.setdefault(name, ObserverState())
        except asyncio.TimeoutError:
            logger.error(f"Observer state access timed out for {name}")
            return ObserverState()

    async def clear(self) -> None:
        """Clear all state"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                async with self._lock:
                    self._metrics_history.clear()
                    self._observation_times.clear()
                    self._observer_states.clear()
                    self._tasks.clear()
                    self._error_counts.clear()
        except asyncio.TimeoutError:
            logger.error("State clearing timed out")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return {
            'status': self._status,
            'error_count': sum(self._error_counts.values()),
            'total_observations': len(self._metrics_history),
            'active_tasks': len(self._tasks),
            'observer_states': self._observer_states.copy()
        }

class MonitoringService(ServiceBase):
    """Centralized monitoring service"""

    def __init__(self,
                 db_service=None,
                 market_service=None,
                 config: Optional[MonitoringConfig] = None):
        super().__init__(config or MonitoringConfig())

        # Initialize state management
        self._state = StateManager(self._config)

        # Initialize observers
        self.observers: Dict[str, SystemObserver] = {
            'resource': ResourceObserver(),
            # 'database': DatabaseObserver(db_service) if db_service else None,
            # 'network': NetworkObserver()
        }

        # Remove any uninitialized observers
        self.observers = {k: v for k, v in self.observers.items() if v is not None}

        # Initialize action dispatcher
        self.dispatcher = ActionDispatcher()
        self._setup_actions()


        # Initialize metrics
        self.metrics = MonitoringMetrics()

        # Update service info
        self.metrics.service_info.info({
            'version': '1.0.0',
            'config_retention_hours': str(self._config.retention_hours),
            'config_check_intervals': str(self._config.check_intervals)
        })

    def _setup_actions(self) -> None:
        """Setup action mappings"""
        mappings = [
            ActionMapping(
                source='resource',
                severity_threshold='warning',
                action_type=MemoryRecoveryAction,
                enabled=True
            ),
            ActionMapping(
                source='resource',
                severity_threshold='critical',
                action_type=CPURecoveryAction,
                enabled=True
            ),
            ActionMapping(
                source='resource',
                severity_threshold='warning',
                action_type=DiskRecoveryAction,
                enabled=True
            )
        ]
        for mapping in mappings:
            self.dispatcher.add_mapping(mapping)

    async def start(self) -> None:
        """Start monitoring service"""
        try:
            await self._state.set_status(ServiceStatus.STARTING)
            logger.info("Starting monitoring service")

            # Connect observers to dispatcher
            for observer in self.observers.values():
                observer.add_observer(self.dispatcher)

            # Start observation tasks
            for name, observer in self.observers.items():
                task = asyncio.create_task(
                    self._run_observer(name, observer),
                    name=f"monitor_{name}"
                )
                await self._state.add_task(name, task)

            await self._state.set_status(ServiceStatus.RUNNING)
            logger.info("Monitoring service started successfully")

        except Exception as e:
            await self._state.set_status(ServiceStatus.ERROR, e)
            logger.error(f"Failed to start monitoring service: {e}")
            raise

    async def stop(self) -> None:
        """Stop monitoring service with proper cleanup"""
        try:
            await self._state.set_status(ServiceStatus.STOPPING)
            logger.info("Stopping monitoring service")

            # Add cleanup start timestamp
            cleanup_start = datetime.now()

            # Stop all tasks
            tasks = await self._state.get_tasks()
            for task in tasks:
                task.cancel()

            if tasks:
                done, pending = await asyncio.wait(
                    tasks,
                    timeout=10.0,
                    return_when=asyncio.ALL_COMPLETED
                )

                # Force cancel any pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=1.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

            # Cleanup observers
            for observer in self.observers.values():
                try:
                    observer.remove_observer(self.dispatcher)
                    if hasattr(observer, 'cleanup'):
                        await observer.cleanup()
                except Exception as e:
                    logger.error(f"Error cleaning up observer: {e}")

            # Clear state
            await self._state.clear()
            await self._state.set_status(ServiceStatus.STOPPED)
            # Log cleanup duration
            cleanup_duration = (datetime.now() - cleanup_start).total_seconds()
            logger.info(f"Monitoring service stopped successfully in {cleanup_duration:.2f}s")

        except Exception as e:
            await self._state.set_status(ServiceStatus.ERROR, e)
            logger.error(f"Error stopping monitoring service: {e}", exc_info=True)
            raise

    async def _run_observer(self, name: str, observer: SystemObserver) -> None:
        """Run observer with error handling and backoff"""
        interval = self._config.check_intervals.get(name, 60)

        while True:
            try:
                state = await self._state.get_observer_state(name)

                if state.backoff_until and datetime.now() < state.backoff_until:
                    await asyncio.sleep(5)
                    continue

                observation = await observer.observe()

                # Update state with success
                await self._state.update_observer_state(
                    name,
                    error_count=0,
                    last_error=None,
                    last_success=datetime.now(),
                    backoff_until=None
                )

                await self._state.add_observation(name, observation)
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info(f"Observer {name} cancelled")
                raise
            except Exception as e:
                await self._handle_observer_error(name, e)
                await asyncio.sleep(self._config.base_backoff)

    # TODO: add circuit breaker mechanism based on max error_count
    async def _handle_observer_error(self, name: str, error: Exception) -> None:
        """Handle observer errors with backoff"""
        try:
            state = await self._state.get_observer_state(name)

            # Update error state
            state.error_count += 1
            state.last_error = str(error)

            # Calculate backoff
            backoff_seconds = min(
                self._config.max_backoff,
                self._config.base_backoff * (2 ** (state.error_count - 1))
            )

            state.backoff_until = datetime.now() + timedelta(seconds=backoff_seconds)

            # Update state
            await self._state.update_observer_state(name, **{
                'error_count': state.error_count,
                'last_error': state.last_error,
                'backoff_until': state.backoff_until
            })

            logger.error(
                f"Error in {name} observer (attempt {state.error_count}): {error}. "
                f"Backing off for {backoff_seconds}s"
            )

        except Exception as e:
            logger.error(f"Error handling observer error: {e}")

    async def get_health_metrics(self) -> HealthMetrics:
        """Get health metrics for Prometheus"""
        try:
            current_time = datetime.now()

            # Get basic metrics
            metrics = self._state.get_metrics()
            start_time = self._state.start_time or current_time
            uptime = (current_time - start_time).total_seconds()

            # Get memory usage
            process = psutil.Process()
            memory_usage = process.memory_info().rss / (1024 * 1024)

            # Get observer states and metrics
            observer_states = {}
            observer_metrics = {}

            for name, observer in self.observers.items():
                state = await self._state.get_observer_state(name)
                observer_states[name] = self._get_observer_health_status(state)
                observer_metrics[name] = await self._get_observer_metrics(name)

            return HealthMetrics(
                status=self._state.status,
                uptime_seconds=uptime,
                active_observers=metrics['active_tasks'],
                total_observations=metrics['total_observations'],
                last_observation_time=await self._get_last_observation_time(),
                error_count=metrics['error_count'],
                observer_states=observer_states,
                memory_usage_mb=memory_usage,
                observer_metrics=observer_metrics
            )

        except Exception as e:
            logger.error(f"Error collecting health metrics: {e}")
            raise

    def _get_observer_health_status(self, state: ObserverState) -> str:
        """Get health status for an observer"""
        if state.backoff_until and datetime.now() < state.backoff_until:
            return 'backoff'
        if state.error_count > 0:
            return 'degraded'
        if state.last_success and (datetime.now() - state.last_success) < timedelta(minutes=5):
            return 'healthy'
        return 'unknown'

    async def _get_observer_metrics(self, observer_name: str) -> Dict[str, float]:
        """Get detailed metrics for an observer"""
        try:
            state = await self._state.get_observer_state(observer_name)

            # Calculate success rate
            total_attempts = max(1, state.error_count + (1 if state.last_success else 0))
            success_rate = (1 if state.last_success else 0) / total_attempts

            # Calculate time since last success
            seconds_since_success = (
                (datetime.now() - state.last_success).total_seconds()
                if state.last_success
                else float('inf')
            )

            return {
                'success_rate': success_rate,
                'error_count': float(state.error_count),
                'seconds_since_success': seconds_since_success
            }

        except Exception as e:
            logger.error(f"Error getting observer metrics for {observer_name}: {e}")
            return {
                'success_rate': 0.0,
                'error_count': 0.0,
                'seconds_since_success': float('inf')
            }

    async def _get_last_observation_time(self) -> Optional[datetime]:
        """Get the timestamp of the last observation"""
        try:
            async with asyncio.timeout(self._config.lock_timeout):
                if not self._state._observation_times:
                    return None
                return max(self._state._observation_times.values())
        except (asyncio.TimeoutError, Exception) as e:
            logger.error(f"Error getting last observation time: {e}")
            return None

    async def get_prometheus_metrics(self) -> bytes:
        """Get metrics in Prometheus format"""
        try:
            health = await self.get_health_metrics()

            # Update Prometheus metrics based on health data
            self.metrics.service_up.labels(
                status=health.status.value
            ).set(1 if health.status == ServiceStatus.RUNNING else 0)

            # Basic service metrics
            self.metrics.service_uptime.inc(health.uptime_seconds)
            self.metrics.active_observers.set(health.active_observers)
            self.metrics.total_observations.inc()
            self.metrics.error_count.inc(health.error_count)
            self.metrics.memory_usage_bytes.set(health.memory_usage_mb * 1024 * 1024)

            # Observer-specific metrics
            for observer_name, observer_metrics in health.observer_metrics.items():
                if 'success_rate' in observer_metrics:
                    self.metrics.observer_success_rate.labels(
                        observer=observer_name
                    ).set(observer_metrics['success_rate'])

                if 'seconds_since_success' in observer_metrics:
                    self.metrics.observer_last_success.labels(
                        observer=observer_name
                    ).set(observer_metrics['seconds_since_success'])

            # Let prometheus_client handle the formatting
            return generate_latest(self.metrics.registry)

        except Exception as e:
            logger.error(f"Error generating Prometheus metrics: {e}")
            # Return basic up metric on error
            self.metrics.service_up.labels(status="error").set(0)
            return generate_latest(self.metrics.registry)

    @property
    def is_healthy(self) -> bool:
        """Enhanced health check"""
        try:
            # Check basic service health
            if self._state.status != ServiceStatus.RUNNING:
                return False

            # Check observer health
            for name, observer in self.observers.items():
                state = self._state._observer_states.get(name)
                if not state or not state.last_success:
                    return False

                # Check if observer is responding within expected interval
                interval = self._config.check_intervals.get(name, 60)
                if (datetime.now() - state.last_success) > timedelta(seconds=interval * 2):
                    return False

            return True

        except Exception as e:
            logger.error(f"Error checking health: {e}")
            return False