# src/services/database/recovery.py

from dataclasses import dataclass
from enum import Enum
import asyncio
from typing import Dict, Any, Union, Protocol
from sqlalchemy import text

from ...core.monitoring import DatabaseMetrics
from ...utils.time import TimeUtils
from ...utils.domain_types import DatabaseErrorType
from ...utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class CircuitBreakerThreshold:
    """Thresholds for circuit breaker state transitions"""
    warning_threshold: float
    error_threshold: float
    critical_threshold: float
    recovery_threshold: float
    check_window: int = 300    # Window for checking metrics (seconds)

class CircuitState(Enum):
    """Circuit breaker states"""
    NORMAL = "normal"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class RecoveryAction(Protocol):
    """Protocol for recovery action callables"""
    async def __call__(self, value: float) -> None: ...

class DatabaseRecovery:
    """
    Enhanced database recovery using circuit breaker pattern.
    Handles all database health monitoring and recovery actions.
    """

    def __init__(self, db_service: Any):  # Any used as we can't import DatabaseService (circular)
        self.db_service = db_service
        self._states: Dict[str, CircuitState] = {}
        self._last_check: Dict[str, float] = {}
        self._running: bool = True
        self._lock = asyncio.Lock()

        # Configure thresholds for all database health metrics
        self.thresholds = {
            "connection_usage": CircuitBreakerThreshold(
                warning_threshold=0.7,    # 70% pool usage
                error_threshold=0.85,     # 85% pool usage
                critical_threshold=0.95,  # 95% pool usage
                recovery_threshold=0.6    # Recover when below 60%
            ),
            "connection_timeout": CircuitBreakerThreshold(
                warning_threshold=3,      # 3 timeouts/minute
                error_threshold=5,        # 5 timeouts/minute
                critical_threshold=10,    # 10 timeouts/minute
                recovery_threshold=1,     # Back to normal when below this
                check_window=60           # Check every minute
            ),
            "replication_lag": CircuitBreakerThreshold(
                warning_threshold=30,     # 30s lag
                error_threshold=60,       # 1min lag
                critical_threshold=300,   # 5min lag
                recovery_threshold=15     # Recover when below 15s
            ),
            "query_timeout": CircuitBreakerThreshold(
                warning_threshold=3,      # 3 long queries
                error_threshold=5,        # 5 long queries
                critical_threshold=10,    # 10 long queries
                recovery_threshold=1      # Recover when only 1 long query
            ),
            "lock_timeout": CircuitBreakerThreshold(
                warning_threshold=3,      # 3 lock timeouts
                error_threshold=5,        # 5 lock timeouts
                critical_threshold=10,    # 10 lock timeouts
                recovery_threshold=1      # Recover when only 1 timeout
            )
        }

        # Configure recovery actions for each state and metric
        self.recovery_actions: Dict[str, Dict[str, Union[Dict[str, Any], RecoveryAction]]] = {
            "connection_usage": {
                "warning": {"max_overflow": 10, "pool_timeout": 15},
                "error": {"max_overflow": 20, "pool_timeout": 20},
                "critical": {"max_overflow": 30, "pool_timeout": 30}
            },
            "connection_timeout": {
                "warning": {"pool_timeout": 15, "pool_recycle": 900},
                "error": {"pool_timeout": 20, "pool_recycle": 600},
                "critical": {"pool_timeout": 30, "pool_recycle": 300}
            },
            "query_timeout": {
                "warning": {"statement_timeout": 20},
                "error": {"statement_timeout": 15},
                "critical": {"statement_timeout": 10}
            },
            "lock_timeout": {
                "warning": {"lock_timeout": 20},
                "error": {"lock_timeout": 15},
                "critical": {"lock_timeout": 10}
            },
            "replication_lag": {
                "warning": {"synchronous_commit": "local"},
                "error": {"synchronous_commit": "on"},
                "critical": {"synchronous_commit": "remote_write"}
            }
        }

    async def check_and_recover(self, metrics: DatabaseMetrics) -> None:
        """
        Check all metrics and handle recovery, including deadlocks and maintenance.

        Args:
            metrics: DatabaseMetrics containing current database state
        """
        if not self._running:
            return

        async with self._lock:
            current_time = TimeUtils.get_current_timestamp()

            # Calculate metrics values
            metric_values = {
                "connection_usage": metrics.active_connections / metrics.pool_size,
                "connection_timeout": metrics.error_count,  # Using error count as proxy for timeouts
                "replication_lag": metrics.replication_lag_seconds,
                "query_timeout": metrics.long_queries,
                "lock_timeout": metrics.deadlocks  # Using deadlocks as indicator for lock issues
            }

            for metric, value in metric_values.items():
                threshold = self.thresholds[metric]
                current_state = self._states.get(metric, CircuitState.NORMAL)
                last_check = self._last_check.get(metric, 0)

                # Only check if enough time has passed
                if (current_time - last_check) < (threshold.check_window * 1000):
                    continue

                new_state = self._determine_state(value, threshold, metric)

                if new_state != current_state:
                    await self._handle_state_transition(metric, current_state, new_state, value)
                    self._states[metric] = new_state
                    self._last_check[metric] = current_time

    def _determine_state(self, value: float, threshold: CircuitBreakerThreshold, metric: str) -> CircuitState:
        """
        Determine circuit state based on current value and thresholds.

        Args:
            value: Current metric value
            threshold: Threshold configuration for the metric
            metric: Name of the metric

        Returns:
            CircuitState: New state for the circuit
        """
        if value >= threshold.critical_threshold:
            return CircuitState.CRITICAL
        elif value >= threshold.error_threshold:
            return CircuitState.ERROR
        elif value >= threshold.warning_threshold:
            return CircuitState.WARNING
        elif value <= threshold.recovery_threshold:
            return CircuitState.NORMAL

        # Stay in current state if between recovery and warning thresholds
        return self._states.get(metric, CircuitState.NORMAL)

    async def _handle_state_transition(
        self,
        metric: str,
        old_state: CircuitState,
        new_state: CircuitState,
        current_value: float
    ) -> None:
        """
        Handle state transition with enhanced recovery actions.

        Args:
            metric: Name of the metric
            old_state: Previous circuit state
            new_state: New circuit state
            current_value: Current metric value
        """
        try:
            actions = self.recovery_actions[metric].get(new_state.value)
            if not actions:
                return

            if isinstance(actions, dict):
                # Handle configuration changes
                if metric in ("connection_usage", "connection_timeout"):
                    await self.db_service._reconfigure_pool(**actions)
                else:
                    async with self.db_service.get_session() as session:
                        for setting, value in actions.items():
                            await session.execute(text(f"SET {setting} = '{value}s'"))
            else:
                # Handle callable recovery actions (deadlocks, maintenance)
                await actions(current_value)

            logger.info(
                f"State transition for {metric}: {old_state.value} -> {new_state.value} "
                f"(current value: {current_value})"
            )

        except Exception as e:
            logger.error(f"Error handling state transition for {metric}: {e}")
            if new_state == CircuitState.CRITICAL:
                await self.db_service.handle_critical_condition({
                    "type": DatabaseErrorType.EMERGENCY,  # Use proper error type
                    "severity": "critical",
                    "message": f"Critical state transition failed for {metric}",
                    "error_type": e.__class__.__name__,
                    "context": {"metric": metric, "value": current_value}
                })

    async def cleanup(self) -> None:
        """Cleanup resources and stop recovery operations"""
        self._running = False
        async with self._lock:
            self._states.clear()
            self._last_check.clear()

    def get_status(self) -> str:
        """
        Get current recovery status including maintenance and deadlocks.

        Returns:
            str: Formatted status string
        """
        current_time = TimeUtils.get_current_timestamp()
        status_lines = ["Recovery Status:"]

        for metric, state in self._states.items():
            last_check = self._last_check.get(metric, 0)
            time_since = (current_time - last_check) / 1000  # Convert to seconds

            status_lines.append(
                f"  {metric}: {state.value} "
                f"(last checked: {time_since:.1f}s ago)"
            )

        return "\n".join(status_lines)