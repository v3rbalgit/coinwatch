# src/monitoring/actions/dispatcher.py

from collections import deque
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Type, Optional
from datetime import datetime
import asyncio
from dataclasses import dataclass

from src.core.protocols import ObserverProtocol

from .base import Action
from .resource import MemoryRecoveryAction, CPURecoveryAction, DiskRecoveryAction
from ..observers.base import Observation
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class ActionPriority(IntEnum):
    """Priority levels for actions"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

@dataclass
class ActionMapping:
    """Enhanced action mapping with priority"""
    source: str
    severity_threshold: str
    action_type: Type[Action]
    enabled: bool = True
    priority: ActionPriority = ActionPriority.NORMAL

@dataclass
class ActionResult:
    """Represents the result of an executed action"""
    action_name: str
    observation: Observation
    success: bool
    timestamp: datetime
    error: Optional[str] = None
    details: Optional[Dict] = None

class ActionDispatcher(ObserverProtocol):
    """Dispatches actions based on observations"""

    def __init__(self):
        self._mappings: List[ActionMapping] = []
        self._action_history: deque[ActionResult] = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._setup_default_mappings()
        self.max_history = 1000  # Keep last 1000 actions
        self._cooldown_periods: Dict[str, datetime] = {}
        self.cooldown_minutes = 5  # Default cooldown period

        self._action_semaphore = asyncio.Semaphore(5)  # Limit concurrent actions
        self._priority_queues: Dict[ActionPriority, asyncio.Queue] = {
            priority: asyncio.Queue() for priority in ActionPriority
        }

    def _setup_default_mappings(self) -> None:
        """Setup default action mappings"""
        self._mappings = [
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
            ),
        ]

    def add_mapping(self, mapping: ActionMapping) -> None:
        """Add new action mapping"""
        self._mappings.append(mapping)
        logger.info(f"Added new action mapping for {mapping.source}")

    def remove_mapping(self, source: str, action_type: Type[Action]) -> None:
        """Remove action mapping"""
        self._mappings = [
            m for m in self._mappings
            if not (m.source == source and m.action_type == action_type)
        ]
        logger.info(f"Removed action mapping for {source}")

    async def _check_cooldown(self, action_type: Type[Action]) -> bool:
        """Check if action is in cooldown period"""
        action_key = action_type.__name__
        if action_key in self._cooldown_periods:
            last_execution = self._cooldown_periods[action_key]
            if (datetime.now() - last_execution).total_seconds() < (self.cooldown_minutes * 60):
                logger.debug(f"Action {action_key} is in cooldown period")
                return False
        return True

    def _set_cooldown(self, action_type: Type[Action]) -> None:
        """Set cooldown period for action"""
        self._cooldown_periods[action_type.__name__] = datetime.now()

    async def _cleanup_cooldowns(self) -> None:
        """Add periodic cooldown cleanup"""
        current_time = datetime.now()
        expired = [
            action for action, time in self._cooldown_periods.items()
            if (current_time - time).total_seconds() > self.cooldown_minutes * 60
        ]
        for action in expired:
            del self._cooldown_periods[action]

    async def _execute_action(self,
                            action_type: Type[Action],
                            observation: Observation,
                            priority: ActionPriority) -> ActionResult:
        """Execute action with priority handling"""
        if priority == ActionPriority.CRITICAL:
            # Critical actions bypass cooldown and concurrency limits
            return await self._execute_critical_action(action_type, observation)

        async with self._action_semaphore:
            if not await self._check_cooldown(action_type):
                return ActionResult(
                    action_name=action_type.__name__,
                    observation=observation,
                    success=False,
                    timestamp=datetime.now(),
                    error="Action in cooldown period"
                )

            try:
                action = action_type()
                success = await action.execute(observation.context)
                self._set_cooldown(action_type)

                return ActionResult(
                    action_name=action_type.__name__,
                    observation=observation,
                    success=success,
                    timestamp=datetime.now()
                )
            except Exception as e:
                error_msg = f"Action execution failed: {str(e)}"
                logger.error(error_msg)
                return ActionResult(
                    action_name=action_type.__name__,
                    observation=observation,
                    success=False,
                    timestamp=datetime.now(),
                    error=error_msg
                )

    async def _execute_critical_action(self,
                                     action_type: Type[Action],
                                     observation: Observation) -> ActionResult:
        """Execute critical action with minimal restrictions"""
        try:
            action = action_type()
            success = await action.execute(observation.context)

            return ActionResult(
                action_name=action_type.__name__,
                observation=observation,
                success=success,
                timestamp=datetime.now()
            )
        except Exception as e:
            error_msg = f"Critical action execution failed: {str(e)}"
            logger.error(error_msg)
            return ActionResult(
                action_name=action_type.__name__,
                observation=observation,
                success=False,
                timestamp=datetime.now(),
                error=error_msg
            )

    def _get_action_priority(self, observation: Observation) -> ActionPriority:
        """Determine action priority based on observation"""
        if observation.severity == 'critical':
            return ActionPriority.CRITICAL
        elif observation.severity == 'warning':
            return ActionPriority.HIGH
        return ActionPriority.NORMAL

    def _should_take_action(self,
                           mapping: ActionMapping,
                           observation: Observation) -> bool:
        """Determine if action should be taken with priority consideration"""
        if not mapping.enabled:
            return False

        severity_levels = {
            'normal': 0,
            'warning': 1,
            'critical': 2
        }

        observation_severity = severity_levels.get(observation.severity, 0)
        threshold_severity = severity_levels.get(mapping.severity_threshold, 0)

        # Always take critical actions regardless of mapping threshold
        if observation.severity == 'critical' and mapping.priority == ActionPriority.CRITICAL:
            return True

        return observation_severity >= threshold_severity

    async def on_observation(self, observation: Observation) -> None:
        """Handle new observation and dispatch actions with priority"""
        try:
            async with self._lock:
                # Get relevant mappings and sort by priority
                relevant_mappings = [
                    m for m in self._mappings
                    if m.source == observation.source
                ]

                # Sort mappings by priority (highest first)
                prioritized_mappings = sorted(
                    relevant_mappings,
                    key=lambda m: m.priority.value,
                    reverse=True
                )

                for mapping in prioritized_mappings:
                    if not self._should_take_action(mapping, observation):
                        continue

                    # Get action priority
                    priority = (ActionPriority.CRITICAL
                            if observation.severity == 'critical'
                            else mapping.priority)

                    # Execute action based on priority
                    result = await self._execute_action(
                        mapping.action_type,
                        observation,
                        priority
                    )

                    # Add to history with deque's automatic length management
                    self._action_history.append(result)

                    # Enhanced error logging
                    if not result.success:
                        extra_info = {
                            'priority': priority.name,
                            'source': observation.source,
                            'severity': observation.severity,
                            'action': result.action_name
                        }
                        if priority == ActionPriority.CRITICAL:
                            logger.error(
                                f"Critical action {result.action_name} failed: {result.error}",
                                extra=extra_info
                            )
                        else:
                            logger.warning(
                                f"Action {result.action_name} failed: {result.error}",
                                extra=extra_info
                            )
                    else:
                        logger.info(
                            f"Action {result.action_name} executed successfully",
                            extra={
                                'priority': priority.name,
                                'source': observation.source,
                                'severity': observation.severity
                            }
                        )

                    # Break after first successful critical action
                    if priority == ActionPriority.CRITICAL and result.success:
                        break

        except Exception as e:
            logger.error(
                f"Error in action dispatch: {e}",
                extra={
                    'source': observation.source,
                    'severity': observation.severity
                },
                exc_info=True
            )


    def get_action_history(self,
                      limit: Optional[int] = None,
                      source: Optional[str] = None) -> List[ActionResult]:
        """Get action execution history with efficient filtering"""
        # Convert deque to list for filtering
        history = list(self._action_history)

        # Filter by source if specified
        if source:
            history = [
                r for r in history
                if r.observation.source == source
            ]

        # Apply limit if specified
        if limit and limit < len(history):
            return history[-limit:]

        return history

    def clear_history(self) -> None:
        """Clear action history"""
        self._action_history.clear()  # Use deque's clear method instead of reassignment
        logger.info("Action history cleared")

    def get_mappings(self) -> List[ActionMapping]:
        """Get current action mappings"""
        return self._mappings.copy()

    def set_cooldown_period(self, minutes: int) -> None:
        """Set global cooldown period"""
        if minutes < 1:
            raise ValueError("Cooldown period must be at least 1 minute")
        self.cooldown_minutes = minutes
        logger.info(f"Cooldown period set to {minutes} minutes")