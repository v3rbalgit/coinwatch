# src/core/actions.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Any, List, Optional, Type, Callable, TYPE_CHECKING
import asyncio
import uuid
import time
import logging
from .events import Event, EventType
from .exceptions import ActionError, ActionValidationError

if TYPE_CHECKING:
    from .state import StateManager

logger = logging.getLogger(__name__)

class ActionPriority(IntEnum):
    """Priority levels for actions."""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3

@dataclass
class ActionContext:
    """Context information for action execution."""
    id: str
    params: Dict[str, Any]
    metadata: Dict[str, Any]
    correlation_id: str
    priority: ActionPriority
    max_retries: int = 3
    retry_count: int = 0

    @classmethod
    def create(cls,
               params: Dict[str, Any],
               priority: ActionPriority,
               metadata: Optional[Dict[str, Any]] = None,
               correlation_id: Optional[str] = None,
               max_retries: int = 3) -> 'ActionContext':
        """Create a new action context."""
        return cls(
            id=str(uuid.uuid4()),
            params=params,
            metadata=metadata or {},
            correlation_id=correlation_id or str(uuid.uuid4()),
            priority=priority,
            max_retries=max_retries
        )

class Action(ABC):
    """Base class for all actions."""

    def __init__(self, context: ActionContext):
        self.context = context
        self.state_manager: Optional['StateManager'] = None

    @abstractmethod
    async def validate(self) -> None:
        """Validate action parameters."""
        pass

    @abstractmethod
    async def execute(self) -> None:
        """Execute the action."""
        pass

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the action if needed."""
        pass

    async def emit_event(self, event_type: EventType, data: Dict[str, Any]) -> None:
        """Emit an event through the state manager."""
        if self.state_manager is None:
            raise ActionError("State manager not set")

        event = Event.create(
            event_type=event_type,
            data=data,
            correlation_id=self.context.correlation_id
        )
        await self.state_manager.emit_event(event)

class ActionRegistry:
    """Registry for action types and their validators."""

    def __init__(self):
        self._actions: Dict[str, Type[Action]] = {}
        self._validators: Dict[str, List[Callable]] = {}
        logger.info("Action Registry initialized")

    def register(self,
                action_name: str,
                action_class: Type[Action],
                validators: Optional[List[Callable]] = None) -> None:
        """Register a new action type."""
        self._actions[action_name] = action_class
        self._validators[action_name] = validators or []
        logger.debug(f"Registered action: {action_name}")

    def create_action(self,
                     action_name: str,
                     context: ActionContext) -> Optional[Action]:
        """Create an instance of a registered action."""
        action_class = self._actions.get(action_name)
        if not action_class:
            return None

        return action_class(context)

    def get_validators(self, action_name: str) -> List[Callable]:
        """Get validators for an action type."""
        return self._validators.get(action_name, [])

class ActionManager:
    """Manages action execution and coordination."""

    def __init__(self, state_manager: 'StateManager', registry: ActionRegistry):
        self.state_manager = state_manager
        self.registry = registry
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._running: Dict[str, Action] = {}
        self._lock = asyncio.Lock()
        self._processing_task: Optional[asyncio.Task] = None
        self._running_flag = False
        logger.info("Action Manager initialized")

    async def start(self) -> None:
        """Start action processing."""
        if self._running_flag:
            return

        self._running_flag = True
        self._processing_task = asyncio.create_task(self._process_actions())
        logger.info("Action Manager started")

    async def stop(self) -> None:
        """Stop action processing."""
        if not self._running_flag:
            return

        self._running_flag = False

        # Wait for running actions to complete
        if self._running:
            logger.info(f"Waiting for {len(self._running)} actions to complete")
            await asyncio.gather(*[
                self._wait_for_action(action_id)
                for action_id in list(self._running.keys())
            ])

        # Wait for processing task
        if self._processing_task:
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for action processing to complete")

        logger.info("Action Manager stopped")

    async def submit_action(self,
                          action_name: str,
                          params: Dict[str, Any],
                          priority: ActionPriority = ActionPriority.MEDIUM,
                          metadata: Optional[Dict[str, Any]] = None,
                          correlation_id: Optional[str] = None) -> str:
        """Submit a new action for execution."""
        context = ActionContext.create(
            params=params,
            priority=priority,
            metadata=metadata,
            correlation_id=correlation_id
        )

        action = self.registry.create_action(action_name, context)
        if not action:
            raise ActionError(f"Unknown action type: {action_name}")

        action.state_manager = self.state_manager

        # Put in priority queue
        await self._queue.put((priority.value, time.time(), action))

        logger.debug(f"Submitted action {action_name} with ID {context.id}")
        return context.id

    async def _process_actions(self) -> None:
        """Process actions from the queue."""
        while True:
            try:
                # Get next action
                _, _, action = await self._queue.get()

                # Execute action
                async with self._lock:
                    self._running[action.context.id] = action

                try:
                    # Validate
                    await action.validate()
                    for validator in self.registry.get_validators(
                        action.__class__.__name__
                    ):
                        await validator(action.context)

                    # Execute
                    await self._execute_action(action)

                finally:
                    async with self._lock:
                        self._running.pop(action.context.id, None)

                self._queue.task_done()

            except Exception as e:
                logger.error(f"Error processing action: {e}", exc_info=True)

    async def _execute_action(self, action: Action) -> None:
        """Execute an action with event emission."""
        try:
            # Emit start event
            await action.emit_event(
                EventType.ACTION_STARTED,
                {
                    'action_id': action.context.id,
                    'action_type': action.__class__.__name__
                }
            )

            # Execute action
            await action.execute()

            # Emit completion event
            await action.emit_event(
                EventType.ACTION_COMPLETED,
                {
                    'action_id': action.context.id,
                    'action_type': action.__class__.__name__
                }
            )

        except Exception as e:
            # Handle failure
            await self._handle_action_failure(action, e)

    async def _handle_action_failure(self, action: Action, error: Exception) -> None:
        """Handle action failure with potential retry."""
        try:
            # Attempt rollback
            await action.rollback()

            if (action.context.retry_count < action.context.max_retries and
                not isinstance(error, ActionValidationError)):
                # Retry the action
                action.context.retry_count += 1
                await self._queue.put(
                    (action.context.priority.value, time.time(), action)
                )
                logger.info(
                    f"Retrying action {action.context.id} "
                    f"(attempt {action.context.retry_count})"
                )
            else:
                # Emit failure event
                await action.emit_event(
                    EventType.ACTION_FAILED,
                    {
                        'action_id': action.context.id,
                        'action_type': action.__class__.__name__,
                        'error': str(error)
                    }
                )

        except Exception as rollback_error:
            logger.error(
                f"Rollback failed for action {action.context.id}: {rollback_error}",
                exc_info=True
            )

    async def _wait_for_action(self, action_id: str, timeout: float = 30.0) -> None:
        """Wait for a specific action to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if action_id not in self._running:
                return
            await asyncio.sleep(0.1)
        logger.warning(f"Timeout waiting for action {action_id}")