# src/core/state.py

from enum import Enum
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field
import asyncio
import logging
import time
from .events import Event, EventType

logger = logging.getLogger(__name__)

class SystemState(Enum):
    """System state enumeration."""
    INITIALIZING = "initializing"
    READY = "ready"
    ACTION_IN_PROGRESS = "action_in_progress"
    ERROR = "error"
    SHUTTING_DOWN = "shutting_down"

@dataclass
class StateSnapshot:
    """Immutable snapshot of system state at a point in time."""
    current_state: SystemState
    service_health: Dict[str, str]
    running_actions: Set[str]
    last_event: Optional[Event] = None
    timestamp: float = field(default_factory=time.time)

class StateManager:
    """
    Central state manager for the system.
    Handles state transitions and event distribution.
    """

    def __init__(self):
        self._state = SystemState.INITIALIZING
        self._service_health: Dict[str, str] = {}
        self._running_actions: Set[str] = set()
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue()
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._last_event: Optional[Event] = None
        self._lock = asyncio.Lock()
        logger.info("State Manager initialized")

    @property
    def current_state(self) -> SystemState:
        """Get current system state."""
        return self._state

    async def start(self) -> None:
        """Start the state manager and begin processing events."""
        logger.info("Starting State Manager")
        asyncio.create_task(self._process_events())
        # Emit system initialized event
        await self.emit_event(Event.create(
            EventType.SYSTEM_INITIALIZED,
            data={'initial_state': self._state.value}
        ))

    async def stop(self) -> None:
        """Gracefully stop the state manager."""
        logger.info("Stopping State Manager")
        self._state = SystemState.SHUTTING_DOWN
        # Emit shutdown event
        await self.emit_event(Event.create(
            EventType.SYSTEM_SHUTDOWN,
            data={'final_state': self._state.value}
        ))

    async def emit_event(self, event: Event) -> None:
        """
        Emit a new event into the system.

        Args:
            event: The event to emit
        """
        await self._event_queue.put(event)
        logger.debug(f"Event emitted: {event.type.value}")

    def subscribe(self, event_type: EventType, handler: Callable) -> None:
        """
        Subscribe to events of a specific type.

        Args:
            event_type: Type of events to subscribe to
            handler: Async callback function to handle events
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed to {event_type.value}")

    def get_snapshot(self) -> StateSnapshot:
        """Get current state snapshot."""
        return StateSnapshot(
            current_state=self._state,
            service_health=self._service_health.copy(),
            running_actions=self._running_actions.copy(),
            last_event=self._last_event,
            timestamp=time.time()
        )

    async def _process_events(self) -> None:
        """Process events from the queue."""
        while True:
            try:
                event = await self._event_queue.get()
                self._last_event = event

                # Process state changes
                async with self._lock:
                    await self._handle_state_change(event)

                # Notify subscribers
                await self._notify_subscribers(event)

                self._event_queue.task_done()
                logger.debug(f"Event processed: {event.type.value}")

            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)
                if self._state != SystemState.ERROR:
                    self._state = SystemState.ERROR
                    # Emit error state event
                    await self.emit_event(Event.create(
                        EventType.STATE_CHANGED,
                        data={'new_state': SystemState.ERROR.value, 'error': str(e)}
                    ))

    async def _handle_state_change(self, event: Event) -> None:
        """
        Handle state changes based on events.

        Args:
            event: The event that might trigger state change
        """
        if event.type == EventType.SERVICE_HEALTH_CHANGED:
            service_name = event.data.get('service_name')
            health_status = event.data.get('status')
            if service_name and health_status:
                self._service_health[service_name] = health_status

        elif event.type == EventType.ACTION_STARTED:
            action_id = event.data.get('action_id')
            if action_id:
                self._running_actions.add(action_id)
                if self._state == SystemState.READY:
                    self._state = SystemState.ACTION_IN_PROGRESS

        elif event.type == EventType.ACTION_COMPLETED:
            action_id = event.data.get('action_id')
            if action_id:
                self._running_actions.discard(action_id)
                if not self._running_actions and self._state == SystemState.ACTION_IN_PROGRESS:
                    self._state = SystemState.READY

    async def _notify_subscribers(self, event: Event) -> None:
        """
        Notify all subscribers of an event.

        Args:
            event: The event to notify about
        """
        handlers = self._subscribers.get(event.type, [])
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}", exc_info=True)