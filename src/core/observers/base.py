# src/core/observers/base.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import asyncio
from dataclasses import dataclass
from enum import Enum
from src.core.events import Event, EventType
from src.core.state import StateManager
from src.core.actions import ActionManager
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class ObserverState(Enum):
    """States that an observer can be in."""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    STOPPED = "stopped"

@dataclass
class ObservationContext:
    """Context for each observation cycle."""
    timestamp: float
    previous_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    changes_detected: bool = False
    error: Optional[Exception] = None

class Observer(ABC):
    """
    Base class for all observers in the system.

    Implements the Observer pattern with async support and proper
    state management. Observers can watch system components and
    trigger appropriate actions and events.
    """

    def __init__(self,
                 name: str,
                 state_manager: StateManager,
                 action_manager: ActionManager,
                 observation_interval: float = 30.0):
        self.name = name
        self.state_manager = state_manager
        self.action_manager = action_manager
        self.observation_interval = observation_interval
        self._state = ObserverState.INITIALIZING
        self._last_observation: Optional[ObservationContext] = None
        self._observation_task: Optional[asyncio.Task] = None
        self._running = False
        self._pause_event = asyncio.Event()
        self._metrics: Dict[str, Any] = {}

    async def start(self) -> None:
        """Start the observer."""
        if self._running:
            return

        self._running = True
        self._state = ObserverState.RUNNING
        self._observation_task = asyncio.create_task(self._observation_loop())

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_STARTED,
            {
                'service_name': self.name,
                'service_type': 'observer',
                'state': self._state.value
            }
        ))

        logger.info(f"Observer {self.name} started")

    async def stop(self) -> None:
        """Stop the observer gracefully."""
        if not self._running:
            return

        self._running = False
        self._state = ObserverState.STOPPED

        if self._observation_task:
            try:
                await asyncio.wait_for(self._observation_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for {self.name} observation to complete")

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_STOPPED,
            {
                'service_name': self.name,
                'service_type': 'observer',
                'state': self._state.value
            }
        ))

        logger.info(f"Observer {self.name} stopped")

    async def pause(self) -> None:
        """Pause observations temporarily."""
        self._state = ObserverState.PAUSED
        self._pause_event.set()

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_HEALTH_CHANGED,
            {
                'service_name': self.name,
                'status': 'paused',
                'state': self._state.value
            }
        ))

    async def resume(self) -> None:
        """Resume paused observations."""
        self._state = ObserverState.RUNNING
        self._pause_event.clear()

        await self.state_manager.emit_event(Event.create(
            EventType.SERVICE_HEALTH_CHANGED,
            {
                'service_name': self.name,
                'status': 'running',
                'state': self._state.value
            }
        ))

    async def get_metrics(self) -> Dict[str, Any]:
        """Get current observer metrics."""
        return {
            'name': self.name,
            'state': self._state.value,
            'last_observation': self._last_observation.timestamp if self._last_observation else None,
            'metrics': self._metrics,
            'running': self._running
        }

    @abstractmethod
    async def observe(self) -> ObservationContext:
        """
        Perform a single observation cycle.

        Returns:
            ObservationContext: Context containing observation results
        """
        pass

    @abstractmethod
    async def analyze(self, context: ObservationContext) -> None:
        """
        Analyze observation results and determine if actions are needed.

        Args:
            context: The observation context to analyze
        """
        pass

    @abstractmethod
    async def handle_error(self, error: Exception) -> None:
        """
        Handle observation errors.

        Args:
            error: The error that occurred
        """
        pass

    async def _observation_loop(self) -> None:
        """Main observation loop."""
        while self._running:
            try:
                # Check for pause
                if self._state == ObserverState.PAUSED:
                    await self._pause_event.wait()
                    continue

                # Perform observation
                context = await self.observe()
                self._last_observation = context

                # Analyze results if no error
                if not context.error:
                    await self.analyze(context)
                else:
                    await self.handle_error(context.error)

                # Update metrics
                self._metrics.update({
                    'last_observation_time': context.timestamp,
                    'state': self._state.value,
                    'error': str(context.error) if context.error else None
                })

                # Emit metrics event
                await self.state_manager.emit_event(Event.create(
                    EventType.SERVICE_HEALTH_CHANGED,
                    {
                        'service_name': self.name,
                        'metrics': await self.get_metrics()
                    }
                ))

                # Wait for next observation cycle
                await asyncio.sleep(self.observation_interval)

            except Exception as e:
                logger.error(f"Error in {self.name} observation loop: {e}", exc_info=True)
                self._state = ObserverState.ERROR
                await self.handle_error(e)
                await asyncio.sleep(self.observation_interval)