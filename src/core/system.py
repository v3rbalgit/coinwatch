# src/core/system.py

import asyncio
import logging
from typing import Optional, Type, List, Callable
from .state import StateManager, SystemState
from .actions import Action, ActionManager, ActionRegistry
from .events import Event, EventType
from .exceptions import SystemError

logger = logging.getLogger(__name__)

class System:
    """
    Core system coordinator that manages state and actions.

    The System class serves as the central nervous system of the application,
    coordinating between the state manager and action manager, handling
    lifecycle events, and providing a unified interface for the application.
    """

    def __init__(self):
        """Initialize core system components."""
        self.state_manager = StateManager()
        self.action_registry = ActionRegistry()
        self.action_manager = ActionManager(
            state_manager=self.state_manager,
            registry=self.action_registry
        )
        self._shutdown_event = asyncio.Event()
        self._started = False
        self._health_check_task: Optional[asyncio.Task] = None

        # Subscribe to critical system events
        self.state_manager.subscribe(
            EventType.SYSTEM_INITIALIZED,
            self._handle_system_initialized
        )
        self.state_manager.subscribe(
            EventType.SYSTEM_SHUTDOWN,
            self._handle_system_shutdown
        )

        logger.info("System core initialized")

    async def start(self) -> None:
        """
        Start the system and all its components.

        This method initializes and starts both the state manager
        and action manager in the correct order, ensuring all
        components are properly initialized before the system
        is considered ready.
        """
        if self._started:
            logger.warning("System already started")
            return

        try:
            logger.info("Starting system...")

            # Start state manager first
            await self.state_manager.start()

            # Start action manager
            await self.action_manager.start()

            # Start health monitoring
            self._health_check_task = asyncio.create_task(
                self._health_check_loop()
            )

            self._started = True
            logger.info("System started successfully")

        except Exception as e:
            logger.error(f"Failed to start system: {e}", exc_info=True)
            await self._emergency_shutdown()
            raise SystemError("System startup failed") from e

    async def stop(self) -> None:
        """
        Gracefully stop the system and all its components.

        Ensures proper shutdown order and cleanup of resources.
        """
        if not self._started:
            return

        try:
            logger.info("Stopping system...")
            self._shutdown_event.set()

            # Stop health check
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass

            # Stop managers in reverse order
            await self.action_manager.stop()
            await self.state_manager.stop()

            self._started = False
            logger.info("System stopped successfully")

        except Exception as e:
            logger.error(f"Error during system shutdown: {e}", exc_info=True)
            raise SystemError("System shutdown failed") from e

    def register_action(self,
                       action_name: str,
                       action_class: Type[Action],
                       validators: Optional[List[Callable]] = None) -> None:
        """
        Register a new action type with the system.

        Args:
            action_name: Name to register the action under
            action_class: Action class to register
            validators: Optional list of validator functions
        """
        try:
            self.action_registry.register(
                action_name,
                action_class,
                validators
            )
            logger.debug(f"Registered action: {action_name}")
        except Exception as e:
            logger.error(f"Failed to register action {action_name}: {e}")
            raise

    async def _health_check_loop(self) -> None:
        """Periodic system health check."""
        while not self._shutdown_event.is_set():
            try:
                # Check component health
                state_health = await self._check_state_manager_health()
                action_health = await self._check_action_manager_health()

                if not all([state_health, action_health]):
                    logger.error("System health check failed")
                    await self.state_manager.emit_event(Event.create(
                        EventType.STATE_CHANGED,
                        {
                            'new_state': SystemState.ERROR.value,
                            'reason': 'health_check_failed'
                        }
                    ))

                await asyncio.sleep(30)  # Check every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check: {e}")
                await asyncio.sleep(5)  # Brief delay before retry

    async def _check_state_manager_health(self) -> bool:
        """Check state manager health."""
        try:
            return self.state_manager._running
        except Exception as e:
            logger.error(f"State manager health check failed: {e}")
            return False

    async def _check_action_manager_health(self) -> bool:
        """Check action manager health."""
        try:
            return self.action_manager._running_flag
        except Exception as e:
            logger.error(f"Action manager health check failed: {e}")
            return False

    async def _emergency_shutdown(self) -> None:
        """Emergency shutdown in case of critical failure."""
        try:
            logger.critical("Initiating emergency shutdown")

            # Cancel health check immediately
            if self._health_check_task:
                self._health_check_task.cancel()

            # Force stop managers
            await self.action_manager.stop()
            await self.state_manager.stop()

            self._started = False

        except Exception as e:
            logger.critical(f"Emergency shutdown failed: {e}", exc_info=True)

    async def _handle_system_initialized(self, event: Event) -> None:
        """Handle system initialization completion."""
        logger.info(
            f"System initialized: {event.data.get('initial_state', 'unknown')}"
        )

    async def _handle_system_shutdown(self, event: Event) -> None:
        """Handle system shutdown event."""
        logger.info(
            f"System shutdown: {event.data.get('final_state', 'unknown')}"
        )