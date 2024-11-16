# src/core/coordination.py

from enum import Enum
from dataclasses import dataclass
from typing import Any, Coroutine, Dict, Optional, Callable, Set, Union, overload, TypeVar
import asyncio
import time
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

T = TypeVar('T')

class MarketDataCommand(Enum):
    """Initial set of commands for market data service"""

    # Synchronization
    SYNC_ERROR = "sync_error"
    SYNC_SCHEDULED = "sync_scheduled"
    SYNC_COMPLETE = "sync_complete"

    # Collection
    COLLECTION_START = "collection_started"
    COLLECTION_COMPLETE = "collection_complete"
    COLLECTION_ERROR = "collection_error"
    SYMBOL_ADDED = "symbol_added"
    SYMBOL_DELISTED = "symbol_delisted"
    GAP_DETECTED = "gap_detected"

class MonitoringCommand(Enum):
    """Monitoring service commands"""
    REPORT_METRICS = "report_metrics"
    SYSTEM_METRICS = "system_metrics"

@dataclass
class Command:
    """Command message for service coordination"""
    type: Union[MarketDataCommand, MonitoringCommand]
    params: Dict[str, Any]
    priority: int = 0
    created_at: float = time.time()
    expects_response: bool = False

class CommandResult:
    """Wrapper for command execution results"""
    def __init__(self, is_success: bool, data: Any = None, error_message: Optional[str] = None):
        self.is_success = is_success
        self.data = data
        self.error_message = error_message

    @classmethod
    def success(cls, data: Any = None) -> 'CommandResult':
        """Create a successful result"""
        return cls(is_success=True, data=data)

    @classmethod
    def error(cls, error_message: str) -> 'CommandResult':
        """Create an error result"""
        return cls(is_success=False, error_message=error_message)

    def __str__(self) -> str:
        if self.is_success:
            return f"CommandResult(success, data={self.data})"
        return f"CommandResult(error, message={self.error_message})"

CommandHandler = Callable[[Command], Coroutine[Any, Any, Optional[CommandResult]]]

class ServiceCoordinator:
    """Enhanced service coordinator with support for command responses."""

    def __init__(self):
        self._handlers: Dict[Union[MarketDataCommand, MonitoringCommand], Set[CommandHandler]] = {}
        self._queue: asyncio.Queue[tuple[Command, Optional[asyncio.Future[CommandResult]]]] = asyncio.Queue()
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def register_handler(self,
                             command_type: Union[MarketDataCommand, MonitoringCommand],
                             handler: CommandHandler) -> None:
        """Register a handler for a specific command type"""
        async with self._lock:
            if command_type not in self._handlers:
                self._handlers[command_type] = set()
            self._handlers[command_type].add(handler)
            logger.info(f"Registered handler for command: {command_type.value}")

    async def unregister_handler(self,
                                command_type: Union[MarketDataCommand, MonitoringCommand],
                                handler: CommandHandler) -> None:
        """Unregister a command handler"""
        async with self._lock:
            if command_type in self._handlers:
                self._handlers[command_type].discard(handler)
                logger.info(f"Unregistered handler for command: {command_type.value}")

    @overload
    async def execute(self, command: Command) -> None:
        ...

    @overload
    async def execute(self, command: Command) -> CommandResult:
        ...

    async def execute(self, command: Command) -> Optional[CommandResult]:
        """
        Execute a command and optionally wait for response.

        Args:
            command: Command to execute

        Returns:
            CommandResult if command.expects_response is True, None otherwise
        """
        if command.expects_response:
            response_future: asyncio.Future[CommandResult] = asyncio.Future()
            await self._queue.put((command, response_future))
            try:
                return await response_future
            except asyncio.CancelledError:
                logger.warning(f"Command response cancelled: {command.type.value}")
                return CommandResult.error("Command cancelled")
        else:
            await self._queue.put((command, None))
            return None

    async def _process_command(self,
                             command: Command,
                             response_future: Optional[asyncio.Future[CommandResult]]) -> None:
        """Process a single command"""
        try:
            handlers = self._handlers.get(command.type, set())
            if not handlers:
                logger.warning(f"No handlers registered for command: {command.type.value}")
                if response_future is not None and not response_future.done():
                    response_future.set_result(CommandResult.error("No handlers registered"))
                return

            if command.expects_response:
                # We know response_future is not None when expects_response is True
                assert response_future is not None, "Response future must be set when expecting response"

                # For commands expecting response, run handlers until we get a result
                for handler in handlers:
                    try:
                        result = await handler(command)
                        if result and not response_future.done():
                            response_future.set_result(result)
                            break
                    except Exception as e:
                        logger.error(f"Handler error for {command.type.value}: {e}")

                # If no handler provided a result, set error
                if not response_future.done():
                    response_future.set_result(CommandResult.error("No handler provided result"))
            else:
                # Execute all handlers for fire-and-forget commands
                tasks = [
                    asyncio.create_task(handler(command))
                    for handler in handlers
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error processing command {command.type.value}: {e}")
            if response_future is not None and not response_future.done():
                response_future.set_result(CommandResult.error(str(e)))

    async def _worker(self) -> None:
        """Background worker for processing commands"""
        while self._running:
            try:
                command, response_future = await self._queue.get()
                await self._process_command(command, response_future)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in command worker: {e}")
                await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the coordinator"""
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker())
            logger.info("Service coordinator started")

    async def stop(self) -> None:
        """Stop the coordinator"""
        if self._running:
            self._running = False
            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
            logger.info("Service coordinator stopped")