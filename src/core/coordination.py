# src/core/coordination.py

from enum import Enum
from dataclasses import dataclass
from typing import Any, Coroutine, Dict, Optional, Callable, Set
import asyncio
import time
from ..utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MarketDataCommand(Enum):
    """Initial set of commands for market data service"""
    # Service Control
    CLEANUP_OLD_DATA = "cleanup_old_data"
    ADJUST_BATCH_SIZE = "adjust_batch_size"
    UPDATE_SYMBOL_STATE = "update_symbol_state"
    HANDLE_ERROR = "handle_error"

    # Synchronization
    PAUSE_SYNC = "pause_sync"
    RESUME_SYNC = "resume_sync"
    SYNC_ERROR = "sync_error"
    SYNC_SCHEDULED = "sync_scheduled"
    SYNC_COMPLETED = "sync_completed"

    # Collection
    COLLECTION_STARTED = "collection_started"
    COLLECTION_PROGRESS = "collection_progress"
    COLLECTION_COMPLETE = "collection_complete"
    COLLECTION_ERROR = "collection_error"
    SYMBOL_DELISTED = "symbol_delisted"

    EXCHANGE_ERROR = "exchange_error"
    MONITOR_ERROR = "monitor_error"
    GAP_DETECTED = "gap_detected"

@dataclass
class Command:
    """Command message for service coordination"""
    type: MarketDataCommand
    params: Dict[str, Any]
    priority: int = 0
    created_at: float = time.time()

CommandHandler = Callable[[Command], Coroutine[Any, Any, None]]

class ServiceCoordinator:
    """
    Initial implementation of service coordinator.
    Handles command routing and execution.
    """

    def __init__(self):
        self._handlers: Dict[MarketDataCommand, Set[CommandHandler]] = {}
        self._queue: asyncio.Queue[Command] = asyncio.Queue()
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def register_handler(self,
                             command_type: MarketDataCommand,
                             handler: CommandHandler) -> None:
        """Register a handler for a specific command type"""
        async with self._lock:
            if command_type not in self._handlers:
                self._handlers[command_type] = set()
            self._handlers[command_type].add(handler)
            logger.info(f"Registered handler for command: {command_type.value}")

    async def unregister_handler(self,
                                command_type: MarketDataCommand,
                                handler: CommandHandler) -> None:
        """Unregister a command handler"""
        async with self._lock:
            if command_type in self._handlers:
                self._handlers[command_type].discard(handler)
                logger.info(f"Unregistered handler for command: {command_type.value}")

    async def execute(self, command: Command) -> None:
        """Submit a command for execution"""
        await self._queue.put(command)
        logger.debug(f"Command queued: {command.type.value}")

    async def _process_command(self, command: Command) -> None:
        """Process a single command"""
        try:
            handlers = self._handlers.get(command.type, set())
            if not handlers:
                logger.warning(f"No handlers registered for command: {command.type.value}")
                return

            # Execute all registered handlers
            tasks = [
                asyncio.create_task(handler(command))
                for handler in handlers
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error processing command {command.type.value}: {e}")

    async def _worker(self) -> None:
        """Background worker for processing commands"""
        while self._running:
            try:
                command = await self._queue.get()
                await self._process_command(command)
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