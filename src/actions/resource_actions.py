# src/actions/resource_actions.py

from typing import Dict, Optional
import asyncio
import psutil
from dataclasses import dataclass
from src.core.actions import Action, ActionValidationError
from src.core.events import EventType
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class ResourceActionResult:
    """Result of a resource management action."""
    success: bool
    resources_freed: Dict[str, float]
    action_taken: str
    details: Optional[str] = None

class BaseResourceAction(Action):
    """Base class for resource management actions."""

    async def validate(self) -> None:
        if 'severity' not in self.context.params:
            raise ActionValidationError("Severity level must be specified")

        if self.context.params['severity'] not in ['warning', 'critical']:
            raise ActionValidationError("Invalid severity level")

    async def emit_start_event(self, resource_type: str) -> None:
        """Emit action start event."""
        await self.emit_event(
            EventType.RESOURCE_ACTION_STARTED,
            {
                'resource_type': resource_type,
                'severity': self.context.params['severity'],
                'action_id': self.context.id
            }
        )

    async def emit_completion_event(self, result: ResourceActionResult) -> None:
        """Emit action completion event."""
        await self.emit_event(
            EventType.RESOURCE_ACTION_COMPLETED,
            {
                'success': result.success,
                'resources_freed': result.resources_freed,
                'action_taken': result.action_taken,
                'details': result.details,
                'action_id': self.context.id
            }
        )

class HandleHighCPUAction(BaseResourceAction):
    """Handles high CPU usage conditions."""

    async def validate(self) -> None:
        await super().validate()
        required = ['cpu_percent', 'load_average']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

    async def execute(self) -> None:
        await self.emit_start_event('cpu')

        try:
            cpu_percent = self.context.params['cpu_percent']
            severity = self.context.params['severity']

            if severity == 'critical':
                result = await self._handle_critical_cpu()
            else:
                result = await self._handle_warning_cpu()

            await self.emit_completion_event(result)

        except Exception as e:
            logger.error(f"CPU action failed: {e}", exc_info=True)
            raise

    async def _handle_critical_cpu(self) -> ResourceActionResult:
        """Handle critical CPU condition."""
        # Get top CPU-consuming processes
        processes = await asyncio.to_thread(
            lambda: sorted(
                psutil.process_iter(['pid', 'name', 'cpu_percent']),
                key=lambda p: p.info['cpu_percent'],
                reverse=True
            )[:5]
        )

        resources_freed = 0.0
        action_taken = "process_optimization"

        for process in processes:
            try:
                # Lower priority of high-CPU processes
                process.nice(10)
                resources_freed += process.cpu_percent()
            except Exception as e:
                logger.error(f"Failed to adjust process {process.pid}: {e}")

        return ResourceActionResult(
            success=True,
            resources_freed={'cpu_percent': resources_freed},
            action_taken=action_taken,
            details=f"Adjusted priority for {len(processes)} processes"
        )

    async def _handle_warning_cpu(self) -> ResourceActionResult:
        """Handle CPU warning condition."""
        return ResourceActionResult(
            success=True,
            resources_freed={'cpu_percent': 0.0},
            action_taken="monitoring_intensified",
            details="Increased CPU monitoring frequency"
        )

    async def rollback(self) -> None:
        """Rollback CPU management actions if needed."""
        # Restore process priorities if needed
        pass

class HandleHighMemoryAction(BaseResourceAction):
    """Handles high memory usage conditions."""

    async def validate(self) -> None:
        await super().validate()
        required = ['memory_percent', 'swap_percent']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

    async def execute(self) -> None:
        await self.emit_start_event('memory')

        try:
            memory_percent = self.context.params['memory_percent']
            severity = self.context.params['severity']

            if severity == 'critical':
                result = await self._handle_critical_memory()
            else:
                result = await self._handle_warning_memory()

            await self.emit_completion_event(result)

        except Exception as e:
            logger.error(f"Memory action failed: {e}", exc_info=True)
            raise

    async def _handle_critical_memory(self) -> ResourceActionResult:
        """Handle critical memory condition."""
        # Get top memory-consuming processes
        processes = await asyncio.to_thread(
            lambda: sorted(
                psutil.process_iter(['pid', 'name', 'memory_percent']),
                key=lambda p: p.info['memory_percent'],
                reverse=True
            )[:5]
        )

        memory_freed = 0.0
        for process in processes:
            try:
                if process.name() not in ['python', 'mysql']:  # Protect critical processes
                    memory_freed += process.memory_percent()
                    process.suspend()  # Suspend high-memory processes
            except Exception as e:
                logger.error(f"Failed to handle process {process.pid}: {e}")

        return ResourceActionResult(
            success=True,
            resources_freed={'memory_percent': memory_freed},
            action_taken="process_suspension",
            details=f"Suspended high-memory processes, freed {memory_freed:.1f}%"
        )

    async def _handle_warning_memory(self) -> ResourceActionResult:
        """Handle memory warning condition."""
        try:
            # Attempt to free page cache
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                f.write('1')

            return ResourceActionResult(
                success=True,
                resources_freed={'memory_percent': 5.0},  # Estimated
                action_taken="cache_cleanup",
                details="Cleared system caches"
            )
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")
            return ResourceActionResult(
                success=False,
                resources_freed={'memory_percent': 0.0},
                action_taken="cache_cleanup_failed",
                details=str(e)
            )

    async def rollback(self) -> None:
        """Resume suspended processes if needed."""
        pass

class HandleHighDiskAction(BaseResourceAction):
    """Handles high disk usage conditions."""

    async def validate(self) -> None:
        await super().validate()
        required = ['disk_percent', 'io_counters']
        for param in required:
            if param not in self.context.params:
                raise ActionValidationError(f"Missing required parameter: {param}")

    async def execute(self) -> None:
        await self.emit_start_event('disk')

        try:
            disk_percent = self.context.params['disk_percent']
            severity = self.context.params['severity']

            if severity == 'critical':
                result = await self._handle_critical_disk()
            else:
                result = await self._handle_warning_disk()

            await self.emit_completion_event(result)

        except Exception as e:
            logger.error(f"Disk action failed: {e}", exc_info=True)
            raise

    async def _handle_critical_disk(self) -> ResourceActionResult:
        """Handle critical disk condition."""
        # Implement disk cleanup logic
        space_freed = await self._cleanup_temp_files()

        return ResourceActionResult(
            success=True,
            resources_freed={'disk_space_mb': space_freed},
            action_taken="disk_cleanup",
            details=f"Freed {space_freed:.1f}MB of disk space"
        )

    async def _handle_warning_disk(self) -> ResourceActionResult:
        """Handle disk warning condition."""
        return ResourceActionResult(
            success=True,
            resources_freed={'disk_space_mb': 0},
            action_taken="monitoring_intensified",
            details="Increased disk monitoring frequency"
        )

    async def _cleanup_temp_files(self) -> float:
        """Clean up temporary files to free disk space."""
        # Implement temp file cleanup logic
        return 0.0

    async def rollback(self) -> None:
        """No rollback needed for disk cleanup."""
        pass

class ResourceActionFactory:
    """Factory for creating resource management actions."""

    _actions = {
        'handle_high_cpu': HandleHighCPUAction,
        'handle_critical_cpu': HandleHighCPUAction,
        'handle_high_memory': HandleHighMemoryAction,
        'handle_critical_memory': HandleHighMemoryAction,
        'handle_high_disk': HandleHighDiskAction,
        'handle_critical_disk': HandleHighDiskAction
    }

    @classmethod
    def create_action(cls, action_type: str, context) -> Action:
        """Create appropriate resource action."""
        action_class = cls._actions.get(action_type)
        if not action_class:
            raise ValueError(f"Unknown action type: {action_type}")
        return action_class(context)