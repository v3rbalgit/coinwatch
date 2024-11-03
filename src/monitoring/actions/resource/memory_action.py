# src/monitoring/actions/resource/memory_action.py

import asyncio
import gc
import psutil
from datetime import datetime
from typing import Dict, Any

from .base import ResourceAction
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

class MemoryRecoveryAction(ResourceAction):
    """Handle high memory usage"""

    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate memory-specific context"""
        try:
            if not await super().validate(context):
                return False

            memory_metrics = context.get('memory', {})
            required_fields = {'usage', 'available_mb', 'total_mb'}

            if not all(field in memory_metrics for field in required_fields):
                logger.error(f"Missing required memory metrics: {required_fields}")
                return False

            if not isinstance(memory_metrics['usage'], (int, float)):
                return False

            return True
        except Exception as e:
            logger.error(f"Memory validation error: {e}")
            return False

    async def _execute_action(self, context: Dict[str, Any]) -> bool:
        try:
            memory_metrics = context.get('memory', {})
            memory_usage = memory_metrics.get('usage', 0)

            if memory_usage > self.config.memory_critical_threshold:
                await self._handle_critical_memory(memory_metrics)
            elif memory_usage > self.config.memory_warning_threshold:
                await self._handle_high_memory(memory_metrics)
            else:
                return True  # No action needed

            # Verify the action had an effect
            new_usage = psutil.virtual_memory().percent / 100
            return new_usage < memory_usage

        except Exception as e:
            logger.error(f"Memory recovery failed: {e}")
            return False

    async def _handle_high_memory(self, metrics: Dict[str, Any]) -> None:
        """Handle high memory usage"""
        logger.info(f"Handling high memory usage: {metrics}")

        # Force garbage collection
        gc.collect()
        await asyncio.sleep(1)  # Let GC complete

        # Log memory-hungry processes
        self._log_memory_processes()

    async def _handle_critical_memory(self, metrics: Dict[str, Any]) -> None:
        """Handle critical memory usage"""
        logger.warning(f"Handling critical memory usage: {metrics}")

        # Aggressive garbage collection
        for _ in range(3):
            gc.collect()
            await asyncio.sleep(1)

        # Log detailed memory information
        self._log_memory_processes(detailed=True)

        # Consider emergency measures
        await self._emergency_memory_measures()

    def _log_memory_processes(self, detailed: bool = False) -> None:
        """Log memory usage by process with optional details"""
        processes = []
        proc_attrs = ['pid', 'name', 'memory_percent']
        if detailed:
            proc_attrs.extend(['username', 'create_time', 'memory_info'])

        for proc in psutil.process_iter(proc_attrs):
            try:
                proc_info = proc.info
                if proc_info['memory_percent'] > 1.0:
                    processes.append(proc_info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        processes.sort(key=lambda x: x['memory_percent'], reverse=True)
        top_processes = processes[:10]

        logger.info("Top memory-consuming processes:")
        for proc in top_processes:
            base_info = (
                f"PID: {proc['pid']}, Name: {proc['name']}, "
                f"Memory: {proc['memory_percent']:.1f}%"
            )

            if detailed:
                try:
                    created = datetime.fromtimestamp(proc['create_time']).strftime('%Y-%m-%d %H:%M:%S')
                    memory_mb = proc.get('memory_info', {}).get('rss', 0) / (1024 * 1024)
                    detailed_info = (
                        f"\n  User: {proc.get('username', 'unknown')}"
                        f"\n  Created: {created}"
                        f"\n  Memory (MB): {memory_mb:.1f}"
                    )
                    logger.info(f"{base_info}{detailed_info}")
                except (AttributeError, KeyError):
                    logger.info(base_info)
            else:
                logger.info(base_info)

    async def _emergency_memory_measures(self) -> None:
        """Emergency measures for critical memory usage"""
        # Log memory state before emergency measures
        logger.warning("Initiating emergency memory measures")
        memory_before = psutil.virtual_memory()

        # Clear Python's internal memory pools
        gc.collect()
        gc.collect()
        gc.collect()

        # Log memory state after measures
        memory_after = psutil.virtual_memory()
        freed_memory = (memory_before.percent - memory_after.percent)
        logger.info(f"Emergency measures freed {freed_memory:.1f}% memory")
