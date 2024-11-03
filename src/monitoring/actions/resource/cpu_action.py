# src/monitoring/actions/resource/cpu_action.py

import psutil
from datetime import datetime
from typing import Dict, Any

from .base import ResourceAction
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


class CPURecoveryAction(ResourceAction):
    """Handle high CPU usage"""

    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate CPU-specific context"""
        try:
            if not await super().validate(context):
                return False

            cpu_metrics = context.get('cpu', {})
            required_fields = {'usage', 'count', 'load_avg'}

            if not all(field in cpu_metrics for field in required_fields):
                logger.error(f"Missing required CPU metrics: {required_fields}")
                return False

            if not isinstance(cpu_metrics['usage'], (int, float)):
                return False

            if not isinstance(cpu_metrics['load_avg'], (list, tuple)):
                return False

            return True
        except Exception as e:
            logger.error(f"CPU validation error: {e}")
            return False

    async def _execute_action(self, context: Dict[str, Any]) -> bool:
        try:
            cpu_metrics = context.get('cpu', {})
            cpu_usage = cpu_metrics.get('usage', 0)

            if cpu_usage > self.config.cpu_critical_threshold:
                await self._handle_critical_cpu(cpu_metrics)
            elif cpu_usage > self.config.cpu_warning_threshold:
                await self._handle_high_cpu(cpu_metrics)
            else:
                return True  # No action needed

            # Verify the action had an effect
            new_usage = psutil.cpu_percent(interval=1) / 100
            return new_usage < cpu_usage

        except Exception as e:
            logger.error(f"CPU recovery failed: {e}")
            return False

    async def _handle_high_cpu(self, metrics: Dict[str, Any]) -> None:
        """Handle high CPU usage"""
        logger.info(f"Handling high CPU usage: {metrics}")

        # Log CPU-hungry processes
        self._log_cpu_processes()

        # Suggest throttling if supported by the application
        await self._suggest_throttling(aggressive=False)

    async def _handle_critical_cpu(self, metrics: Dict[str, Any]) -> None:
        """Handle critical CPU usage"""
        logger.warning(f"Handling critical CPU usage: {metrics}")

        # Log detailed CPU information
        self._log_cpu_processes(detailed=True)

        # Suggest aggressive throttling
        await self._suggest_throttling(aggressive=True)

        # Consider emergency measures
        await self._emergency_cpu_measures()

    def _log_cpu_processes(self, detailed: bool = False) -> None:
        """Log CPU usage by process with optional details"""
        processes = []
        proc_attrs = ['pid', 'name', 'cpu_percent']
        if detailed:
            proc_attrs.extend(['username', 'create_time', 'num_threads', 'cpu_times'])

        for proc in psutil.process_iter(proc_attrs):
            try:
                proc_info = proc.info
                if proc_info['cpu_percent'] > 1.0:
                    processes.append(proc_info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
        top_processes = processes[:10]

        logger.info("Top CPU-consuming processes:")
        for proc in top_processes:
            base_info = (
                f"PID: {proc['pid']}, Name: {proc['name']}, "
                f"CPU: {proc['cpu_percent']:.1f}%"
            )

            if detailed:
                try:
                    created = datetime.fromtimestamp(proc['create_time']).strftime('%Y-%m-%d %H:%M:%S')
                    cpu_times = proc.get('cpu_times', {})
                    detailed_info = (
                        f"\n  User: {proc.get('username', 'unknown')}"
                        f"\n  Created: {created}"
                        f"\n  Threads: {proc.get('num_threads', 0)}"
                        f"\n  User Time: {getattr(cpu_times, 'user', 0):.1f}s"
                        f"\n  System Time: {getattr(cpu_times, 'system', 0):.1f}s"
                    )
                    logger.info(f"{base_info}{detailed_info}")
                except (AttributeError, KeyError):
                    logger.info(base_info)
            else:
                logger.info(base_info)

    async def _suggest_throttling(self, aggressive: bool = False) -> None:
        """Suggest throttling measures"""
        if aggressive:
            logger.warning(
                "Suggesting aggressive CPU throttling:\n"
                "- Increase sync intervals\n"
                "- Reduce batch sizes\n"
                "- Pause non-critical tasks"
            )
        else:
            logger.info(
                "Suggesting CPU throttling:\n"
                "- Consider increasing intervals\n"
                "- Consider reducing batch sizes"
            )

    async def _emergency_cpu_measures(self) -> None:
        """Emergency measures for critical CPU usage"""
        logger.warning("Initiating emergency CPU measures")

        # Log system load
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count()

        logger.warning(
            f"System load: {load1:.1f} (1m), {load5:.1f} (5m), {load15:.1f} (15m)\n"
            f"Per CPU: {load1/cpu_count:.1f} (1m), {load5/cpu_count:.1f} (5m)"
        )
