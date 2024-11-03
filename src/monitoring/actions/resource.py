# src/monitoring/actions/resource.py

import os
import shutil
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import asyncio
import gc
import psutil
from datetime import datetime

from .base import Action
from ...utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)

@dataclass
class ResourceActionConfig:
    """Configuration for resource actions"""
    # Memory thresholds
    memory_warning_threshold: float = 0.75
    memory_critical_threshold: float = 0.85

    # CPU thresholds
    cpu_warning_threshold: float = 0.70
    cpu_critical_threshold: float = 0.80

    # Action settings
    max_retries: int = 3
    retry_delay: float = 5.0
    cooldown_period: int = 300  # 5 minutes

    # Disk thresholds
    disk_warning_threshold: float = 0.80
    disk_critical_threshold: float = 0.90

    # Disk cleanup settings
    temp_file_max_age: int = 7 * 24 * 3600  # 7 days in seconds
    log_file_max_age: int = 30 * 24 * 3600  # 30 days in seconds
    min_free_space_gb: float = 10.0         # Minimum 10GB free
    protected_paths: Set[str] = field(
        default_factory=lambda: {
            '/boot', '/etc', '/bin', '/sbin',
            '/usr/bin', '/usr/sbin'
        }
    )         # Paths to never clean

    def __post_init__(self):
        if self.protected_paths is None:
            self.protected_paths = {
                '/boot', '/etc', '/bin', '/sbin',
                '/usr/bin', '/usr/sbin'
            }

class ResourceAction(Action):
    """Base class for resource-related actions"""

    def __init__(self, config: Optional[ResourceActionConfig]):
        self.config = config or ResourceActionConfig()
        self._last_execution: Dict[str, datetime] = {}

    async def execute(self, context: Dict[str, Any]) -> bool:
        """Execute the resource action with retries and timeout protection"""
        if not await self._check_cooldown():
            logger.info("Action in cooldown period, skipping execution")
            return False

        retries = 0
        while retries < self.config.max_retries:
            try:
                # Add timeout protection for the execution
                async with asyncio.timeout(300):  # 5 minute timeout
                    if await self._execute_action(context):
                        self._last_execution[self.__class__.__name__] = datetime.now()
                        return True

                retries += 1
                if retries < self.config.max_retries:
                    delay = self.config.retry_delay * (2 ** retries)  # Exponential backoff
                    logger.info(f"Action retry {retries}/{self.config.max_retries} after {delay}s")
                    await asyncio.sleep(delay)

            except asyncio.TimeoutError:
                logger.error(f"{self.__class__.__name__} action timed out")
                retries += 1
                if retries < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)

            except Exception as e:
                logger.error(f"Action execution failed: {e}", exc_info=True)
                retries += 1
                if retries < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay)

        logger.error(f"Action failed after {self.config.max_retries} retries")
        return False

    async def _check_cooldown(self) -> bool:
        """Check if action is in cooldown period"""
        last_exec = self._last_execution.get(self.__class__.__name__)
        if last_exec:
            elapsed = (datetime.now() - last_exec).total_seconds()
            return elapsed >= self.config.cooldown_period
        return True

    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate resource context"""
        try:
            if not isinstance(context, dict):
                return False
            if not context.get('source') == 'resource':
                return False
            if not isinstance(context.get('metrics'), dict):
                return False
            return True
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    async def _execute_action(self, context: Dict[str, Any]) -> bool:
        """Template method for resource action execution.

        Args:
            context: Dictionary containing:
                - metrics: Current resource metrics
                - thresholds: Configured thresholds
                - source: Resource type (memory, cpu, disk)

        Returns:
            bool: True if action was successful, False otherwise

        Implementation requirements:
        - Must handle missing context values gracefully
        - Should log action outcomes
        - Should verify action effectiveness
        """
        raise NotImplementedError(
            f"Action {self.__class__.__name__} must implement _execute_action"
        )

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

class DiskRecoveryAction(ResourceAction):
    """Handle disk space issues"""

    def __init__(self, config: Optional[ResourceActionConfig] = None):
        super().__init__(config)
        self._temp_files: Set[str] = set()  # Track files for cleanup

    async def validate(self, context: Dict[str, Any]) -> bool:
        """Validate disk-specific context"""
        try:
            if not await super().validate(context):
                return False

            disk_metrics = context.get('disk', {})
            required_fields = {'usage', 'free_gb', 'total_gb'}

            if not all(field in disk_metrics for field in required_fields):
                logger.error(f"Missing required disk metrics: {required_fields}")
                return False

            if not isinstance(disk_metrics['usage'], (int, float)):
                return False

            if disk_metrics['total_gb'] <= 0:
                return False

            return True
        except Exception as e:
            logger.error(f"Disk validation error: {e}")
            return False

    async def _execute_action(self, context: Dict[str, Any]) -> bool:
        try:
            disk_metrics = context.get('disk', {})
            disk_usage = disk_metrics.get('usage', 0)

            if disk_usage > self.config.disk_critical_threshold:
                await self._handle_critical_disk(disk_metrics)
            elif disk_usage > self.config.disk_warning_threshold:
                await self._handle_high_disk(disk_metrics)
            else:
                return True  # No action needed

            # Verify the action had an effect
            new_usage = psutil.disk_usage('/').percent / 100
            return new_usage < disk_usage

        except Exception as e:
            logger.error(f"Disk recovery failed: {e}")
            return False

    async def _handle_high_disk(self, metrics: Dict[str, Any]) -> None:
        """Handle high disk usage"""
        logger.info(f"Handling high disk usage: {metrics}")

        # Log disk space information
        self._log_disk_usage()

        # Clean temporary files
        cleaned = await self._clean_temp_files()
        logger.info(f"Cleaned {cleaned:.2f}GB of temporary files")

        # Clean old log files
        cleaned = await self._clean_log_files()
        logger.info(f"Cleaned {cleaned:.2f}GB of old log files")

    async def _handle_critical_disk(self, metrics: Dict[str, Any]) -> None:
        """Handle critical disk usage"""
        logger.warning(f"Handling critical disk usage: {metrics}")

        # Log detailed disk information
        self._log_disk_usage(detailed=True)

        # Aggressive cleanup
        cleaned = await self._emergency_disk_cleanup()
        logger.warning(f"Emergency cleanup freed {cleaned:.2f}GB")

        # Check if we need more drastic measures
        if psutil.disk_usage('/').percent > 95:
            await self._critical_disk_measures()

    def _log_disk_usage(self, detailed: bool = False) -> None:
        """Log disk usage information"""
        disk_info = psutil.disk_usage('/')
        logger.info(
            f"Disk usage: {disk_info.percent}% "
            f"(Free: {disk_info.free / 1e9:.1f}GB, "
            f"Total: {disk_info.total / 1e9:.1f}GB)"
        )

        if detailed:
            # Log usage by directory
            logger.info("Disk usage by directory:")
            for path in ['/var', '/tmp', '/home', '/var/log']:
                if os.path.exists(path):
                    try:
                        size = self._get_dir_size(path)
                        logger.info(f"{path}: {size / 1e9:.1f}GB")
                    except OSError as e:
                        logger.error(f"Error getting size of {path}: {e}")

    def _get_dir_size(self, path: str) -> int:
        """Get directory size in bytes"""
        total = 0
        with os.scandir(path) as it:
            for entry in it:
                try:
                    if entry.is_file():
                        total += entry.stat().st_size
                    elif entry.is_dir():
                        total += self._get_dir_size(entry.path)
                except OSError as e:
                    logger.error(f"Error accessing {entry.path}: {e}")
        return total

    async def _clean_temp_files(self) -> float:
        """Clean temporary files, return GB cleaned"""
        cleaned_bytes = 0
        temp_dirs = ['/tmp', '/var/tmp']
        current_time = time.time()

        for temp_dir in temp_dirs:
            if not os.path.exists(temp_dir):
                return 0.0

            self._temp_files.add(entry.path)

            try:
                with os.scandir(temp_dir) as it:
                    for entry in it:
                        try:
                            if entry.is_file():
                                stats = entry.stat()
                                age = current_time - stats.st_mtime
                                if age > self.config.temp_file_max_age:
                                    cleaned_bytes += stats.st_size
                                    os.unlink(entry.path)
                        except OSError as e:
                            logger.error(f"Error cleaning {entry.path}: {e}")
            except OSError as e:
                logger.error(f"Error accessing {temp_dir}: {e}")
            finally:
                # Cleanup tracked files
                for path in self._temp_files:
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except OSError:
                        pass
                self._temp_files.clear()

        return cleaned_bytes / 1e9  # Convert to GB

    async def _clean_log_files(self) -> float:
        """Clean old log files, return GB cleaned"""
        cleaned_bytes = 0
        log_dirs = ['/var/log']
        current_time = time.time()

        for log_dir in log_dirs:
            if not os.path.exists(log_dir):
                continue

            for root, _, files in os.walk(log_dir):
                for file in files:
                    if file.endswith(('.log', '.gz')):
                        file_path = os.path.join(root, file)
                        try:
                            stats = os.stat(file_path)
                            age = current_time - stats.st_mtime
                            if age > self.config.log_file_max_age:
                                cleaned_bytes += stats.st_size
                                os.unlink(file_path)
                        except OSError as e:
                            logger.error(f"Error cleaning {file_path}: {e}")

        return cleaned_bytes / 1e9  # Convert to GB

    async def _emergency_disk_cleanup(self) -> float:
        """Emergency disk cleanup, return GB cleaned"""
        cleaned_total = 0.0

        # Aggressive temporary file cleanup (all files older than 1 day)
        self.config.temp_file_max_age = 24 * 3600  # 1 day
        cleaned_total += await self._clean_temp_files()

        # Aggressive log cleanup (all files older than 7 days)
        self.config.log_file_max_age = 7 * 24 * 3600  # 7 days
        cleaned_total += await self._clean_log_files()

        # Clean package manager caches if available
        cleaned_total += await self._clean_package_caches()

        return cleaned_total

    async def _clean_package_caches(self) -> float:
        """Clean package manager caches, return GB cleaned"""
        cleaned_bytes = 0

        # APT cache
        apt_cache = '/var/cache/apt/archives'
        if os.path.exists(apt_cache):
            cleaned_bytes += self._get_dir_size(apt_cache)
            try:
                shutil.rmtree(apt_cache)
                os.makedirs(apt_cache)
            except OSError as e:
                logger.error(f"Error cleaning APT cache: {e}")

        return cleaned_bytes / 1e9  # Convert to GB

    async def _critical_disk_measures(self) -> None:
        """Handle critically low disk space"""
        logger.critical("Initiating critical disk space measures")

        # Find largest files
        largest_files = self._find_largest_files('/', limit=10)
        logger.warning("Largest files found:")
        for size, path in largest_files:
            logger.warning(f"{path}: {size / 1e9:.1f}GB")

        # Find largest directories
        largest_dirs = self._find_largest_directories('/', limit=10)
        logger.warning("Largest directories found:")
        for size, path in largest_dirs:
            logger.warning(f"{path}: {size / 1e9:.1f}GB")

    def _find_largest_files(self,
                          start_path: str,
                          limit: int = 10) -> List[Tuple[int, str]]:
        """Find largest files in directory tree"""
        largest_files = []

        for root, _, files in os.walk(start_path):
            if any(p in root for p in self.config.protected_paths):
                continue

            for file in files:
                try:
                    file_path = os.path.join(root, file)
                    size = os.path.getsize(file_path)
                    largest_files.append((size, file_path))
                    largest_files.sort(reverse=True)
                    if len(largest_files) > limit:
                        largest_files.pop()
                except OSError:
                    continue

        return largest_files

    def _find_largest_directories(self,
                                start_path: str,
                                limit: int = 10) -> List[Tuple[int, str]]:
        """Find largest directories"""
        largest_dirs = []

        for root, dirs, _ in os.walk(start_path):
            if any(p in root for p in self.config.protected_paths):
                continue

            for dir in dirs:
                try:
                    dir_path = os.path.join(root, dir)
                    size = self._get_dir_size(dir_path)
                    largest_dirs.append((size, dir_path))
                    largest_dirs.sort(reverse=True)
                    if len(largest_dirs) > limit:
                        largest_dirs.pop()
                except OSError:
                    continue

        return largest_dirs