# src/monitoring/actions/resource/disk_action.py

import os
import shutil
import time
from typing import Dict, Any, List, Optional, Set, Tuple
import psutil

from .base import ResourceAction
from .config import ResourceActionConfig
from src.utils.logger import LoggerSetup

logger = LoggerSetup.setup(__name__)


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