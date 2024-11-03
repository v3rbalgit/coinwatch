# src/monitoring/actions/resource/config.py

from dataclasses import dataclass, field
from typing import Set

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
