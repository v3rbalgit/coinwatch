# src/services/monitor/monitor_types.py

from typing import TypedDict, List, Dict

class CPUMetrics(TypedDict):
    """CPU monitoring metrics"""
    usage: float           # CPU usage as percentage (0-1)
    count: int            # Number of physical CPUs
    count_logical: int    # Number of logical CPUs
    load_avg: List[float] # 1, 5, 15 minute load averages

class MemoryMetrics(TypedDict):
    """Memory monitoring metrics"""
    usage: float          # Memory usage as percentage (0-1)
    available_mb: float   # Available memory in MB
    total_mb: float       # Total memory in MB
    swap_usage: float     # Swap usage as percentage (0-1)

class DiskMetrics(TypedDict):
    """Disk monitoring metrics"""
    usage: float          # Disk usage as percentage (0-1)
    free_gb: float       # Free space in GB
    total_gb: float      # Total space in GB
    read_mb: float       # Total bytes read in MB
    write_mb: float      # Total bytes written in MB

class SystemMetrics(TypedDict):
    """Combined system metrics"""
    cpu: CPUMetrics
    memory: MemoryMetrics
    disk: DiskMetrics
    boot_time: int       # System boot timestamp
    process_count: int   # Number of running processes

ResourceThresholds = Dict[str, Dict[str, float]]