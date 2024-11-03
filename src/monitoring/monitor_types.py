# src/monitoring/types.py

from typing import TypedDict, List, Union

class CPUMetrics(TypedDict):
    usage: float
    count: int
    count_logical: int
    load_avg: List[float]

class MemoryMetrics(TypedDict):
    usage: float
    available_mb: float
    total_mb: float
    swap_usage: float

class DiskMetrics(TypedDict):
    usage: float
    free_gb: float
    total_gb: float
    read_mb: float
    write_mb: float

ResourceMetrics = Union[CPUMetrics, MemoryMetrics, DiskMetrics]