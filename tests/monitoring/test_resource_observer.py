# tests/monitoring/test_resource_observer.py
import pytest
from unittest.mock import patch, MagicMock

from src.monitoring.observers.observers import ResourceObserver
from src.monitoring.monitor_types import CPUMetrics, MemoryMetrics, DiskMetrics

@pytest.fixture
def resource_observer():
    return ResourceObserver()

@pytest.fixture
def mock_psutil():
    with patch('src.monitoring.observers.observers.psutil') as mock:
        # Mock virtual_memory
        mock.virtual_memory.return_value = MagicMock(
            percent=50.0,
            available=8 * 1024 * 1024 * 1024,  # 8GB
            total=16 * 1024 * 1024 * 1024      # 16GB
        )

        # Mock CPU
        mock.cpu_percent.return_value = 30.0
        mock.cpu_count.return_value = 8
        mock.getloadavg.return_value = [1.0, 1.5, 2.0]

        # Mock disk
        mock.disk_usage.return_value = MagicMock(
            percent=40.0,
            free=500 * 1024 * 1024 * 1024,    # 500GB
            total=1000 * 1024 * 1024 * 1024   # 1TB
        )

        # Mock swap
        mock.swap_memory.return_value = MagicMock(percent=20.0)

        yield mock

@pytest.mark.asyncio
async def test_get_memory_metrics(resource_observer, mock_psutil):
    metrics = await resource_observer._get_memory_metrics()

    assert isinstance(metrics, dict)
    assert metrics['usage'] == 0.5
    assert metrics['available_mb'] == 8 * 1024  # 8GB in MB
    assert metrics['total_mb'] == 16 * 1024     # 16GB in MB
    assert metrics['swap_usage'] == 0.2

@pytest.mark.asyncio
async def test_get_cpu_metrics(resource_observer, mock_psutil):
    metrics = await resource_observer._get_cpu_metrics()

    assert isinstance(metrics, dict)
    assert metrics['usage'] == 0.3
    assert metrics['count'] == 8
    assert metrics['count_logical'] == 8
    assert len(metrics['load_avg']) == 3

@pytest.mark.asyncio
async def test_severity_evaluation(resource_observer, mock_psutil):
    # Test normal conditions
    observation = await resource_observer.observe()
    assert observation.severity == 'normal'

    # Test warning conditions
    mock_psutil.virtual_memory.return_value.percent = 80.0
    observation = await resource_observer.observe()
    assert observation.severity == 'normal'  # First warning
    observation = await resource_observer.observe()
    observation = await resource_observer.observe()
    assert observation.severity == 'warning'  # After 3 warnings

    # Test critical conditions
    mock_psutil.virtual_memory.return_value.percent = 90.0
    observation = await resource_observer.observe()
    assert observation.severity == 'critical'

@pytest.mark.asyncio
async def test_threshold_updates(resource_observer):
    # Test updating thresholds
    resource_observer.update_thresholds('memory', warning=0.6, critical=0.8)
    thresholds = resource_observer.get_threshold('memory')
    assert thresholds['warning'] == 0.6
    assert thresholds['critical'] == 0.8