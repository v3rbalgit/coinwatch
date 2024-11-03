# tests/monitoring/actions/test_resource_actions.py
import pytest
from unittest.mock import patch, MagicMock
import psutil
from datetime import datetime

from src.monitoring.actions.resource import (
    ResourceActionConfig,
    MemoryRecoveryAction,
    CPURecoveryAction,
    DiskRecoveryAction
)

class TestMemoryActionValidation:
    @pytest.mark.asyncio
    async def test_valid_context(self, memory_action):
        context = {
            'source': 'resource',
            'metrics': {},
            'memory': {
                'usage': 0.75,
                'available_mb': 8192,
                'total_mb': 16384,
                'swap_usage': 0.2
            }
        }
        assert await memory_action.validate(context) is True

    @pytest.mark.asyncio
    async def test_invalid_context(self, memory_action):
        invalid_contexts = [
            {},  # Empty context
            {'source': 'wrong'},  # Wrong source
            {  # Missing required fields
                'source': 'resource',
                'memory': {'usage': 0.75}
            },
            {  # Invalid type
                'source': 'resource',
                'memory': {'usage': 'high'}
            }
        ]
        for context in invalid_contexts:
            assert await memory_action.validate(context) is False

class TestCPUActionValidation:
    @pytest.mark.asyncio
    async def test_valid_context(self, cpu_action):
        context = {
            'source': 'resource',
            'metrics': {},
            'cpu': {
                'usage': 0.75,
                'count': 8,
                'load_avg': [1.0, 1.5, 2.0]
            }
        }
        assert await cpu_action.validate(context) is True

    @pytest.mark.asyncio
    async def test_invalid_context(self, cpu_action):
        invalid_contexts = [
            {},  # Empty context
            {'source': 'wrong'},  # Wrong source
            {  # Missing required fields
                'source': 'resource',
                'cpu': {'usage': 0.75}
            },
            {  # Invalid type
                'source': 'resource',
                'cpu': {'usage': 'high', 'load_avg': 1.0}
            }
        ]
        for context in invalid_contexts:
            assert await cpu_action.validate(context) is False

class TestDiskActionValidation:
    @pytest.mark.asyncio
    async def test_valid_context(self, disk_action):
        context = {
            'source': 'resource',
            'metrics': {},
            'disk': {
                'usage': 0.75,
                'free_gb': 100,
                'total_gb': 500
            }
        }
        assert await disk_action.validate(context) is True

    @pytest.mark.asyncio
    async def test_invalid_context(self, disk_action):
        invalid_contexts = [
            {},  # Empty context
            {'source': 'wrong'},  # Wrong source
            {  # Missing required fields
                'source': 'resource',
                'disk': {'usage': 0.75}
            },
            {  # Invalid values
                'source': 'resource',
                'disk': {
                    'usage': 0.75,
                    'free_gb': 100,
                    'total_gb': 0  # Invalid total
                }
            }
        ]
        for context in invalid_contexts:
            assert await disk_action.validate(context) is False

@pytest.fixture
def mock_psutil():
    with patch('src.monitoring.actions.resource.psutil') as mock:
        # Mock memory
        mock.virtual_memory.return_value = MagicMock(
            percent=75.0,
            available=8 * 1024 * 1024 * 1024,  # 8GB
            total=16 * 1024 * 1024 * 1024      # 16GB
        )

        # Mock CPU
        mock.cpu_percent.return_value = 50.0
        mock.cpu_count.return_value = 8
        mock.getloadavg.return_value = (1.0, 1.5, 2.0)

        # Mock disk
        mock.disk_usage.return_value = MagicMock(
            percent=70.0,
            free=100 * 1024 * 1024 * 1024,    # 100GB
            total=500 * 1024 * 1024 * 1024    # 500GB
        )

        yield mock

@pytest.fixture
def config():
    return ResourceActionConfig()

@pytest.fixture
def memory_action(config):
    return MemoryRecoveryAction(config)

@pytest.fixture
def cpu_action(config):
    return CPURecoveryAction(config)

@pytest.fixture
def disk_action(config):
    return DiskRecoveryAction(config)

class TestMemoryRecoveryAction:
    @pytest.mark.asyncio
    async def test_normal_memory(self, memory_action, mock_psutil):
        context = {'memory': {'usage': 0.5}}
        result = await memory_action.execute(context)
        assert result is True
        # No recovery actions should be taken
        assert not mock_psutil.method_calls

    @pytest.mark.asyncio
    async def test_high_memory(self, memory_action, mock_psutil):
        context = {'memory': {'usage': 0.8}}

        with patch('gc.collect') as mock_gc:
            result = await memory_action.execute(context)

        assert result is True
        assert mock_gc.called

    @pytest.mark.asyncio
    async def test_critical_memory(self, memory_action, mock_psutil):
        context = {'memory': {'usage': 0.9}}

        with patch('gc.collect') as mock_gc:
            result = await memory_action.execute(context)

        assert result is True
        # Should call gc.collect multiple times
        assert mock_gc.call_count >= 3

    @pytest.mark.asyncio
    async def test_cooldown_period(self, memory_action):
        context = {'memory': {'usage': 0.8}}

        # First execution should work
        result1 = await memory_action.execute(context)
        assert result1 is True

        # Second immediate execution should be blocked by cooldown
        result2 = await memory_action.execute(context)
        assert result2 is False

class TestCPURecoveryAction:
    @pytest.mark.asyncio
    async def test_normal_cpu(self, cpu_action, mock_psutil):
        context = {'cpu': {'usage': 0.5}}
        result = await cpu_action.execute(context)
        assert result is True
        # No recovery actions should be taken
        assert not mock_psutil.method_calls

    @pytest.mark.asyncio
    async def test_high_cpu(self, cpu_action, mock_psutil):
        context = {'cpu': {'usage': 0.75}}
        result = await cpu_action.execute(context)
        assert result is True
        # Should log CPU processes
        assert mock_psutil.process_iter.called

    @pytest.mark.asyncio
    async def test_critical_cpu(self, cpu_action, mock_psutil):
        context = {'cpu': {'usage': 0.85}}
        result = await cpu_action.execute(context)
        assert result is True
        # Should check load average
        assert mock_psutil.getloadavg.called

class TestDiskRecoveryAction:
    @pytest.fixture
    def mock_os(self):
        with patch('src.monitoring.actions.resource.os') as mock:
            mock.path.exists.return_value = True
            mock.scandir.return_value.__enter__.return_value = [
                MagicMock(
                    is_file=lambda: True,
                    stat=lambda: MagicMock(
                        st_size=1024*1024,  # 1MB
                        st_mtime=datetime.now().timestamp() - 86400*10  # 10 days old
                    ),
                    path="/tmp/test_file"
                )
            ]
            yield mock

    @pytest.mark.asyncio
    async def test_normal_disk(self, disk_action, mock_psutil):
        context = {'disk': {'usage': 0.5}}
        result = await disk_action.execute(context)
        assert result is True
        # No recovery actions should be taken
        assert not mock_psutil.disk_usage.called

    @pytest.mark.asyncio
    async def test_high_disk(self, disk_action, mock_psutil, mock_os):
        context = {'disk': {'usage': 0.85}}
        result = await disk_action.execute(context)
        assert result is True
        # Should check disk usage
        assert mock_psutil.disk_usage.called
        # Should scan directories
        assert mock_os.scandir.called

    @pytest.mark.asyncio
    async def test_critical_disk(self, disk_action, mock_psutil, mock_os):
        context = {'disk': {'usage': 0.95}}
        result = await disk_action.execute(context)
        assert result is True
        # Should perform emergency cleanup
        assert mock_os.unlink.called

    @pytest.mark.asyncio
    async def test_cleanup_temp_files(self, disk_action, mock_os):
        cleaned = await disk_action._clean_temp_files()
        assert cleaned > 0
        assert mock_os.unlink.called

class TestResourceActionConfig:
    def test_default_config(self):
        config = ResourceActionConfig()
        assert config.memory_warning_threshold == 0.75
        assert config.memory_critical_threshold == 0.85
        assert config.cpu_warning_threshold == 0.70
        assert config.cpu_critical_threshold == 0.80
        assert '/boot' in config.protected_paths

    def test_custom_config(self):
        config = ResourceActionConfig(
            memory_warning_threshold=0.80,
            memory_critical_threshold=0.90,
            protected_paths={'/custom'}
        )
        assert config.memory_warning_threshold == 0.80
        assert config.memory_critical_threshold == 0.90
        assert '/custom' in config.protected_paths

@pytest.mark.asyncio
class TestActionRetries:
    async def test_retry_on_failure(self, memory_action, mock_psutil):
        # Make the first attempt fail
        mock_psutil.virtual_memory.side_effect = [
            MagicMock(percent=90.0),  # First try
            MagicMock(percent=85.0),  # Second try
            MagicMock(percent=70.0)   # Third try
        ]

        context = {'memory': {'usage': 0.9}}
        result = await memory_action.execute(context)

        assert result is True
        assert mock_psutil.virtual_memory.call_count == 3

    async def test_max_retries_exceeded(self, memory_action, mock_psutil):
        # Make all attempts fail
        mock_psutil.virtual_memory.return_value = MagicMock(percent=95.0)

        context = {'memory': {'usage': 0.9}}
        result = await memory_action.execute(context)

        assert result is False
        assert mock_psutil.virtual_memory.call_count >= 3

@pytest.mark.asyncio
class TestErrorHandling:
    async def test_handle_os_error(self, disk_action, mock_os):
        mock_os.scandir.side_effect = OSError("Permission denied")

        context = {'disk': {'usage': 0.85}}
        result = await disk_action.execute(context)

        # Should handle error gracefully
        assert result is False

    async def test_handle_psutil_error(self, memory_action, mock_psutil):
        mock_psutil.virtual_memory.side_effect = psutil.Error("Test error")

        context = {'memory': {'usage': 0.85}}
        result = await memory_action.execute(context)

        # Should handle error gracefully
        assert result is False