# tests/services/test_database_recovery.py

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.services.database.recovery import (
    DatabaseRecovery,
    CircuitState
)
from src.core.monitoring import DatabaseMetrics
from src.utils.domain_types import ServiceStatus
from src.utils.time import TimeUtils

@pytest.fixture
def mock_db_service():
    service = MagicMock()
    service._reconfigure_pool = AsyncMock()

    # Setup async context manager for get_session
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    async_context = AsyncMock()
    async_context.__aenter__.return_value = mock_session
    service.get_session.return_value = async_context

    return service

@pytest.fixture
def recovery(mock_db_service):
    return DatabaseRecovery(mock_db_service)

@pytest.mark.asyncio
async def test_state_transitions(recovery, mock_db_service):
    """Test state transitions through different threshold levels"""
    # Test transition to warning state
    metrics = DatabaseMetrics(
        service_name="database",
        status=ServiceStatus.RUNNING.value,
        uptime_seconds=100,
        last_error=None,
        error_count=0,
        warning_count=0,
        timestamp=TimeUtils.get_current_datetime(),
        additional_metrics={},
        active_connections=80,  # 80% usage
        pool_size=100,
        max_overflow=20,
        available_connections=20,
        deadlocks=0,
        long_queries=0,
        maintenance_due=False,
        replication_lag_seconds=0
    )

    # First check - should go to WARNING
    await recovery.check_and_recover(metrics)
    assert recovery._states.get("connection_usage") == CircuitState.WARNING

    # Force ignore check window by clearing last check time
    recovery._last_check.clear()

    # Update metrics for ERROR state
    metrics.active_connections = 90  # 90% usage
    await recovery.check_and_recover(metrics)
    assert recovery._states.get("connection_usage") == CircuitState.ERROR

    # Force ignore check window again
    recovery._last_check.clear()

    # Update metrics for CRITICAL state
    metrics.active_connections = 98  # 98% usage
    await recovery.check_and_recover(metrics)
    assert recovery._states.get("connection_usage") == CircuitState.CRITICAL

    # Force ignore check window one last time
    recovery._last_check.clear()

    # Test recovery to NORMAL state
    metrics.active_connections = 50  # 50% usage
    await recovery.check_and_recover(metrics)
    assert recovery._states.get("connection_usage") == CircuitState.NORMAL

@pytest.mark.asyncio
async def test_multiple_metric_handling(recovery):
    """Test handling multiple metrics simultaneously"""
    metrics = DatabaseMetrics(
        service_name="database",
        status=ServiceStatus.RUNNING.value,
        uptime_seconds=100,
        last_error=None,
        error_count=5,  # High error count
        warning_count=0,
        timestamp=TimeUtils.get_current_datetime(),
        additional_metrics={},
        active_connections=80,  # High connection usage
        pool_size=100,
        max_overflow=20,
        available_connections=20,
        deadlocks=3,  # Multiple deadlocks
        long_queries=4,  # Multiple long queries
        maintenance_due=False,
        replication_lag_seconds=40  # High replication lag
    )

    await recovery.check_and_recover(metrics)

    # Check that multiple states were updated
    assert recovery._states.get("connection_usage") == CircuitState.WARNING
    assert recovery._states.get("lock_timeout") == CircuitState.WARNING
    assert recovery._states.get("query_timeout") == CircuitState.WARNING
    assert recovery._states.get("replication_lag") == CircuitState.WARNING

@pytest.mark.asyncio
async def test_error_handling_in_recovery_actions(recovery, mock_db_service):
    """Test error handling during recovery actions"""
    # Setup mock to raise error
    mock_db_service._reconfigure_pool.side_effect = Exception("Reconfiguration failed")
    mock_db_service.handle_critical_condition = AsyncMock()

    metrics = DatabaseMetrics(
        service_name="database",
        status=ServiceStatus.RUNNING.value,
        uptime_seconds=100,
        last_error=None,
        error_count=0,
        warning_count=0,
        timestamp=TimeUtils.get_current_datetime(),
        additional_metrics={},
        active_connections=98,  # Force critical state
        pool_size=100,
        max_overflow=20,
        available_connections=2,
        deadlocks=0,
        long_queries=0,
        maintenance_due=False,
        replication_lag_seconds=0
    )

    # Should handle error and call handle_critical_condition
    await recovery.check_and_recover(metrics)
    mock_db_service.handle_critical_condition.assert_called_once()

@pytest.mark.asyncio
async def test_recovery_action_execution(recovery, mock_db_service):
    """Test execution of different types of recovery actions"""
    # Test pool reconfiguration for connection usage
    metrics = DatabaseMetrics(
        service_name="database",
        status=ServiceStatus.RUNNING.value,
        uptime_seconds=100,
        last_error=None,
        error_count=0,
        warning_count=0,
        timestamp=TimeUtils.get_current_datetime(),
        additional_metrics={},
        active_connections=80,  # Force warning state
        pool_size=100,
        max_overflow=20,
        available_connections=20,
        deadlocks=0,
        long_queries=0,
        maintenance_due=False,
        replication_lag_seconds=0
    )

    await recovery.check_and_recover(metrics)
    mock_db_service._reconfigure_pool.assert_called_once()

    # Reset mocks
    mock_db_service._reconfigure_pool.reset_mock()
    mock_db_service._reconfigure_pool.side_effect = None

    # Clear check window
    recovery._last_check.clear()

    # Test database setting changes for replication lag
    metrics.replication_lag_seconds = 100  # Force error state
    await recovery.check_and_recover(metrics)

    # Verify session.execute was called with correct SQL
    mock_session = mock_db_service.get_session.return_value.__aenter__.return_value
    assert mock_session.execute.called
    sql_call = mock_session.execute.call_args[0][0]
    assert "SET" in str(sql_call)

@pytest.mark.asyncio
async def test_cleanup_functionality(recovery):
    """Test cleanup of recovery resources"""
    # Set some initial states
    recovery._states = {
        "connection_usage": CircuitState.WARNING,
        "replication_lag": CircuitState.ERROR
    }
    recovery._last_check = {
        "connection_usage": TimeUtils.get_current_timestamp(),
        "replication_lag": TimeUtils.get_current_timestamp()
    }

    await recovery.cleanup()

    assert not recovery._running
    assert not recovery._states
    assert not recovery._last_check

@pytest.mark.asyncio
async def test_status_reporting(recovery):
    """Test status reporting with various states"""
    # Set up some test states
    recovery._states = {
        "connection_usage": CircuitState.WARNING,
        "replication_lag": CircuitState.ERROR,
        "query_timeout": CircuitState.CRITICAL
    }
    recovery._last_check = {
        "connection_usage": TimeUtils.get_current_timestamp() - 5000,
        "replication_lag": TimeUtils.get_current_timestamp() - 10000,
        "query_timeout": TimeUtils.get_current_timestamp() - 15000
    }

    status = recovery.get_status()

    # Verify all states are reported
    assert "connection_usage: warning" in status.lower()
    assert "replication_lag: error" in status.lower()
    assert "query_timeout: critical" in status.lower()

@pytest.mark.asyncio
async def test_check_window_handling(recovery):
    """Test handling of check windows for different metrics"""
    metrics = DatabaseMetrics(
        service_name="database",
        status=ServiceStatus.RUNNING.value,
        uptime_seconds=100,
        last_error=None,
        error_count=0,
        warning_count=0,
        timestamp=TimeUtils.get_current_datetime(),
        additional_metrics={},
        active_connections=80,
        pool_size=100,
        max_overflow=20,
        available_connections=20,
        deadlocks=0,
        long_queries=0,
        maintenance_due=False,
        replication_lag_seconds=0
    )

    # First check should update state
    await recovery.check_and_recover(metrics)
    first_state = recovery._states.get("connection_usage")

    # Immediate second check should not update state
    metrics.active_connections = 95  # Would normally trigger different state
    await recovery.check_and_recover(metrics)
    second_state = recovery._states.get("connection_usage")

    assert first_state == second_state  # State should not change within check window