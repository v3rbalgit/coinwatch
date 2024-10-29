# src/core/events.py
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional
import uuid
import time
import json

class EventType(Enum):
    # System Events
    SYSTEM_INITIALIZED = "system_initialized"
    SYSTEM_SHUTDOWN = "system_shutdown"

    # State Change Events
    STATE_CHANGED = "state_changed"

    # Service Events
    SERVICE_HEALTH_CHANGED = "service_health_changed"
    SERVICE_STARTED = "service_started"
    SERVICE_STOPPED = "service_stopped"

    # Symbol Events
    SYMBOLS_UPDATED = "symbols_updated"
    SYMBOLS_CHECK_NEEDED = "symbols_check_needed"

    # Data Events
    DATA_GAP_DETECTED = "data_gap_detected"
    DATA_SYNC_NEEDED = "data_sync_needed"
    HISTORICAL_DATA_NEEDED = "historical_data_needed"

    # Historical Data Events
    HISTORICAL_DATA_STARTED = "historical_data_started"
    HISTORICAL_DATA_PROGRESS = "historical_data_progress"
    HISTORICAL_DATA_COMPLETED = "historical_data_completed"
    HISTORICAL_DATA_FAILED = "historical_data_failed"

    # Sync Events
    DATA_SYNC_STARTED = "data_sync_started"
    DATA_SYNC_COMPLETED = "data_sync_completed"
    DATA_SYNC_FAILED = "data_sync_failed"

    # Resource Events
    POOL_OVERFLOW = "pool_overflow"
    RESOURCE_EXHAUSTED = "resource_exhausted"

    # Action Events
    ACTION_STARTED = "action_started"
    ACTION_COMPLETED = "action_completed"
    ACTION_FAILED = "action_failed"

    # Resource Events
    RESOURCE_WARNING = "resource_warning"
    RESOURCE_CRITICAL = "resource_critical"
    RESOURCE_RECOVERED = "resource_recovered"
    RESOURCE_ACTION_STARTED = "resource_action_started"
    RESOURCE_ACTION_COMPLETED = "resource_action_completed"

    # Resource-specific Events
    CPU_THRESHOLD_EXCEEDED = "cpu_threshold_exceeded"
    MEMORY_THRESHOLD_EXCEEDED = "memory_threshold_exceeded"
    DISK_THRESHOLD_EXCEEDED = "disk_threshold_exceeded"
    NETWORK_THRESHOLD_EXCEEDED = "network_threshold_exceeded"
    SYSTEM_OVERLOAD = "system_overload"
    RESOURCE_CLEANUP_NEEDED = "resource_cleanup_needed"

    # Database Events
    DATA_INTEGRITY_CHECK_STARTED = "data_integrity_check_started"
    DATA_INTEGRITY_CHECK_COMPLETED = "data_integrity_check_completed"
    DATA_INTEGRITY_CHECK_FAILED = "data_integrity_check_failed"
    MAINTENANCE_STARTED = "maintenance_started"
    MAINTENANCE_COMPLETED = "maintenance_completed"
    MAINTENANCE_FAILED = "maintenance_failed"
    DATABASE_PERFORMANCE_CRITICAL = "database_performance_critical"
    DATABASE_STORAGE_CRITICAL = "database_storage_critical"
    DATABASE_CONNECTION_CRITICAL = "database_connection_critical"
    DATABASE_RECOVERED = "database_recovered"


@dataclass
class Event:
    """
    Represents a system event with complete tracking information.

    Attributes:
        id: Unique identifier for the event
        type: Type of event from EventType enum
        data: Event payload data
        metadata: Additional event context
        timestamp: Event creation time
        correlation_id: ID to track related events
        causation_id: ID of event that caused this event
    """
    id: str
    type: EventType
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: float
    correlation_id: str
    causation_id: Optional[str] = None

    @classmethod
    def create(cls,
               event_type: EventType,
               data: Dict[str, Any],
               metadata: Optional[Dict[str, Any]] = None,
               correlation_id: Optional[str] = None,
               causation_id: Optional[str] = None) -> 'Event':
        """Create a new event with generated ID and timestamp."""
        return cls(
            id=str(uuid.uuid4()),
            type=event_type,
            data=data,
            metadata=metadata or {},
            timestamp=time.time(),
            correlation_id=correlation_id or str(uuid.uuid4()),
            causation_id=causation_id
        )

    def to_json(self) -> str:
        """Serialize event to JSON string."""
        return json.dumps({
            'id': self.id,
            'type': self.type.value,
            'data': self.data,
            'metadata': self.metadata,
            'timestamp': self.timestamp,
            'correlation_id': self.correlation_id,
            'causation_id': self.causation_id
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'Event':
        """Create event from JSON string."""
        data = json.loads(json_str)
        return cls(
            id=data['id'],
            type=EventType(data['type']),
            data=data['data'],
            metadata=data['metadata'],
            timestamp=data['timestamp'],
            correlation_id=data['correlation_id'],
            causation_id=data['causation_id']
        )