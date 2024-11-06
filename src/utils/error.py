# src/utils/error.py
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

@dataclass
class ErrorRecord:
    """Records information about a specific type of error"""
    error_type: str
    count: int = 0
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    related_entities: Set[str] = field(default_factory=set)  # Changed from related_symbols for reusability
    metadata: Dict[str, Any] = field(default_factory=dict)  # Added for flexible error context

    def update(self, entity: Optional[str] = None, **context) -> None:
        """Update error record with new occurrence"""
        now = datetime.now(timezone.utc)
        self.count += 1
        self.last_seen = now
        if not self.first_seen:
            self.first_seen = now
        if entity:
            self.related_entities.add(entity)
        self.metadata.update(context)

class ErrorTracker:
    """Tracks error patterns and frequencies across the application"""

    def __init__(self, window_size: int = 3600):  # 1 hour window
        self._errors: Dict[str, ErrorRecord] = {}
        self._window_size = window_size

    def record_error(self,
                    error: Exception,
                    entity: Optional[str] = None,
                    **context) -> None:
        """
        Record an error occurrence

        Args:
            error: The exception that occurred
            entity: Optional identifier of affected entity (symbol, user, etc)
            **context: Additional context about the error
        """
        error_type = error.__class__.__name__
        now = datetime.now(timezone.utc)

        if error_type not in self._errors:
            self._errors[error_type] = ErrorRecord(
                error_type=error_type,
                first_seen=now,
                last_seen=now
            )

        self._errors[error_type].update(entity, **context)

    def get_error_frequency(self, error_type: str, window_minutes: Optional[int] = None) -> float:
        """
        Get error frequency per hour within specified window

        Args:
            error_type: Type of error to check
            window_minutes: Optional custom window size in minutes
        """
        if record := self._errors.get(error_type):
            if record.first_seen and record.last_seen:
                if window_minutes:
                    # Check only within specified window
                    window_start = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
                    if record.last_seen < window_start:
                        return 0.0

                duration = (record.last_seen - record.first_seen).total_seconds()
                if duration > 0:
                    return (record.count / duration) * 3600
        return 0.0

    def get_affected_entities(self, error_type: str) -> Set[str]:
        """Get all entities affected by a specific error type"""
        if record := self._errors.get(error_type):
            return record.related_entities.copy()
        return set()

    def get_recent_errors(self, window_minutes: int = 60) -> List[ErrorRecord]:
        """Get errors that occurred within the specified window"""
        window_start = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        return [
            record for record in self._errors.values()
            if record.last_seen and record.last_seen >= window_start
        ]

    def get_error_summary(self, window_minutes: int = 60) -> Dict[str, int]:
        """Get summary of errors within time window"""
        recent = self.get_recent_errors(window_minutes)
        summary = {}
        for record in recent:
            summary[record.error_type] = record.count
        return summary