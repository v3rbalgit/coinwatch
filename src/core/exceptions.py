# src/core/exceptions.py
class StateError(Exception):
    """Base exception for state-related errors."""
    pass

class InvalidStateTransition(StateError):
    """Raised when attempting an invalid state transition."""
    pass

class EventProcessingError(StateError):
    """Raised when an error occurs processing an event."""
    pass

class ActionError(Exception):
    """Base exception for action-related errors."""
    pass

class ActionValidationError(ActionError):
    """Raised when action validation fails."""
    pass

class ActionExecutionError(ActionError):
    """Raised when action execution fails."""
    pass

class ActionRollbackError(ActionError):
    """Raised when action rollback fails."""
    pass