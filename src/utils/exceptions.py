# src/utils/exceptions.py

class CoinwatchError(Exception):
    """Base exception for all Coinwatch errors."""
    pass

class DatabaseError(CoinwatchError):
    """Database-related errors."""
    pass

class SessionError(DatabaseError):
    """Session handling errors."""
    pass

class APIError(CoinwatchError):
    """API-related errors."""
    pass

class DataValidationError(CoinwatchError):
    """Data validation errors."""
    pass

class PartitionError(DatabaseError):
    """Partition-related errors."""
    pass

class ServiceError(CoinwatchError):
    """Service-related errors."""
    pass