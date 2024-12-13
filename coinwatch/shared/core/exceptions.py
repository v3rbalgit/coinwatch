# src/core/exceptions.py

class CoinwatchError(Exception):
    """Base exception for all application errors"""
    pass

class ServiceError(CoinwatchError):
    """Base exception for service layer errors"""
    pass

class RepositoryError(CoinwatchError):
    """Base exception for repository layer errors"""
    pass

class ValidationError(CoinwatchError):
    """Base exception for validation errors"""
    pass

class AdapterError(CoinwatchError):
    """Base exception for adapter errors"""
    pass

class ConfigurationError(CoinwatchError):
    """Base exception for configuration errors"""
    pass