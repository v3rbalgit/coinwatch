# src/utils/db_retry.py

from typing import TypeVar, Callable, Any
from functools import wraps
import logging
from sqlalchemy.exc import OperationalError, IntegrityError, DBAPIError, InvalidRequestError, SAWarning
from sqlalchemy.orm import Session
import warnings
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log
)

logger = logging.getLogger(__name__)

T = TypeVar('T')

def with_db_retry(max_attempts: int = 3, min_wait: int = 1, max_wait: int = 10) -> Callable:
    """
    Decorator for database operations that should be retried on failure.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type((
                OperationalError,
                DBAPIError,
                InvalidRequestError
            )),
            after=after_log(logger, logging.WARNING),  # Only log failures, and at WARNING level
            reraise=True
        )
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            session = None
            for arg in args + tuple(kwargs.values()):
                if isinstance(arg, Session):
                    session = arg
                    break

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=SAWarning)
                    return func(*args, **kwargs)
            except IntegrityError:
                if session and session.in_transaction():
                    session.rollback()
                raise
            except Exception as e:
                if session and session.in_transaction():
                    try:
                        session.rollback()
                    except Exception as rollback_error:
                        logger.error(f"Error during rollback: {rollback_error}")
                logger.error(f"Error in database operation: {str(e)}")
                raise
        return wrapper
    return decorator