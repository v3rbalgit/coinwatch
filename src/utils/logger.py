# src/utils/logger.py
import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

class LoggerSetup:
    """
    Centralized logging configuration for the Coinwatch application.
    Provides consistent logging across all modules with both console and file output.
    """
    _initialized = False
    _logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')

    @classmethod
    def setup(cls, module_name: str) -> logging.Logger:
        """
        Set up and return a logger for a specific module.
        Args:
            module_name: Name of the module requesting the logger
        Returns:
            logging.Logger: Configured logger instance
        Example:
            from src.utils.logger import LoggerSetup
            logger = LoggerSetup.setup(__name__)
        """
        # Get or create logger
        logger = logging.getLogger(module_name)
        logger.setLevel(logging.DEBUG)

        # Avoid adding handlers multiple times
        if not logger.handlers:
            # Console handler for important logs (INFO and above)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

            # Only add file handler if not in test environment
            if "pytest" not in sys.modules:
                try:
                    # Create logs directory if it doesn't exist
                    os.makedirs(cls._logs_dir, exist_ok=True)

                    # File handler for debug logs with rotation
                    debug_log_file = os.path.join(cls._logs_dir, f'{module_name.split(".")[-1]}.log')
                    file_handler = RotatingFileHandler(
                        debug_log_file,
                        maxBytes=10*1024*1024,  # 10MB per file
                        backupCount=5,          # Keep 5 backup files
                        encoding='utf-8'
                    )
                    file_handler.setLevel(logging.DEBUG)
                    file_formatter = logging.Formatter(
                        '%(asctime)s - %(threadName)s - %(levelname)s - [%(name)s] - %(message)s'
                    )
                    file_handler.setFormatter(file_formatter)
                    logger.addHandler(file_handler)
                except (PermissionError, OSError) as e:
                    # Log to console if file logging fails
                    console_handler.setLevel(logging.DEBUG)
                    logger.warning(f"Could not set up file logging: {str(e)}")

        # Set up global logging configuration if not already done
        if not cls._initialized:
            # Quiet noisy loggers
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
            cls._initialized = True

        return logger

    @classmethod
    def get_logger(cls, module_name: str) -> logging.Logger:
        """
        Get an existing logger or create a new one.
        Args:
            module_name: Name of the module requesting the logger
        Returns:
            logging.Logger: Logger instance
        """
        logger = logging.getLogger(module_name)
        if not logger.handlers:
            logger = cls.setup(module_name)
        return logger

    @classmethod
    def update_log_level(cls, module_name: str,
                        console_level: Optional[int] = None,
                        file_level: Optional[int] = None) -> None:
        """
        Update log levels for an existing logger.
        Args:
            module_name: Name of the module
            console_level: New console handler log level (if None, level remains unchanged)
            file_level: New file handler log level (if None, level remains unchanged)
        """
        logger = logging.getLogger(module_name)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                if console_level is not None:
                    handler.setLevel(console_level)
            elif isinstance(handler, RotatingFileHandler):
                if file_level is not None:
                    handler.setLevel(file_level)

    @classmethod
    def setup_test_logger(cls, module_name: str) -> logging.Logger:
        """
        Set up a simplified logger for testing environments.
        Args:
            module_name: Name of the module requesting the logger
        Returns:
            logging.Logger: Configured test logger instance
        """
        logger = logging.getLogger(module_name)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            # Console handler with debug level for tests
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger