import os
import logging
import sys
from logging.handlers import RotatingFileHandler

class LoggerSetup:
    """
    Centralized logging configuration for the Coinwatch application.
    Provides consistent logging across all modules with both console and file output.
    """
    _initialized = False
    # Use absolute path that matches Docker volume mount
    _logs_dir = '/app/logs'

    @classmethod
    def _get_log_path(cls, name: str) -> str:
        """
        Generate a log file path based on the name.
        For module paths (contains dots), creates directory structure.
        For class names (no dots), creates direct log file.

        Args:
            name: Name to create log file for (module path or class name)
        Returns:
            str: Path for the log file
        """
        # All logs go to the mounted directory since Docker already mounts to service-specific directory
        if '.' in name:
            # For module paths, use the last part
            filename = f"{name.split('.')[-1]}.log"
        else:
            # For class names, use as is
            filename = f"{name}.log"

        return os.path.join(cls._logs_dir, filename)

    @classmethod
    def setup(cls, name: str) -> logging.Logger:
        """
        Set up and return a logger.
        Automatically handles both module paths and class names.

        Args:
            name: Logger name (__name__ for modules or __class__.__name__ for classes)
        Returns:
            logging.Logger: Configured logger instance
        Example:
            # For module-level logging:
            logger = LoggerSetup.setup(__name__)
            # Creates collector.log from services.market_data.src.collector

            # For class-level logging:
            logger = LoggerSetup.setup(__class__.__name__)
            # Creates KlineManager.log
        """
        # Get or create logger
        logger = logging.getLogger(name)
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
                    # Get the organized log file path
                    debug_log_file = cls._get_log_path(name)

                    # Create the directory structure
                    os.makedirs(os.path.dirname(debug_log_file), exist_ok=True)

                    # File handler for debug logs with rotation
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
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get an existing logger or create a new one.
        Args:
            name: Logger name (module path or class name)
        Returns:
            logging.Logger: Logger instance
        """
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger = cls.setup(name)
        return logger

    @classmethod
    def update_log_level(cls, name: str,
                        console_level: int | None = None,
                        file_level: int | None = None) -> None:
        """
        Update log levels for an existing logger.
        Args:
            name: Name of the logger
            console_level: New console handler log level (if None, level remains unchanged)
            file_level: New file handler log level (if None, level remains unchanged)
        """
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                if console_level is not None:
                    handler.setLevel(console_level)
            elif isinstance(handler, RotatingFileHandler):
                if file_level is not None:
                    handler.setLevel(file_level)

    @classmethod
    def setup_test_logger(cls, name: str) -> logging.Logger:
        """
        Set up a simplified logger for testing environments.
        Args:
            name: Logger name
        Returns:
            logging.Logger: Configured test logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            # Console handler with debug level for tests
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger
