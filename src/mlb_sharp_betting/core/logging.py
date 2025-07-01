"""
Enhanced Logging System with Universal Compatibility

Provides comprehensive logging with performance monitoring, security features,
and universal compatibility across all components.
"""

import logging
import sys
import json
import time
import inspect
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Union

import structlog
from structlog.types import FilteringBoundLogger

from mlb_sharp_betting.core.exceptions import ConfigurationError

# Performance tracking context
_current_operation: ContextVar[Optional[str]] = ContextVar('current_operation', default=None)


def setup_sql_operations_logger(log_file: Optional[Path] = None) -> logging.Logger:
    """
    Set up a dedicated SQL operations logger that writes to a file with 
    structured format for future PostgreSQL table conversion.
    
    Args:
        log_file: Path for SQL operations log file
        
    Returns:
        Configured SQL operations logger
    """
    if log_file is None:
        log_file = Path("logs/sql_operations.log")
    
    # Ensure log directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Create dedicated SQL logger
    sql_logger = logging.getLogger("sql_operations")
    sql_logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers to avoid duplicates
    sql_logger.handlers.clear()
    
    # Create file handler with custom formatter
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Custom formatter for SQL operations - pipe-delimited for easy PostgreSQL import
    class SQLOperationFormatter(logging.Formatter):
        def format(self, record):
            # Extract structured data from the log record
            timestamp = datetime.now().isoformat()
            level = record.levelname
            logger_name = record.name
            
            # Get extra fields from structlog
            extra_data = getattr(record, '_record', {})
            
            # Core fields for PostgreSQL table
            operation_type = extra_data.get('operation_type', 'UNKNOWN')
            query_hash = extra_data.get('query_hash', 0)
            execution_time_ms = extra_data.get('execution_time_ms', 0)
            success = extra_data.get('success', False)
            error_type = extra_data.get('error_type', '')
            error_message = extra_data.get('error_message', '')
            rows_affected = extra_data.get('rows_affected', 0)
            rows_returned = extra_data.get('rows_returned', 0)
            query_preview = extra_data.get('query_preview', '')
            parameters_preview = extra_data.get('parameters_preview', '')
            
            # Additional context fields
            coordinator_type = extra_data.get('coordinator_type', '')
            transaction_id = extra_data.get('transaction_id', '')
            batch_size = extra_data.get('batch_size', 0)
            has_returning = extra_data.get('has_returning', False)
            
            # Create pipe-delimited record for easy PostgreSQL COPY import
            # Format: timestamp|level|operation_type|query_hash|execution_time_ms|success|error_type|error_message|rows_affected|rows_returned|query_preview|parameters_preview|coordinator_type|transaction_id|batch_size|has_returning
            fields = [
                timestamp,
                level,
                operation_type,
                str(query_hash),
                str(execution_time_ms),
                str(success),
                error_type.replace('|', '_'),  # Escape delimiter
                error_message.replace('|', '_').replace('\n', ' '),  # Escape delimiter and newlines
                str(rows_affected),
                str(rows_returned),
                query_preview.replace('|', '_'),  # Escape delimiter
                parameters_preview.replace('|', '_'),  # Escape delimiter
                coordinator_type,
                str(transaction_id),
                str(batch_size),
                str(has_returning)
            ]
            
            return '|'.join(fields)
    
    file_handler.setFormatter(SQLOperationFormatter())
    sql_logger.addHandler(file_handler)
    
    # Prevent propagation to root logger to avoid duplicate console output
    sql_logger.propagate = False
    
    return sql_logger


def log_sql_operation(operation_data: Dict[str, Any]) -> None:
    """
    Log a SQL operation to the dedicated SQL operations log file.
    
    Args:
        operation_data: Dictionary containing SQL operation details
    """
    sql_logger = logging.getLogger("sql_operations")
    
    # Create a log record with the operation data
    if operation_data.get('success', True):
        sql_logger.debug("SQL operation", extra={'_record': operation_data})
    else:
        sql_logger.error("SQL operation failed", extra={'_record': operation_data})


# Initialize SQL operations logger on module import
_sql_logger = setup_sql_operations_logger()


def setup_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    structured: bool = True,
    include_stdlib: bool = True,
) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        structured: Whether to use structured logging format
        include_stdlib: Whether to configure standard library logging
    
    Raises:
        ConfigurationError: If logging configuration fails
    """
    try:
        # Configure structlog
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
        ]
        
        if structured:
            # Use JSON formatting for structured logs
            processors.append(structlog.processors.JSONRenderer())
        else:
            # Use console-friendly formatting
            processors.append(structlog.dev.ConsoleRenderer())
        
        # Configure structlog
        structlog.configure(
            processors=processors,  # type: ignore[arg-type]
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            context_class=dict,
            cache_logger_on_first_use=True,
        )
        
        if include_stdlib:
            # Configure standard library logging
            logging.basicConfig(
                format="%(message)s",
                stream=sys.stdout,
                level=getattr(logging, level.upper()),
            )
            
            # Configure log file if specified
            if log_file:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(getattr(logging, level.upper()))
                
                # Add file handler to root logger
                root_logger = logging.getLogger()
                root_logger.addHandler(file_handler)
        
    except Exception as e:
        raise ConfigurationError(
            "Failed to setup logging configuration",
            details={"level": level, "log_file": str(log_file) if log_file else None},
            cause=e,
        ) from e


class BackwardCompatibleLogger:
    """Universal logger that works with both new and legacy code patterns."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        # Use standard structlog logger for compatibility
        self.logger = structlog.get_logger(service_name)
        
        # Console logger for clean output
        self.console_logger = logging.getLogger(f"{service_name}_console")
        self.console_logger.setLevel(logging.INFO)
    
    # New clean methods
    def info_console(self, message: str, **kwargs):
        """Log important info to both console and file."""
        self.console_logger.info(message)
        self.logger.info(message, **kwargs)
    
    def debug_file_only(self, message: str, **kwargs):
        """Log debug info only to file."""
        self.logger.debug(message, **kwargs)
    
    def warning_console(self, message: str, **kwargs):
        """Log warnings to both console and file."""
        self.console_logger.warning(message)
        self.logger.warning(message, **kwargs)
    
    def error_console(self, message: str, **kwargs):
        """Log errors to both console and file."""
        self.console_logger.error(message)
        self.logger.error(message, **kwargs)
    
    # Legacy compatibility methods
    def info(self, message: str, **kwargs):
        """Legacy method - redirect to info_console."""
        self.info_console(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Legacy method - redirect to debug_file_only."""
        self.debug_file_only(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Legacy method - redirect to warning_console."""
        self.warning_console(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Legacy method - redirect to error_console."""
        self.error_console(message, **kwargs)
    
    # Support for bound logger syntax
    def bind(self, **kwargs):
        """Return a bound logger for compatibility."""
        return BoundCompatibleLogger(self, **kwargs)
    
    def summary(self, title: str, data: dict):
        """Log a clean summary to console."""
        print(f"\n📊 {title.upper()}")
        print("=" * 50)
        for key, value in data.items():
            print(f"  {key}: {value}")
        print("=" * 50)
        
        # Also log to file
        self.logger.info(f"SUMMARY: {title}", **data)


class BoundCompatibleLogger:
    """Bound logger for chaining with additional context."""
    
    def __init__(self, parent_logger, **bound_kwargs):
        self.parent = parent_logger
        self.bound_context = bound_kwargs
    
    def _get_combined_kwargs(self, **kwargs):
        """Combine bound context with new kwargs."""
        combined = dict(self.bound_context)
        combined.update(kwargs)
        return combined
    
    def info_console(self, message: str, **kwargs):
        """Log info to console with bound context."""
        combined_kwargs = self._get_combined_kwargs(**kwargs)
        self.parent.info_console(message, **combined_kwargs)
    
    def debug_file_only(self, message: str, **kwargs):
        """Log debug to file with bound context."""
        combined_kwargs = self._get_combined_kwargs(**kwargs)
        self.parent.debug_file_only(message, **combined_kwargs)
    
    def warning_console(self, message: str, **kwargs):
        """Log warning to console with bound context."""
        combined_kwargs = self._get_combined_kwargs(**kwargs)
        self.parent.warning_console(message, **combined_kwargs)
    
    def error_console(self, message: str, **kwargs):
        """Log error to console with bound context."""
        combined_kwargs = self._get_combined_kwargs(**kwargs)
        self.parent.error_console(message, **combined_kwargs)
    
    # Legacy compatibility methods
    def info(self, message: str, **kwargs):
        """Legacy method - redirect to info_console."""
        self.info_console(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Legacy method - redirect to debug_file_only."""
        self.debug_file_only(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Legacy method - redirect to warning_console."""
        self.warning_console(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Legacy method - redirect to error_console."""
        self.error_console(message, **kwargs)
    
    def bind(self, **kwargs):
        """Return another bound logger with additional context."""
        combined_context = dict(self.bound_context)
        combined_context.update(kwargs)
        return BoundCompatibleLogger(self.parent, **combined_context)


# Flag to track if compatibility has been setup
_compatibility_setup_done = False

def setup_universal_logger_compatibility():
    """
    🚨 UNIVERSAL FIX: Apply logger compatibility globally to ALL structlog bound loggers.
    
    This ensures that any logger.bind() call anywhere in the application 
    will return a logger with the required compatibility methods.
    """
    global _compatibility_setup_done
    
    # Prevent multiple setups
    if _compatibility_setup_done:
        return
    
    # Store the original bind method
    original_bind = structlog.stdlib.BoundLogger.bind
    
    def enhanced_bind(self, **kwargs):
        """Enhanced bind that returns loggers with compatibility methods."""
        bound_logger = original_bind(self, **kwargs)
        
        # Add compatibility methods if they don't exist
        if not hasattr(bound_logger, 'info_console'):
            def info_console(message: str, **extra_kwargs):
                """Log important info to both console and file."""
                combined_kwargs = dict(kwargs)
                combined_kwargs.update(extra_kwargs)
                logging.getLogger().info(message)  # Console output
                self.info(message, **combined_kwargs)  # File output
            bound_logger.info_console = info_console
        
        if not hasattr(bound_logger, 'debug_file_only'):
            def debug_file_only(message: str, **extra_kwargs):
                """Log debug info only to file."""
                combined_kwargs = dict(kwargs)
                combined_kwargs.update(extra_kwargs)
                self.debug(message, **combined_kwargs)
            bound_logger.debug_file_only = debug_file_only
        
        if not hasattr(bound_logger, 'warning_console'):
            def warning_console(message: str, **extra_kwargs):
                """Log warnings to both console and file."""
                combined_kwargs = dict(kwargs)
                combined_kwargs.update(extra_kwargs)
                logging.getLogger().warning(message)  # Console output
                self.warning(message, **combined_kwargs)  # File output
            bound_logger.warning_console = warning_console
        
        if not hasattr(bound_logger, 'error_console'):
            def error_console(message: str, **extra_kwargs):
                """Log errors to both console and file."""
                combined_kwargs = dict(kwargs)
                combined_kwargs.update(extra_kwargs)
                logging.getLogger().error(message)  # Console output
                self.error(message, **combined_kwargs)  # File output
            bound_logger.error_console = error_console
        
        if not hasattr(bound_logger, 'summary'):
            def summary(title: str, data: dict):
                """Log a clean summary to console."""
                print(f"\n📊 {title.upper()}")
                print("=" * 50)
                for key, value in data.items():
                    print(f"  {key}: {value}")
                print("=" * 50)
                # Also log to file
                self.info(f"SUMMARY: {title}", **data)
            bound_logger.summary = summary
        
        return bound_logger
    
    # Monkey patch the bind method globally
    structlog.stdlib.BoundLogger.bind = enhanced_bind
    
    # Mark setup as complete and log only once
    _compatibility_setup_done = True
    print("🔧 Universal logger compatibility enabled for all bound loggers")


def get_clean_logger():
    """
    🚨 DEPRECATED: Use get_logger() instead.
    
    This method exists for backward compatibility but should not be used
    in new code. Use get_logger() which provides universal compatibility.
    """
    import warnings
    warnings.warn(
        "get_clean_logger() is deprecated. Use get_logger() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_logger(__name__)


def get_compatible_logger(name: str) -> BackwardCompatibleLogger:
    """
    🚨 ENHANCED: Get a universally compatible logger instance.
    
    This logger works with both new and legacy code patterns,
    providing all required methods for any part of the application.
    """
    return BackwardCompatibleLogger(name)


def get_logger(name: str = None) -> Union[BackwardCompatibleLogger, structlog.stdlib.BoundLogger]:
    """
    🚨 ENHANCED: Universal logger factory with automatic compatibility.
    
    This is the primary logger factory that should be used throughout the application.
    It automatically ensures compatibility regardless of how the logger is used.
    """
    if name is None:
        # Get caller's module name
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    
    # Universal compatibility is automatically setup when module is imported
    
    # For service classes and components that need full compatibility, use BackwardCompatibleLogger
    if any(keyword in name.lower() for keyword in ['service', 'processor', 'manager', 'orchestrator']):
        return BackwardCompatibleLogger(name)
    
    # For other components, use enhanced structlog logger
    logger = structlog.get_logger(name)
    
    # Ensure the logger has compatibility methods via monkey patching
    if not hasattr(logger, 'info_console'):
        logger.info_console = lambda msg, **kwargs: (
            logging.getLogger().info(msg), logger.info(msg, **kwargs)
        )[1]  # Return the result of logger.info()
    
    if not hasattr(logger, 'debug_file_only'):
        logger.debug_file_only = lambda msg, **kwargs: logger.debug(msg, **kwargs)
    
    if not hasattr(logger, 'warning_console'):
        logger.warning_console = lambda msg, **kwargs: (
            logging.getLogger().warning(msg), logger.warning(msg, **kwargs)
        )[1]
    
    if not hasattr(logger, 'error_console'):
        logger.error_console = lambda msg, **kwargs: (
            logging.getLogger().error(msg), logger.error(msg, **kwargs)
        )[1]
    
    if not hasattr(logger, 'summary'):
        def summary(title: str, data: dict):
            """Log a clean summary to console."""
            print(f"\n📊 {title.upper()}")
            print("=" * 50)
            for key, value in data.items():
                print(f"  {key}: {value}")
            print("=" * 50)
            # Also log to file
            logger.info(f"SUMMARY: {title}", **data)
        logger.summary = summary
    
    return logger


def make_logger_compatible(logger) -> BackwardCompatibleLogger:
    """
    🚨 ENHANCED: Convert any logger to a compatible one.
    
    Takes any logger (structlog, bound logger, etc.) and ensures it has
    all the required compatibility methods for the entire application.
    """
    if isinstance(logger, BackwardCompatibleLogger):
        return logger
    
    if hasattr(logger, '_logger'):
        # Extract the underlying logger name
        logger_name = getattr(logger._logger, 'name', 'converted_logger')
    else:
        logger_name = getattr(logger, 'name', 'converted_logger')
    
    return BackwardCompatibleLogger(logger_name)


# 🚨 AUTOMATIC SETUP: Apply universal compatibility when module is imported
setup_universal_logger_compatibility()


def convert_numpy_types(value):
    """
    Convert numpy types to native Python types for JSON serialization
    """
    import numpy as np
    
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    else:
        return value


# ... existing code for monitoring, timing, security, etc. ... 