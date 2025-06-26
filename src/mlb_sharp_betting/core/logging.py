"""
Centralized logging configuration for the MLB Sharp Betting system.

This module provides structured logging using structlog for consistent
and comprehensive logging throughout the application.
"""

import logging
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import structlog
from structlog.types import FilteringBoundLogger

from mlb_sharp_betting.core.exceptions import ConfigurationError


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


def get_logger(name: str, **initial_values: Any) -> Any:
    """
    Get a logger instance with optional initial context.
    
    Args:
        name: Logger name (typically __name__)
        **initial_values: Initial context values to bind to logger
    
    Returns:
        Configured structlog logger instance
    """
    logger = structlog.get_logger(name)
    
    if initial_values:
        logger = logger.bind(**initial_values)
    
    return logger


class LoggerMixin:
    """Mixin class to add logging capabilities to other classes."""
    
    @property
    def logger(self) -> Any:
        """Get a logger bound to this class."""
        if not hasattr(self, "_logger"):
            self._logger = get_logger(
                self.__class__.__module__ + "." + self.__class__.__name__
            )
        return self._logger
    
    def log_method_call(
        self, 
        method_name: str, 
        **kwargs: Any
    ) -> Any:
        """
        Log a method call with context.
        
        Args:
            method_name: Name of the method being called
            **kwargs: Additional context to log
        
        Returns:
            Logger bound with method context
        """
        return self.logger.bind(method=method_name, **kwargs)


def log_execution_time(func_name: str) -> Any:
    """
    Decorator to log function execution time.
    
    Args:
        func_name: Name of the function being decorated
    
    Returns:
        Decorator function
    """
    def decorator(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            import time
            
            logger = get_logger(func.__module__)
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                logger.info(
                    "Function execution completed",
                    function=func_name,
                    execution_time=execution_time,
                )
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                logger.error(
                    "Function execution failed",
                    function=func_name,
                    execution_time=execution_time,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise
                
        return wrapper
    return decorator


def log_sensitive_operation(operation: str, **context: Any) -> Any:
    """
    Log a sensitive operation with redacted context.
    
    Args:
        operation: Description of the operation
        **context: Context information (sensitive values will be redacted)
    
    Returns:
        Logger with redacted context
    """
    # List of keys that should be redacted
    sensitive_keys = {
        "password", "token", "key", "secret", "credential", 
        "auth", "authorization", "api_key"
    }
    
    # Redact sensitive values
    safe_context = {}
    for key, value in context.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            safe_context[key] = "[REDACTED]"
        else:
            safe_context[key] = value
    
    logger = get_logger("sensitive_operations")
    return logger.bind(operation=operation, **safe_context) 