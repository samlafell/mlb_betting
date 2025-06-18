"""
Centralized logging configuration for the MLB Sharp Betting system.

This module provides structured logging using structlog for consistent
and comprehensive logging throughout the application.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import structlog
from structlog.types import FilteringBoundLogger

from mlb_sharp_betting.core.exceptions import ConfigurationError


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