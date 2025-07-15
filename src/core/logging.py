"""
Unified Logging System

Consolidates logging patterns from:
- src/mlb_sharp_betting/core/logging.py
- sportsbookreview/logging.py (if exists)
- action/logging.py (if exists)

Provides structured logging with correlation IDs, contextual information,
performance metrics, and comprehensive monitoring capabilities.
"""

import logging
import logging.handlers
import sys
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog
from structlog.stdlib import LoggerFactory

from .exceptions import UnifiedBettingError


class LogLevel(str, Enum):
    """Log levels for the unified logging system."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogComponent(str, Enum):
    """Components that can generate logs."""

    CORE = "core"
    DATABASE = "database"
    API_CLIENT = "api_client"
    SCRAPER = "scraper"
    PARSER = "parser"
    VALIDATOR = "validator"
    ANALYSIS = "analysis"
    STRATEGY = "strategy"
    BACKTESTING = "backtesting"
    NOTIFICATION = "notification"
    CLI = "cli"
    SCHEDULER = "scheduler"
    RATE_LIMITER = "rate_limiter"
    CIRCUIT_BREAKER = "circuit_breaker"
    MONITORING = "monitoring"
    LEGACY_MLB_SHARP = "legacy_mlb_sharp"
    LEGACY_SBR = "legacy_sbr"
    LEGACY_ACTION = "legacy_action"


class UnifiedLogger:
    """
    Unified logger with structured logging, correlation tracking,
    and comprehensive monitoring capabilities.
    """

    def __init__(
        self,
        name: str,
        component: LogComponent,
        correlation_id: str | None = None,
        extra_context: dict[str, Any] | None = None,
    ):
        """
        Initialize unified logger.

        Args:
            name: Logger name
            component: Component generating logs
            correlation_id: Correlation ID for request tracing
            extra_context: Additional context to include in all logs
        """
        self.name = name
        self.component = component
        self.correlation_id = correlation_id or str(uuid4())
        self.extra_context = extra_context or {}

        # Get the underlying structlog logger
        self._logger = structlog.get_logger(name)

        # Bind common context
        self._logger = self._logger.bind(
            component=component.value,
            correlation_id=self.correlation_id,
            **self.extra_context,
        )

    def _log(
        self,
        level: LogLevel,
        message: str,
        *,
        operation: str | None = None,
        duration: float | None = None,
        error: Exception | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """
        Internal logging method with structured data.

        Args:
            level: Log level
            message: Log message
            operation: Operation being performed
            duration: Operation duration in seconds
            error: Exception if applicable
            extra: Additional context data
            **kwargs: Additional keyword arguments
        """
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "level": level.value,
            "message": message,
            "component": self.component.value,
            "correlation_id": self.correlation_id,
        }

        if operation:
            log_data["operation"] = operation

        if duration is not None:
            log_data["duration"] = duration
            log_data["performance"] = self._classify_performance(duration)

        if error:
            log_data["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": str(error) if hasattr(error, "__traceback__") else None,
            }

            # Add unified error context if available
            if isinstance(error, UnifiedBettingError):
                log_data["error"].update(
                    {
                        "error_code": error.error_code,
                        "recoverable": error.recoverable,
                        "user_message": error.user_message,
                        "details": error.details,
                    }
                )

        if extra:
            log_data.update(extra)

        # Add any additional kwargs
        log_data.update(kwargs)

        # Log using structlog
        logger_method = getattr(self._logger, level.value.lower())
        logger_method(message, **log_data)

    def _classify_performance(self, duration: float) -> str:
        """Classify performance based on duration."""
        if duration < 0.1:
            return "fast"
        elif duration < 1.0:
            return "normal"
        elif duration < 5.0:
            return "slow"
        else:
            return "very_slow"

    def debug(
        self,
        message: str,
        *,
        operation: str | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, operation=operation, extra=extra, **kwargs)

    def info(
        self,
        message: str,
        *,
        operation: str | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Log info message."""
        self._log(LogLevel.INFO, message, operation=operation, extra=extra, **kwargs)

    def warning(
        self,
        message: str,
        *,
        operation: str | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Log warning message."""
        self._log(LogLevel.WARNING, message, operation=operation, extra=extra, **kwargs)

    def error(
        self,
        message: str,
        *,
        operation: str | None = None,
        error: Exception | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Log error message."""
        self._log(
            LogLevel.ERROR,
            message,
            operation=operation,
            error=error,
            extra=extra,
            **kwargs,
        )

    def critical(
        self,
        message: str,
        *,
        operation: str | None = None,
        error: Exception | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Log critical message."""
        self._log(
            LogLevel.CRITICAL,
            message,
            operation=operation,
            error=error,
            extra=extra,
            **kwargs,
        )

    def log_operation_start(
        self, operation: str, *, extra: dict[str, Any] | None = None, **kwargs
    ) -> float:
        """
        Log operation start and return start time.

        Args:
            operation: Operation name
            extra: Additional context
            **kwargs: Additional keyword arguments

        Returns:
            Start time for duration calculation
        """
        start_time = time.time()
        self.info(
            f"Starting operation: {operation}",
            operation=operation,
            extra=extra,
            **kwargs,
        )
        return start_time

    def log_operation_end(
        self,
        operation: str,
        start_time: float,
        *,
        success: bool = True,
        error: Exception | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """
        Log operation end with duration.

        Args:
            operation: Operation name
            start_time: Start time from log_operation_start
            success: Whether operation succeeded
            error: Exception if operation failed
            extra: Additional context
            **kwargs: Additional keyword arguments
        """
        duration = time.time() - start_time

        if success:
            self.info(
                f"Completed operation: {operation}",
                operation=operation,
                duration=duration,
                extra=extra,
                **kwargs,
            )
        else:
            self.error(
                f"Failed operation: {operation}",
                operation=operation,
                duration=duration,
                error=error,
                extra=extra,
                **kwargs,
            )

    def log_api_request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        **kwargs,
    ) -> None:
        """Log API request."""
        extra = {
            "api_method": method,
            "api_url": url,
        }

        if headers:
            # Don't log sensitive headers
            safe_headers = {
                k: v
                for k, v in headers.items()
                if k.lower() not in ["authorization", "x-api-key", "cookie"]
            }
            extra["api_headers"] = safe_headers

        if params:
            extra["api_params"] = params

        if data and len(str(data)) < 1000:  # Only log small data
            extra["api_data"] = data

        self.info(
            f"Making API request: {method} {url}",
            operation="api_request",
            extra=extra,
            **kwargs,
        )

    def log_api_response(
        self,
        method: str,
        url: str,
        status_code: int,
        duration: float,
        *,
        response_data: Any | None = None,
        error: Exception | None = None,
        **kwargs,
    ) -> None:
        """Log API response."""
        extra = {
            "api_method": method,
            "api_url": url,
            "api_status_code": status_code,
        }

        if response_data and len(str(response_data)) < 1000:  # Only log small responses
            extra["api_response"] = response_data

        if status_code >= 400:
            self.error(
                f"API request failed: {method} {url} - {status_code}",
                operation="api_request",
                duration=duration,
                error=error,
                extra=extra,
                **kwargs,
            )
        else:
            self.info(
                f"API request succeeded: {method} {url} - {status_code}",
                operation="api_request",
                duration=duration,
                extra=extra,
                **kwargs,
            )

    def log_database_query(
        self,
        query: str,
        *,
        params: dict[str, Any] | None = None,
        table: str | None = None,
        duration: float | None = None,
        rows_affected: int | None = None,
        **kwargs,
    ) -> None:
        """Log database query."""
        extra = {
            "db_query": query[:500] + "..." if len(query) > 500 else query,
        }

        if params:
            extra["db_params"] = params

        if table:
            extra["db_table"] = table

        if rows_affected is not None:
            extra["db_rows_affected"] = rows_affected

        self.info(
            f"Database query executed on table: {table or 'unknown'}",
            operation="database_query",
            duration=duration,
            extra=extra,
            **kwargs,
        )

    def log_scraping_attempt(
        self, url: str, site: str, *, selector: str | None = None, **kwargs
    ) -> None:
        """Log web scraping attempt."""
        extra = {
            "scraping_url": url,
            "scraping_site": site,
        }

        if selector:
            extra["scraping_selector"] = selector

        self.info(
            f"Starting scraping: {site}", operation="scraping", extra=extra, **kwargs
        )

    def log_scraping_result(
        self,
        url: str,
        site: str,
        success: bool,
        duration: float,
        *,
        items_scraped: int | None = None,
        error: Exception | None = None,
        **kwargs,
    ) -> None:
        """Log web scraping result."""
        extra = {
            "scraping_url": url,
            "scraping_site": site,
            "scraping_success": success,
        }

        if items_scraped is not None:
            extra["scraping_items"] = items_scraped

        if success:
            self.info(
                f"Scraping completed: {site}",
                operation="scraping",
                duration=duration,
                extra=extra,
                **kwargs,
            )
        else:
            self.error(
                f"Scraping failed: {site}",
                operation="scraping",
                duration=duration,
                error=error,
                extra=extra,
                **kwargs,
            )

    def log_analysis_result(
        self,
        analysis_type: str,
        game_id: str,
        *,
        confidence: float | None = None,
        recommendation: str | None = None,
        signals: list[str] | None = None,
        **kwargs,
    ) -> None:
        """Log betting analysis result."""
        extra = {
            "analysis_type": analysis_type,
            "game_id": game_id,
        }

        if confidence is not None:
            extra["analysis_confidence"] = confidence

        if recommendation:
            extra["analysis_recommendation"] = recommendation

        if signals:
            extra["analysis_signals"] = signals

        self.info(
            f"Analysis completed: {analysis_type} for game {game_id}",
            operation="analysis",
            extra=extra,
            **kwargs,
        )

    def with_context(self, **context) -> "UnifiedLogger":
        """Create a new logger with additional context."""
        new_extra = {**self.extra_context, **context}
        return UnifiedLogger(
            name=self.name,
            component=self.component,
            correlation_id=self.correlation_id,
            extra_context=new_extra,
        )

    def with_correlation_id(self, correlation_id: str) -> "UnifiedLogger":
        """Create a new logger with different correlation ID."""
        return UnifiedLogger(
            name=self.name,
            component=self.component,
            correlation_id=correlation_id,
            extra_context=self.extra_context,
        )


class LoggingConfig:
    """Configuration for the unified logging system."""

    def __init__(
        self,
        *,
        log_level: LogLevel = LogLevel.INFO,
        log_format: str = "json",
        log_file: Path | None = None,
        log_rotation: bool = True,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        console_output: bool = True,
        structured_logging: bool = True,
        correlation_tracking: bool = True,
        performance_logging: bool = True,
        sensitive_data_masking: bool = True,
    ):
        """
        Initialize logging configuration.

        Args:
            log_level: Minimum log level
            log_format: Log format (json, text)
            log_file: Path to log file
            log_rotation: Enable log rotation
            max_file_size: Maximum file size before rotation
            backup_count: Number of backup files to keep
            console_output: Enable console output
            structured_logging: Enable structured logging
            correlation_tracking: Enable correlation ID tracking
            performance_logging: Enable performance metrics
            sensitive_data_masking: Enable sensitive data masking
        """
        self.log_level = log_level
        self.log_format = log_format
        self.log_file = log_file
        self.log_rotation = log_rotation
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.console_output = console_output
        self.structured_logging = structured_logging
        self.correlation_tracking = correlation_tracking
        self.performance_logging = performance_logging
        self.sensitive_data_masking = sensitive_data_masking


def setup_logging(config: LoggingConfig) -> None:
    """
    Set up the unified logging system.

    Args:
        config: Logging configuration
    """
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

    if config.structured_logging:
        if config.log_format == "json":
            processors.append(structlog.processors.JSONRenderer())
        else:
            processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level.value))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Set up console handler
    if config.console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.log_level.value))

        if config.log_format == "json":
            formatter = logging.Formatter("%(message)s")
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Set up file handler
    if config.log_file:
        config.log_file.parent.mkdir(parents=True, exist_ok=True)

        if config.log_rotation:
            file_handler = logging.handlers.RotatingFileHandler(
                config.log_file,
                maxBytes=config.max_file_size,
                backupCount=config.backup_count,
            )
        else:
            file_handler = logging.FileHandler(config.log_file)

        file_handler.setLevel(getattr(logging, config.log_level.value))

        if config.log_format == "json":
            formatter = logging.Formatter("%(message)s")
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def get_logger(
    name: str,
    component: LogComponent,
    *,
    correlation_id: str | None = None,
    extra_context: dict[str, Any] | None = None,
) -> UnifiedLogger:
    """
    Get a unified logger instance.

    Args:
        name: Logger name
        component: Component generating logs
        correlation_id: Correlation ID for request tracing
        extra_context: Additional context to include in all logs

    Returns:
        UnifiedLogger instance
    """
    return UnifiedLogger(
        name=name,
        component=component,
        correlation_id=correlation_id,
        extra_context=extra_context,
    )


# Legacy logger functions for backward compatibility
def get_mlb_sharp_logger(name: str, **kwargs) -> UnifiedLogger:
    """Get logger for mlb_sharp_betting module."""
    return get_logger(name, LogComponent.LEGACY_MLB_SHARP, **kwargs)


def get_sbr_logger(name: str, **kwargs) -> UnifiedLogger:
    """Get logger for sportsbookreview module."""
    return get_logger(name, LogComponent.LEGACY_SBR, **kwargs)


def get_action_logger(name: str, **kwargs) -> UnifiedLogger:
    """Get logger for action module."""
    return get_logger(name, LogComponent.LEGACY_ACTION, **kwargs)
