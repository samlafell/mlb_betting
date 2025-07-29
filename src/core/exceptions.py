"""
Unified Exception Handling

Consolidates exception patterns from:
- src/mlb_sharp_betting/core/exceptions.py
- sportsbookreview/exceptions.py (if exists)
- action/exceptions.py (if exists)

Provides comprehensive exception hierarchy with context, correlation IDs,
and detailed error information for debugging and monitoring.
"""

import traceback
from datetime import datetime
from typing import Any
from uuid import uuid4


class UnifiedBettingError(Exception):
    """
    Base exception for all unified betting system errors.

    Provides comprehensive error context, correlation tracking,
    and structured error information for monitoring and debugging.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
        correlation_id: str | None = None,
        component: str | None = None,
        operation: str | None = None,
        recoverable: bool = False,
        user_message: str | None = None,
    ):
        """
        Initialize unified betting error.

        Args:
            message: Technical error message
            error_code: Unique error code for categorization
            details: Additional error context and details
            cause: Original exception that caused this error
            correlation_id: Correlation ID for request tracing
            component: Component where error occurred
            operation: Operation being performed when error occurred
            recoverable: Whether error is recoverable
            user_message: User-friendly error message
        """
        super().__init__(message)

        self.message = message
        self.error_code = error_code or self._generate_error_code()
        self.details = details or {}
        self.cause = cause
        self.correlation_id = correlation_id or str(uuid4())
        self.component = component
        self.operation = operation
        self.recoverable = recoverable
        self.user_message = user_message or "An error occurred in the betting system"
        self.timestamp = datetime.now()
        self.traceback_info = traceback.format_exc() if cause else None

        # Add cause details if available
        if cause:
            self.details["cause_type"] = type(cause).__name__
            self.details["cause_message"] = str(cause)

    def _generate_error_code(self) -> str:
        """Generate error code based on exception class."""
        class_name = self.__class__.__name__
        # Convert CamelCase to UPPER_SNAKE_CASE
        import re

        error_code = re.sub("([a-z0-9])([A-Z])", r"\1_\2", class_name).upper()
        return error_code.replace("_ERROR", "")

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for logging/serialization."""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "user_message": self.user_message,
            "correlation_id": self.correlation_id,
            "component": self.component,
            "operation": self.operation,
            "recoverable": self.recoverable,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "traceback": self.traceback_info,
        }

    def __str__(self) -> str:
        """String representation with context."""
        parts = [f"{self.__class__.__name__}: {self.message}"]

        if self.error_code:
            parts.append(f"Code: {self.error_code}")

        if self.correlation_id:
            parts.append(f"ID: {self.correlation_id}")

        if self.component:
            parts.append(f"Component: {self.component}")

        if self.operation:
            parts.append(f"Operation: {self.operation}")

        return " | ".join(parts)


class ConfigurationError(UnifiedBettingError):
    """Raised when there are configuration-related errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message, component="configuration", error_code="CONFIG_ERROR", **kwargs
        )


class DatabaseError(UnifiedBettingError):
    """Raised when database operations fail."""

    def __init__(
        self,
        message: str,
        *,
        query: str | None = None,
        table: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if query:
            details["query"] = query
        if table:
            details["table"] = table
        if database:
            details["database"] = database

        super().__init__(
            message,
            component="database",
            error_code="DB_ERROR",
            details=details,
            **kwargs,
        )


class DataError(UnifiedBettingError):
    """Raised when data validation or processing fails."""

    def __init__(
        self,
        message: str,
        *,
        data_source: str | None = None,
        data_type: str | None = None,
        validation_errors: list[str] | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if data_source:
            details["data_source"] = data_source
        if data_type:
            details["data_type"] = data_type
        if validation_errors:
            details["validation_errors"] = validation_errors

        super().__init__(
            message,
            component="data_processing",
            error_code="DATA_ERROR",
            details=details,
            **kwargs,
        )


class APIError(UnifiedBettingError):
    """Raised when external API calls fail."""

    def __init__(
        self,
        message: str,
        *,
        api_name: str | None = None,
        endpoint: str | None = None,
        status_code: int | None = None,
        response_data: Any | None = None,
        request_data: Any | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if api_name:
            details["api_name"] = api_name
        if endpoint:
            details["endpoint"] = endpoint
        if status_code:
            details["status_code"] = status_code
        if response_data:
            details["response_data"] = response_data
        if request_data:
            details["request_data"] = request_data

        # API errors are often recoverable (temporary network issues)
        recoverable = kwargs.get("recoverable", True)

        super().__init__(
            message,
            component="api_client",
            error_code="API_ERROR",
            details=details,
            recoverable=recoverable,
            **kwargs,
        )


class ScrapingError(UnifiedBettingError):
    """Raised when web scraping operations fail."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        site: str | None = None,
        selector: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if url:
            details["url"] = url
        if site:
            details["site"] = site
        if selector:
            details["selector"] = selector

        super().__init__(
            message,
            component="scraper",
            error_code="SCRAPING_ERROR",
            details=details,
            recoverable=True,  # Scraping errors are usually recoverable
            **kwargs,
        )


class ParsingError(UnifiedBettingError):
    """Raised when data parsing fails."""

    def __init__(
        self,
        message: str,
        *,
        parser_type: str | None = None,
        input_data: str | None = None,
        expected_format: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if parser_type:
            details["parser_type"] = parser_type
        if input_data:
            # Truncate large input data for logging
            details["input_data"] = (
                input_data[:500] + "..." if len(input_data) > 500 else input_data
            )
        if expected_format:
            details["expected_format"] = expected_format

        super().__init__(
            message,
            component="parser",
            error_code="PARSING_ERROR",
            details=details,
            **kwargs,
        )


class ValidationError(UnifiedBettingError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        field_value: Any | None = None,
        validation_rule: str | None = None,
        validation_errors: list[str] | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if field_name:
            details["field_name"] = field_name
        if field_value is not None:
            details["field_value"] = str(field_value)
        if validation_rule:
            details["validation_rule"] = validation_rule
        if validation_errors:
            details["validation_errors"] = validation_errors

        super().__init__(
            message,
            component="validator",
            error_code="VALIDATION_ERROR",
            details=details,
            **kwargs,
        )


class AnalysisError(UnifiedBettingError):
    """Raised when betting analysis operations fail."""

    def __init__(
        self,
        message: str,
        *,
        analysis_type: str | None = None,
        game_id: str | None = None,
        strategy: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if analysis_type:
            details["analysis_type"] = analysis_type
        if game_id:
            details["game_id"] = game_id
        if strategy:
            details["strategy"] = strategy

        super().__init__(
            message,
            component="analysis",
            error_code="ANALYSIS_ERROR",
            details=details,
            **kwargs,
        )


class StrategyError(UnifiedBettingError):
    """Raised when betting strategy operations fail."""

    def __init__(
        self,
        message: str,
        *,
        strategy_name: str | None = None,
        processor: str | None = None,
        threshold_config: dict[str, Any] | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if strategy_name:
            details["strategy_name"] = strategy_name
        if processor:
            details["processor"] = processor
        if threshold_config:
            details["threshold_config"] = threshold_config

        super().__init__(
            message,
            component="strategy",
            error_code="STRATEGY_ERROR",
            details=details,
            **kwargs,
        )


class BacktestingError(UnifiedBettingError):
    """Raised when backtesting operations fail."""

    def __init__(
        self,
        message: str,
        *,
        backtest_id: str | None = None,
        strategy: str | None = None,
        date_range: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if backtest_id:
            details["backtest_id"] = backtest_id
        if strategy:
            details["strategy"] = strategy
        if date_range:
            details["date_range"] = date_range

        super().__init__(
            message,
            component="backtesting",
            error_code="BACKTEST_ERROR",
            details=details,
            **kwargs,
        )


class RateLimitError(UnifiedBettingError):
    """Raised when rate limits are exceeded."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        retry_after: int | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if service:
            details["service"] = service
        if retry_after:
            details["retry_after"] = retry_after

        super().__init__(
            message,
            component="rate_limiter",
            error_code="RATE_LIMIT_ERROR",
            details=details,
            recoverable=True,  # Rate limit errors are recoverable
            **kwargs,
        )


class TimeoutError(UnifiedBettingError):
    """Raised when operations timeout."""

    def __init__(
        self,
        message: str,
        *,
        timeout_duration: float | None = None,
        operation_type: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if timeout_duration:
            details["timeout_duration"] = timeout_duration
        if operation_type:
            details["operation_type"] = operation_type

        super().__init__(
            message,
            component="timeout_handler",
            error_code="TIMEOUT_ERROR",
            details=details,
            recoverable=True,  # Timeout errors are often recoverable
            **kwargs,
        )


class CircuitBreakerError(UnifiedBettingError):
    """Raised when circuit breaker is open."""

    def __init__(
        self,
        message: str,
        *,
        circuit_name: str | None = None,
        failure_count: int | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if circuit_name:
            details["circuit_name"] = circuit_name
        if failure_count:
            details["failure_count"] = failure_count

        super().__init__(
            message,
            component="circuit_breaker",
            error_code="CIRCUIT_BREAKER_ERROR",
            details=details,
            recoverable=True,  # Circuit breaker errors are recoverable
            **kwargs,
        )


class MonitoringError(UnifiedBettingError):
    """Raised when monitoring operations fail."""

    def __init__(
        self,
        message: str,
        *,
        monitor_type: str | None = None,
        endpoint: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if monitor_type:
            details["monitor_type"] = monitor_type
        if endpoint:
            details["endpoint"] = endpoint

        super().__init__(
            message,
            component="monitoring",
            error_code="MONITORING_ERROR",
            details=details,
            **kwargs,
        )


class PipelineExecutionError(UnifiedBettingError):
    """Raised when pipeline execution fails."""

    def __init__(
        self,
        message: str,
        *,
        pipeline_id: str | None = None,
        pipeline_type: str | None = None,
        stage: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if pipeline_id:
            details["pipeline_id"] = pipeline_id
        if pipeline_type:
            details["pipeline_type"] = pipeline_type
        if stage:
            details["stage"] = stage

        super().__init__(
            message,
            component="pipeline_orchestrator",
            error_code="PIPELINE_EXECUTION_ERROR",
            details=details,
            **kwargs,
        )


class WebSocketError(UnifiedBettingError):
    """Raised when WebSocket operations fail."""

    def __init__(
        self,
        message: str,
        *,
        connection_id: str | None = None,
        client_info: dict | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if connection_id:
            details["connection_id"] = connection_id
        if client_info:
            details["client_info"] = client_info

        super().__init__(
            message,
            component="websocket",
            error_code="WEBSOCKET_ERROR",
            details=details,
            recoverable=True,  # WebSocket errors are usually recoverable
            **kwargs,
        )


class OrchestrationError(UnifiedBettingError):
    """Raised when orchestration operations fail."""

    def __init__(
        self,
        message: str,
        *,
        orchestrator_id: str | None = None,
        orchestration_type: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if orchestrator_id:
            details["orchestrator_id"] = orchestrator_id
        if orchestration_type:
            details["orchestration_type"] = orchestration_type

        super().__init__(
            message,
            component="orchestration",
            error_code="ORCHESTRATION_ERROR",
            details=details,
            **kwargs,
        )


class AlertException(UnifiedBettingError):
    """Raised when alert operations fail."""

    def __init__(
        self,
        message: str,
        *,
        alert_type: str | None = None,
        alert_level: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if alert_type:
            details["alert_type"] = alert_type
        if alert_level:
            details["alert_level"] = alert_level

        super().__init__(
            message,
            component="alerting",
            error_code="ALERT_ERROR",
            details=details,
            **kwargs,
        )


class ReportGenerationException(UnifiedBettingError):
    """Raised when report generation fails."""

    def __init__(
        self,
        message: str,
        *,
        report_type: str | None = None,
        output_format: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if report_type:
            details["report_type"] = report_type
        if output_format:
            details["output_format"] = output_format

        super().__init__(
            message,
            component="reporting",
            error_code="REPORT_GENERATION_ERROR",
            details=details,
            **kwargs,
        )


class PipelineError(UnifiedBettingError):
    """Raised when pipeline operations fail."""

    def __init__(
        self,
        message: str,
        *,
        pipeline_name: str | None = None,
        stage_name: str | None = None,
        **kwargs,
    ):
        details = kwargs.get("details", {})

        if pipeline_name:
            details["pipeline_name"] = pipeline_name
        if stage_name:
            details["stage_name"] = stage_name

        super().__init__(
            message,
            component="pipeline",
            error_code="PIPELINE_ERROR",
            details=details,
            **kwargs,
        )


# Legacy exception aliases for backward compatibility
# MLBSharpBettingError removed - mlb_sharp_betting directory cleanup
SportsbookReviewError = UnifiedBettingError  # For sportsbookreview module compatibility
ActionNetworkError = UnifiedBettingError  # For action module compatibility
MonitoringException = MonitoringError  # Alias for consistency


def handle_exception(
    exc: Exception,
    *,
    component: str | None = None,
    operation: str | None = None,
    correlation_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> UnifiedBettingError:
    """
    Convert any exception to a UnifiedBettingError with context.

    Args:
        exc: Original exception
        component: Component where error occurred
        operation: Operation being performed
        correlation_id: Correlation ID for tracing
        details: Additional error details

    Returns:
        UnifiedBettingError with full context
    """
    if isinstance(exc, UnifiedBettingError):
        # Already a unified error, just update context if provided
        if component and not exc.component:
            exc.component = component
        if operation and not exc.operation:
            exc.operation = operation
        if correlation_id and not exc.correlation_id:
            exc.correlation_id = correlation_id
        if details:
            exc.details.update(details)
        return exc

    # Convert other exceptions to unified errors
    error_message = f"Unhandled {type(exc).__name__}: {str(exc)}"

    return UnifiedBettingError(
        error_message,
        cause=exc,
        component=component,
        operation=operation,
        correlation_id=correlation_id,
        details=details or {},
        error_code="UNHANDLED_ERROR",
    )
