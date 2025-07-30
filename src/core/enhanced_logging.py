#!/usr/bin/env python3
"""
Enhanced Logging Service with OpenTelemetry Integration

Extends the existing unified logging system with:
- OpenTelemetry distributed tracing
- Enhanced correlation ID management
- Performance timing and profiling
- Structured log aggregation preparation
- Pipeline-specific context tracking
"""

import contextvars
import re
import time
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

from .logging import LogComponent, UnifiedLogger, get_logger

# Context variables for correlation tracking
correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=None
)
pipeline_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "pipeline_id", default=None
)
operation_context_var: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "operation_context", default=None
)


@dataclass
class OperationContext:
    """Context for tracking operation execution."""

    operation_id: str
    operation_name: str
    correlation_id: str
    pipeline_id: str | None = None
    parent_operation: str | None = None
    start_time: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "operation_id": self.operation_id,
            "operation_name": self.operation_name,
            "correlation_id": self.correlation_id,
            "pipeline_id": self.pipeline_id,
            "parent_operation": self.parent_operation,
            "start_time": self.start_time,
            "metadata": self.metadata,
            "tags": self.tags,
        }


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""

    duration: float
    cpu_time: float | None = None
    memory_delta: int | None = None
    database_queries: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    cache_misses: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "duration": self.duration,
            "cpu_time": self.cpu_time,
            "memory_delta": self.memory_delta,
            "database_queries": self.database_queries,
            "api_calls": self.api_calls,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "performance_class": self._classify_performance(),
        }

    def _classify_performance(self) -> str:
        """Classify performance based on duration."""
        if self.duration < 0.1:
            return "excellent"
        elif self.duration < 0.5:
            return "good"
        elif self.duration < 2.0:
            return "acceptable"
        elif self.duration < 10.0:
            return "slow"
        else:
            return "critical"


class EnhancedLoggingService:
    """
    Enhanced logging service with OpenTelemetry integration.

    Provides comprehensive observability including:
    - Distributed tracing with OpenTelemetry
    - Enhanced correlation ID management across async operations
    - Performance profiling and timing
    - Structured log aggregation preparation
    - Pipeline-specific context tracking
    """

    def __init__(
        self,
        service_name: str = "mlb-betting-system",
        otlp_endpoint: str | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the enhanced logging service."""
        self.service_name = service_name
        self.enable_tracing = enable_tracing

        # Initialize OpenTelemetry if enabled
        if enable_tracing and otlp_endpoint:
            self._setup_opentelemetry(otlp_endpoint)

        # Get tracer
        self.tracer = trace.get_tracer(__name__)

        # Operation tracking
        self.active_operations: dict[str, OperationContext] = {}

        # Base logger
        self.logger = get_logger(__name__, LogComponent.MONITORING)

        self.logger.info(
            "Enhanced logging service initialized",
            service_name=service_name,
            tracing_enabled=enable_tracing,
            otlp_endpoint=otlp_endpoint,
        )

    def _validate_otlp_endpoint(self, endpoint: str) -> bool:
        """Validate OTLP endpoint URL for security."""
        try:
            # Parse the URL
            parsed = urlparse(endpoint)

            # Check for valid scheme
            if parsed.scheme not in ["http", "https", "grpc"]:
                self.logger.error(f"Invalid OTLP endpoint scheme: {parsed.scheme}")
                return False

            # Check for valid hostname (no localhost/private IPs in production)
            if not parsed.hostname:
                self.logger.error("OTLP endpoint missing hostname")
                return False

            # Basic hostname validation - no obviously malicious patterns
            hostname_pattern = re.compile(
                r"^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$"
            )
            if not hostname_pattern.match(parsed.hostname):
                self.logger.error(f"Invalid OTLP endpoint hostname: {parsed.hostname}")
                return False

            # Check port range
            if parsed.port and (parsed.port < 1 or parsed.port > 65535):
                self.logger.error(f"Invalid OTLP endpoint port: {parsed.port}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating OTLP endpoint: {e}")
            return False

    def _setup_opentelemetry(self, otlp_endpoint: str):
        """Setup OpenTelemetry tracing with proper resource management and validation."""
        span_processor = None
        otlp_exporter = None
        tracer_provider = None

        try:
            # Validate endpoint URL
            if not self._validate_otlp_endpoint(otlp_endpoint):
                raise ValueError(f"Invalid OTLP endpoint: {otlp_endpoint}")

            # Configure tracer provider
            tracer_provider = TracerProvider()
            trace.set_tracer_provider(tracer_provider)

            # Configure OTLP exporter
            otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            span_processor = BatchSpanProcessor(otlp_exporter)

            tracer_provider.add_span_processor(span_processor)

            self.logger.info(
                "OpenTelemetry tracing configured", otlp_endpoint=otlp_endpoint
            )

        except ValueError as e:
            self.logger.error(
                "Invalid OTLP endpoint configuration",
                error=str(e),
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise
        except ConnectionError as e:
            self.logger.error(
                "Failed to connect to OTLP endpoint",
                error=str(e),
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise
        except (ImportError, ModuleNotFoundError) as e:
            self.logger.error(
                "OpenTelemetry dependencies not installed",
                error=str(e),
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise
        except (TimeoutError, OSError) as e:
            self.logger.error(
                "Network error setting up OpenTelemetry",
                error=str(e),
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise
        except RuntimeError as e:
            self.logger.error(
                "Runtime error during OpenTelemetry setup",
                error=str(e),
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise
        except Exception as e:
            self.logger.error(
                "Unexpected error setting up OpenTelemetry tracing",
                error=str(e),
                error_type=type(e).__name__,
                endpoint=otlp_endpoint,
            )
            self._cleanup_opentelemetry_resources(
                span_processor, otlp_exporter, tracer_provider
            )
            raise

    def _cleanup_opentelemetry_resources(
        self, span_processor, otlp_exporter, tracer_provider
    ):
        """Clean up OpenTelemetry resources in proper order to prevent memory leaks."""
        # Clean up span processor first
        if span_processor:
            try:
                span_processor.shutdown(timeout=5)  # Give it 5 seconds to finish
            except Exception as e:
                self.logger.warning(f"Error shutting down span processor: {e}")

        # Clean up exporter
        if otlp_exporter:
            try:
                otlp_exporter.shutdown(timeout=5)  # Give it 5 seconds to finish
            except Exception as e:
                self.logger.warning(f"Error shutting down OTLP exporter: {e}")

        # Clean up tracer provider
        if tracer_provider:
            try:
                tracer_provider.shutdown()
            except Exception as e:
                self.logger.warning(f"Error shutting down tracer provider: {e}")

    def get_correlation_id(self) -> str:
        """Get current correlation ID or create new one."""
        correlation_id = correlation_id_var.get()
        if not correlation_id:
            correlation_id = str(uuid4())
            correlation_id_var.set(correlation_id)
        return correlation_id

    def get_pipeline_id(self) -> str | None:
        """Get current pipeline ID."""
        return pipeline_id_var.get()

    def set_pipeline_context(self, pipeline_id: str, pipeline_type: str):
        """Set pipeline context for current execution."""
        pipeline_id_var.set(pipeline_id)

        context = operation_context_var.get() or {}
        context.update({"pipeline_id": pipeline_id, "pipeline_type": pipeline_type})
        operation_context_var.set(context)

    def get_enhanced_logger(
        self, name: str, component: LogComponent, correlation_id: str | None = None
    ) -> UnifiedLogger:
        """Get enhanced logger with current context."""
        if not correlation_id:
            correlation_id = self.get_correlation_id()

        pipeline_id = self.get_pipeline_id()
        operation_context = operation_context_var.get() or {}

        extra_context = {"pipeline_id": pipeline_id, **operation_context}

        return get_logger(
            name=name,
            component=component,
            correlation_id=correlation_id,
            extra_context=extra_context,
        )

    @contextmanager
    def operation_context(
        self,
        operation_name: str,
        *,
        operation_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> Generator[OperationContext, None, None]:
        """Context manager for tracking operations with tracing and proper cleanup."""

        if not operation_id:
            operation_id = str(uuid4())

        if not correlation_id:
            correlation_id = self.get_correlation_id()

        pipeline_id = self.get_pipeline_id()

        # Create operation context
        op_context = OperationContext(
            operation_id=operation_id,
            operation_name=operation_name,
            correlation_id=correlation_id,
            pipeline_id=pipeline_id,
            metadata=metadata or {},
            tags=tags or {},
        )

        # Set context variables
        token_correlation = correlation_id_var.set(correlation_id)

        current_op_context = operation_context_var.get() or {}
        new_op_context = {**current_op_context, **op_context.to_dict()}
        token_op_context = operation_context_var.set(new_op_context)

        # Start tracing span
        span_context = None
        if self.enable_tracing:
            span_context = self.tracer.start_span(operation_name)
            attributes = {
                "operation.id": operation_id,
                "operation.name": operation_name,
                "correlation.id": correlation_id,
                "pipeline.id": pipeline_id or "",
            }
            if tags:
                attributes.update(tags)
            span_context.set_attributes(attributes)

        # Track active operation
        self.active_operations[operation_id] = op_context

        start_time = time.time()

        try:
            # Log operation start
            logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)
            logger.info(
                f"Starting operation: {operation_name}",
                operation=operation_name,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "start",
                },
            )

            yield op_context

            # Calculate metrics
            duration = time.time() - start_time
            metrics = PerformanceMetrics(duration=duration)

            # Log successful completion
            logger.info(
                f"Completed operation: {operation_name}",
                operation=operation_name,
                duration=duration,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "complete",
                    "performance_metrics": metrics.to_dict(),
                },
            )

            # Set span status as OK
            if span_context:
                span_context.set_status(Status(StatusCode.OK))
                span_context.set_attributes(
                    {"operation.duration": duration, "operation.status": "success"}
                )

        except Exception as e:
            # Calculate metrics for failed operation
            duration = time.time() - start_time
            metrics = PerformanceMetrics(duration=duration)

            # Log operation failure
            logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)
            logger.error(
                f"Failed operation: {operation_name}",
                operation=operation_name,
                duration=duration,
                error=e,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "error",
                    "performance_metrics": metrics.to_dict(),
                },
            )

            # Set span status as error
            if span_context:
                span_context.set_status(Status(StatusCode.ERROR, str(e)))
                span_context.set_attributes(
                    {
                        "operation.duration": duration,
                        "operation.status": "error",
                        "error.message": str(e),
                        "error.type": type(e).__name__,
                    }
                )

            raise

        finally:
            # Clean up resources and context variables in proper order
            cleanup_errors = []

            # End span first to avoid memory leaks
            if span_context:
                try:
                    span_context.end()
                except Exception as e:
                    cleanup_errors.append(f"Error ending span: {e}")

            # Remove from active operations tracking
            try:
                self.active_operations.pop(operation_id, None)
            except Exception as e:
                cleanup_errors.append(f"Error removing active operation: {e}")

            # Reset context variables to prevent memory leaks (critical for long-running processes)
            try:
                correlation_id_var.reset(token_correlation)
            except (LookupError, RuntimeError) as e:
                cleanup_errors.append(
                    f"Error resetting correlation_id context variable: {e}"
                )
            except Exception as e:
                cleanup_errors.append(f"Unexpected error resetting correlation_id: {e}")

            try:
                operation_context_var.reset(token_op_context)
            except (LookupError, RuntimeError) as e:
                cleanup_errors.append(
                    f"Error resetting operation_context variable: {e}"
                )
            except Exception as e:
                cleanup_errors.append(
                    f"Unexpected error resetting operation_context: {e}"
                )

            # Log all cleanup errors at once if any occurred
            if cleanup_errors:
                self.logger.warning(
                    f"Context cleanup completed with {len(cleanup_errors)} errors",
                    cleanup_errors=cleanup_errors,
                    operation_id=operation_id,
                )

    @asynccontextmanager
    async def async_operation_context(
        self,
        operation_name: str,
        *,
        operation_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> AsyncGenerator[OperationContext, None]:
        """Async context manager for tracking operations with tracing."""

        if not operation_id:
            operation_id = str(uuid4())

        if not correlation_id:
            correlation_id = self.get_correlation_id()

        pipeline_id = self.get_pipeline_id()

        # Create operation context
        op_context = OperationContext(
            operation_id=operation_id,
            operation_name=operation_name,
            correlation_id=correlation_id,
            pipeline_id=pipeline_id,
            metadata=metadata or {},
            tags=tags or {},
        )

        # Set context variables
        token_correlation = correlation_id_var.set(correlation_id)

        current_op_context = operation_context_var.get() or {}
        new_op_context = {**current_op_context, **op_context.to_dict()}
        token_op_context = operation_context_var.set(new_op_context)

        # Start tracing span
        span_context = None
        if self.enable_tracing:
            span_context = self.tracer.start_span(operation_name)
            attributes = {
                "operation.id": operation_id,
                "operation.name": operation_name,
                "correlation.id": correlation_id,
                "pipeline.id": pipeline_id or "",
            }
            if tags:
                attributes.update(tags)
            span_context.set_attributes(attributes)

        # Track active operation
        self.active_operations[operation_id] = op_context

        start_time = time.time()

        try:
            # Log operation start
            logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)
            logger.info(
                f"Starting async operation: {operation_name}",
                operation=operation_name,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "start",
                    "async_operation": True,
                },
            )

            yield op_context

            # Calculate metrics
            duration = time.time() - start_time
            metrics = PerformanceMetrics(duration=duration)

            # Log successful completion
            logger.info(
                f"Completed async operation: {operation_name}",
                operation=operation_name,
                duration=duration,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "complete",
                    "async_operation": True,
                    "performance_metrics": metrics.to_dict(),
                },
            )

            # Set span status as OK
            if span_context:
                span_context.set_status(Status(StatusCode.OK))
                span_context.set_attributes(
                    {"operation.duration": duration, "operation.status": "success"}
                )

        except Exception as e:
            # Calculate metrics for failed operation
            duration = time.time() - start_time
            metrics = PerformanceMetrics(duration=duration)

            # Log operation failure
            logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)
            logger.error(
                f"Failed async operation: {operation_name}",
                operation=operation_name,
                duration=duration,
                error=e,
                extra={
                    "operation_context": op_context.to_dict(),
                    "operation_phase": "error",
                    "async_operation": True,
                    "performance_metrics": metrics.to_dict(),
                },
            )

            # Set span status as error
            if span_context:
                span_context.set_status(Status(StatusCode.ERROR, str(e)))
                span_context.set_attributes(
                    {
                        "operation.duration": duration,
                        "operation.status": "error",
                        "error.message": str(e),
                        "error.type": type(e).__name__,
                    }
                )

            raise

        finally:
            # Clean up resources and context variables in proper order (async version)
            cleanup_errors = []

            # End span first to avoid memory leaks
            if span_context:
                try:
                    span_context.end()
                except Exception as e:
                    cleanup_errors.append(f"Error ending async span: {e}")

            # Remove from active operations tracking
            try:
                self.active_operations.pop(operation_id, None)
            except Exception as e:
                cleanup_errors.append(f"Error removing active async operation: {e}")

            # Reset context variables to prevent memory leaks (critical for long-running async processes)
            try:
                correlation_id_var.reset(token_correlation)
            except (LookupError, RuntimeError) as e:
                cleanup_errors.append(
                    f"Error resetting async correlation_id context variable: {e}"
                )
            except Exception as e:
                cleanup_errors.append(
                    f"Unexpected error resetting async correlation_id: {e}"
                )

            try:
                operation_context_var.reset(token_op_context)
            except (LookupError, RuntimeError) as e:
                cleanup_errors.append(
                    f"Error resetting async operation_context variable: {e}"
                )
            except Exception as e:
                cleanup_errors.append(
                    f"Unexpected error resetting async operation_context: {e}"
                )

            # Log all cleanup errors at once if any occurred
            if cleanup_errors:
                self.logger.warning(
                    f"Async context cleanup completed with {len(cleanup_errors)} errors",
                    cleanup_errors=cleanup_errors,
                    operation_id=operation_id,
                )

    def log_pipeline_event(
        self,
        event_type: str,
        pipeline_id: str,
        pipeline_type: str,
        *,
        stage: str | None = None,
        status: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Log pipeline-specific events with full context."""
        logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)

        extra = {
            "event_type": event_type,
            "pipeline_id": pipeline_id,
            "pipeline_type": pipeline_type,
            "pipeline_event": True,
        }

        if stage:
            extra["pipeline_stage"] = stage

        if status:
            extra["pipeline_status"] = status

        if metadata:
            extra["pipeline_metadata"] = metadata

        logger.info(
            f"Pipeline event: {event_type} for {pipeline_type}",
            operation="pipeline_event",
            extra=extra,
        )

    def log_performance_metrics(
        self,
        operation_name: str,
        metrics: PerformanceMetrics,
        *,
        correlation_id: str | None = None,
    ):
        """Log detailed performance metrics."""
        logger = self.get_enhanced_logger(__name__, LogComponent.MONITORING)

        logger.info(
            f"Performance metrics for {operation_name}",
            operation=operation_name,
            duration=metrics.duration,
            extra={"performance_metrics": metrics.to_dict(), "metrics_event": True},
        )

    def get_active_operations(self) -> dict[str, OperationContext]:
        """Get currently active operations."""
        return self.active_operations.copy()

    def get_operation_summary(self) -> dict[str, Any]:
        """Get summary of operation activity."""
        active_ops = self.get_active_operations()

        return {
            "active_operations_count": len(active_ops),
            "active_operations": [
                {
                    "operation_id": op.operation_id,
                    "operation_name": op.operation_name,
                    "duration": time.time() - op.start_time,
                    "pipeline_id": op.pipeline_id,
                }
                for op in active_ops.values()
            ],
            "correlation_id": self.get_correlation_id(),
            "pipeline_id": self.get_pipeline_id(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# Global enhanced logging service instance
_enhanced_logging_service: EnhancedLoggingService | None = None


def get_enhanced_logging_service(
    service_name: str = "mlb-betting-system",
    otlp_endpoint: str | None = None,
    enable_tracing: bool = True,
) -> EnhancedLoggingService:
    """Get or create the global enhanced logging service instance."""
    global _enhanced_logging_service
    if _enhanced_logging_service is None:
        _enhanced_logging_service = EnhancedLoggingService(
            service_name=service_name,
            otlp_endpoint=otlp_endpoint,
            enable_tracing=enable_tracing,
        )
    return _enhanced_logging_service


# Convenience functions for common usage patterns
def get_contextual_logger(name: str, component: LogComponent) -> UnifiedLogger:
    """Get logger with current operation context."""
    service = get_enhanced_logging_service()
    return service.get_enhanced_logger(name, component)


def operation_context(operation_name: str, **kwargs):
    """Shortcut for operation context manager."""
    service = get_enhanced_logging_service()
    return service.operation_context(operation_name, **kwargs)


def async_operation_context(operation_name: str, **kwargs):
    """Shortcut for async operation context manager."""
    service = get_enhanced_logging_service()
    return service.async_operation_context(operation_name, **kwargs)


def set_pipeline_context(pipeline_id: str, pipeline_type: str):
    """Set pipeline context for current execution."""
    service = get_enhanced_logging_service()
    service.set_pipeline_context(pipeline_id, pipeline_type)


def log_pipeline_event(event_type: str, pipeline_id: str, pipeline_type: str, **kwargs):
    """Log pipeline event with full context."""
    service = get_enhanced_logging_service()
    service.log_pipeline_event(event_type, pipeline_id, pipeline_type, **kwargs)
