#!/usr/bin/env python3
"""
Unit tests for EnhancedLoggingService

Tests comprehensive logging and tracing infrastructure including:
- Correlation ID management and propagation
- Operation context managers (sync and async)
- OpenTelemetry integration and span management
- Performance timing and metrics classification
- Pipeline event logging with metadata
- Error handling and span status management
"""

import time
from contextvars import ContextVar
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.enhanced_logging import (
    EnhancedLoggingService,
    OperationContext,
    PerformanceMetrics,
)


@pytest.fixture
def enhanced_logging():
    """Create an EnhancedLoggingService instance for testing."""
    return EnhancedLoggingService()


@pytest.fixture
def mock_logger():
    """Create a mock logger for testing."""
    return Mock()


@pytest.fixture
def mock_tracer():
    """Create a mock OpenTelemetry tracer."""
    mock_tracer = Mock()
    mock_span = Mock()
    mock_tracer.start_span.return_value.__enter__ = Mock(return_value=mock_span)
    mock_tracer.start_span.return_value.__exit__ = Mock(return_value=None)
    return mock_tracer


class TestEnhancedLoggingServiceInitialization:
    """Test enhanced logging service initialization."""

    def test_service_initialization(self, enhanced_logging):
        """Test that service initializes with all required components."""
        assert enhanced_logging is not None
        assert hasattr(enhanced_logging, "tracer")
        assert hasattr(enhanced_logging, "logger")
        assert hasattr(enhanced_logging, "performance_thresholds")

    def test_performance_thresholds(self, enhanced_logging):
        """Test that performance thresholds are properly defined."""
        thresholds = enhanced_logging.performance_thresholds

        assert "excellent" in thresholds
        assert "good" in thresholds
        assert "acceptable" in thresholds
        assert "slow" in thresholds

        # Verify threshold ordering
        assert thresholds["excellent"] < thresholds["good"]
        assert thresholds["good"] < thresholds["acceptable"]
        assert thresholds["acceptable"] < thresholds["slow"]

    def test_correlation_id_context_variable(self, enhanced_logging):
        """Test that correlation ID context variable is properly initialized."""
        assert hasattr(enhanced_logging, "correlation_id_var")
        assert isinstance(enhanced_logging.correlation_id_var, ContextVar)


class TestCorrelationIDManagement:
    """Test correlation ID generation and management."""

    def test_generate_correlation_id(self, enhanced_logging):
        """Test correlation ID generation."""
        correlation_id = enhanced_logging.generate_correlation_id()

        assert isinstance(correlation_id, str)
        assert len(correlation_id) > 0
        assert "-" in correlation_id  # UUID format

    def test_generate_unique_correlation_ids(self, enhanced_logging):
        """Test that correlation IDs are unique."""
        id1 = enhanced_logging.generate_correlation_id()
        id2 = enhanced_logging.generate_correlation_id()

        assert id1 != id2

    def test_get_current_correlation_id_none(self, enhanced_logging):
        """Test getting correlation ID when none is set."""
        correlation_id = enhanced_logging.get_current_correlation_id()
        assert correlation_id is None

    def test_get_current_correlation_id_with_context(self, enhanced_logging):
        """Test getting correlation ID from context."""
        test_id = "test-correlation-123"
        enhanced_logging.correlation_id_var.set(test_id)

        correlation_id = enhanced_logging.get_current_correlation_id()
        assert correlation_id == test_id

    def test_set_correlation_id(self, enhanced_logging):
        """Test setting correlation ID in context."""
        test_id = "test-set-correlation-456"

        enhanced_logging.set_correlation_id(test_id)

        # Verify it was set
        current_id = enhanced_logging.correlation_id_var.get()
        assert current_id == test_id


class TestOperationContext:
    """Test OperationContext data class."""

    def test_operation_context_creation(self):
        """Test creating OperationContext with all fields."""
        context = OperationContext(
            operation_id="test-op-123",
            operation_name="test_operation",
            correlation_id="test-corr-456",
            start_time=time.time(),
            metadata={"key": "value"},
            span=Mock(),
        )

        assert context.operation_id == "test-op-123"
        assert context.operation_name == "test_operation"
        assert context.correlation_id == "test-corr-456"
        assert isinstance(context.start_time, (int, float))
        assert context.metadata == {"key": "value"}
        assert context.span is not None

    def test_operation_context_minimal(self):
        """Test creating OperationContext with minimal fields."""
        context = OperationContext(
            operation_id="test-op",
            operation_name="test",
            correlation_id="test-corr",
            start_time=time.time(),
        )

        assert context.metadata is None
        assert context.span is None


class TestPerformanceMetrics:
    """Test performance metrics and classification."""

    def test_performance_metrics_creation(self):
        """Test creating PerformanceMetrics with timing data."""
        metrics = PerformanceMetrics(
            duration=1.25,
            cpu_time=0.95,
            memory_peak=1024,
            io_operations=15,
            cache_hits=8,
            cache_misses=2,
        )

        assert metrics.duration == 1.25
        assert metrics.cpu_time == 0.95
        assert metrics.memory_peak == 1024
        assert metrics.io_operations == 15
        assert metrics.cache_hits == 8
        assert metrics.cache_misses == 2

    def test_performance_metrics_to_dict(self):
        """Test converting performance metrics to dictionary."""
        metrics = PerformanceMetrics(duration=0.5)
        result = metrics.to_dict()

        assert isinstance(result, dict)
        assert "duration" in result
        assert "performance_class" in result
        assert result["duration"] == 0.5

    def test_performance_classification_excellent(self):
        """Test excellent performance classification."""
        metrics = PerformanceMetrics(duration=0.05)  # 50ms - should be excellent
        classification = metrics._classify_performance()

        assert classification == "excellent"

    def test_performance_classification_good(self):
        """Test good performance classification."""
        metrics = PerformanceMetrics(duration=0.25)  # 250ms - should be good
        classification = metrics._classify_performance()

        assert classification == "good"

    def test_performance_classification_acceptable(self):
        """Test acceptable performance classification."""
        metrics = PerformanceMetrics(duration=1.5)  # 1.5s - should be acceptable
        classification = metrics._classify_performance()

        assert classification == "acceptable"

    def test_performance_classification_slow(self):
        """Test slow performance classification."""
        metrics = PerformanceMetrics(duration=5.0)  # 5s - should be slow
        classification = metrics._classify_performance()

        assert classification == "slow"

    def test_performance_classification_critical(self):
        """Test critical performance classification."""
        metrics = PerformanceMetrics(duration=15.0)  # 15s - should be critical
        classification = metrics._classify_performance()

        assert classification == "critical"


class TestSyncOperationContext:
    """Test synchronous operation context manager."""

    @patch("src.core.enhanced_logging.trace")
    def test_sync_operation_context_basic(self, mock_trace, enhanced_logging):
        """Test basic synchronous operation context."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__enter__ = Mock(return_value=mock_span)
        mock_tracer.start_span.return_value.__exit__ = Mock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        with enhanced_logging.operation_context("test_operation") as context:
            assert isinstance(context, OperationContext)
            assert context.operation_name == "test_operation"
            assert context.operation_id is not None
            assert context.correlation_id is not None
            assert context.start_time is not None

    @patch("src.core.enhanced_logging.trace")
    def test_sync_operation_context_with_metadata(self, mock_trace, enhanced_logging):
        """Test synchronous operation context with metadata."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__enter__ = Mock(return_value=mock_span)
        mock_tracer.start_span.return_value.__exit__ = Mock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        metadata = {"pipeline_id": "test-123", "stage": "data_collection"}

        with enhanced_logging.operation_context(
            "test_operation", metadata=metadata
        ) as context:
            assert context.metadata == metadata

    @patch("src.core.enhanced_logging.trace")
    def test_sync_operation_context_with_existing_correlation_id(
        self, mock_trace, enhanced_logging
    ):
        """Test using existing correlation ID in sync context."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__enter__ = Mock(return_value=mock_span)
        mock_tracer.start_span.return_value.__exit__ = Mock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        existing_id = "existing-correlation-123"

        with enhanced_logging.operation_context(
            "test_operation", correlation_id=existing_id
        ) as context:
            assert context.correlation_id == existing_id


class TestAsyncOperationContext:
    """Test asynchronous operation context manager."""

    @patch("src.core.enhanced_logging.trace")
    @pytest.mark.asyncio
    async def test_async_operation_context_basic(self, mock_trace, enhanced_logging):
        """Test basic asynchronous operation context."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__aenter__ = AsyncMock(
            return_value=mock_span
        )
        mock_tracer.start_span.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        async with enhanced_logging.async_operation_context(
            "async_test_operation"
        ) as context:
            assert isinstance(context, OperationContext)
            assert context.operation_name == "async_test_operation"
            assert context.operation_id is not None
            assert context.correlation_id is not None

    @patch("src.core.enhanced_logging.trace")
    @pytest.mark.asyncio
    async def test_async_operation_context_with_tags(
        self, mock_trace, enhanced_logging
    ):
        """Test asynchronous operation context with tags."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__aenter__ = AsyncMock(
            return_value=mock_span
        )
        mock_tracer.start_span.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        tags = {"service": "betting_system", "version": "1.0"}

        async with enhanced_logging.async_operation_context(
            "async_test_operation", tags=tags
        ) as context:
            assert context.operation_name == "async_test_operation"
            # Tags would be applied to span internally


class TestPipelineEventLogging:
    """Test pipeline-specific event logging."""

    def test_log_pipeline_start(self, enhanced_logging):
        """Test pipeline start logging."""
        pipeline_id = "test-pipeline-123"
        pipeline_type = "full_data_collection"
        correlation_id = "test-correlation-456"

        # Should not raise any exceptions
        enhanced_logging.log_pipeline_start(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
        )

    def test_log_pipeline_start_with_metadata(self, enhanced_logging):
        """Test pipeline start logging with metadata."""
        pipeline_id = "test-pipeline-456"
        pipeline_type = "analysis_only"
        correlation_id = "test-correlation-789"
        metadata = {"expected_games": 15, "source": "action_network"}

        enhanced_logging.log_pipeline_start(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
            metadata=metadata,
        )

    def test_log_pipeline_complete(self, enhanced_logging):
        """Test pipeline completion logging."""
        pipeline_id = "test-pipeline-complete"
        pipeline_type = "data_collection"
        correlation_id = "test-correlation-complete"
        duration = 12.5
        status = "success"

        enhanced_logging.log_pipeline_complete(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
            duration=duration,
            status=status,
        )

    def test_log_pipeline_complete_with_results(self, enhanced_logging):
        """Test pipeline completion logging with results."""
        pipeline_id = "test-pipeline-results"
        pipeline_type = "full_analysis"
        correlation_id = "test-correlation-results"
        duration = 8.2
        status = "success"
        results = {"games_processed": 12, "opportunities_found": 3}

        enhanced_logging.log_pipeline_complete(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
            duration=duration,
            status=status,
            results=results,
        )

    def test_log_pipeline_failed(self, enhanced_logging):
        """Test pipeline failure logging."""
        pipeline_id = "test-pipeline-failed"
        pipeline_type = "data_collection"
        correlation_id = "test-correlation-failed"
        duration = 5.1
        error = "Connection timeout to external API"

        enhanced_logging.log_pipeline_failed(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
            duration=duration,
            error=error,
        )

    def test_log_pipeline_failed_with_details(self, enhanced_logging):
        """Test pipeline failure logging with error details."""
        pipeline_id = "test-pipeline-error-details"
        pipeline_type = "analysis"
        correlation_id = "test-correlation-error"
        duration = 3.7
        error = "Database connection lost"
        error_details = {"error_code": "DB_CONNECTION_LOST", "retry_count": 3}

        enhanced_logging.log_pipeline_failed(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            correlation_id=correlation_id,
            duration=duration,
            error=error,
            error_details=error_details,
        )


class TestErrorHandlingAndSpanStatus:
    """Test error handling and OpenTelemetry span status management."""

    @patch("src.core.enhanced_logging.trace")
    def test_span_error_handling_in_context(self, mock_trace, enhanced_logging):
        """Test that exceptions are properly handled in operation context."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__enter__ = Mock(return_value=mock_span)
        mock_tracer.start_span.return_value.__exit__ = Mock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        with pytest.raises(ValueError):
            with enhanced_logging.operation_context("test_error_operation"):
                raise ValueError("Test error for span handling")

        # Verify span exit was called (which should handle the error)
        mock_tracer.start_span.return_value.__exit__.assert_called_once()

    @patch("src.core.enhanced_logging.trace")
    @pytest.mark.asyncio
    async def test_async_span_error_handling(self, mock_trace, enhanced_logging):
        """Test that exceptions are properly handled in async operation context."""
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value.__aenter__ = AsyncMock(
            return_value=mock_span
        )
        mock_tracer.start_span.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_trace.get_tracer.return_value = mock_tracer

        enhanced_logging.tracer = mock_tracer

        with pytest.raises(RuntimeError):
            async with enhanced_logging.async_operation_context(
                "async_error_operation"
            ):
                raise RuntimeError("Test async error for span handling")

        # Verify async span exit was called
        mock_tracer.start_span.return_value.__aexit__.assert_called_once()


class TestPerformanceMetrics:
    """Test performance timing and metrics recording."""

    def test_log_performance_metrics(self, enhanced_logging):
        """Test performance metrics logging."""
        operation = "data_collection"
        metrics = PerformanceMetrics(duration=1.25)

        # Should not raise any exceptions
        enhanced_logging.log_performance_metrics(operation, metrics)

    def test_log_performance_metrics_with_metadata(self, enhanced_logging):
        """Test performance metrics logging with additional data."""
        operation = "api_call"
        metrics = PerformanceMetrics(duration=0.85, memory_peak=2048, io_operations=10)

        # Should not raise any exceptions
        enhanced_logging.log_performance_metrics(operation, metrics)


class TestIntegrationWithExistingServices:
    """Test integration points with existing services."""

    def test_logger_integration(self, enhanced_logging):
        """Test integration with existing logging system."""
        assert hasattr(enhanced_logging, "logger")
        assert enhanced_logging.logger is not None

    @patch("src.core.enhanced_logging.trace.get_tracer")
    def test_opentelemetry_tracer_integration(self, mock_get_tracer, enhanced_logging):
        """Test OpenTelemetry tracer integration."""
        mock_tracer = Mock()
        mock_get_tracer.return_value = mock_tracer

        # Create new service to test tracer initialization
        service = EnhancedLoggingService()

        mock_get_tracer.assert_called_once_with("mlb_betting_system")
        assert service.tracer == mock_tracer


class TestContextVariablePropagation:
    """Test context variable propagation across async boundaries."""

    @pytest.mark.asyncio
    async def test_correlation_id_propagation(self, enhanced_logging):
        """Test that correlation ID propagates across async calls."""
        test_id = "propagation-test-123"
        enhanced_logging.set_correlation_id(test_id)

        async def async_operation():
            return enhanced_logging.get_current_correlation_id()

        # Should maintain correlation ID in async context
        result_id = await async_operation()
        assert result_id == test_id

    def test_correlation_id_isolation(self, enhanced_logging):
        """Test that correlation IDs are isolated between contexts."""
        # This test would require more complex context variable manipulation
        # For now, verify basic functionality

        id1 = enhanced_logging.generate_correlation_id()
        enhanced_logging.set_correlation_id(id1)

        current_id = enhanced_logging.get_current_correlation_id()
        assert current_id == id1


class TestPerformanceMetricsIntegration:
    """Test PerformanceMetrics integration with logging."""

    def test_performance_metrics_integration(self, enhanced_logging):
        """Test that PerformanceMetrics integrates with logging service."""
        metrics = PerformanceMetrics(duration=0.5)

        # Should be able to log performance metrics
        enhanced_logging.log_performance_metrics("test_operation", metrics)

        # Test that metrics can be converted to dict for logging
        metrics_dict = metrics.to_dict()
        assert isinstance(metrics_dict, dict)
        assert "duration" in metrics_dict
        assert "performance_class" in metrics_dict
