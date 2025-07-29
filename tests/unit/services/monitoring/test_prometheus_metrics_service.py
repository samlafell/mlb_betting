#!/usr/bin/env python3
"""
Unit tests for PrometheusMetricsService

Tests comprehensive Prometheus metrics infrastructure including:
- Metrics creation and initialization
- Pipeline execution recording
- Business metric tracking
- System health monitoring  
- SLI/SLO compliance tracking
- Break-glass procedure metrics
"""

import time
from unittest.mock import patch

import pytest
from prometheus_client import CollectorRegistry

from src.services.monitoring.prometheus_metrics_service import (
    AlertLevel,
    MetricType,
    PrometheusMetricsService,
    SLODefinition,
)


@pytest.fixture
def metrics_service():
    """Create a PrometheusMetricsService instance with isolated registry."""
    registry = CollectorRegistry()
    return PrometheusMetricsService(registry=registry)


@pytest.fixture
def sample_slo():
    """Create a sample SLO definition for testing."""
    return SLODefinition(
        name="test_slo",
        description="Test SLO for unit testing",
        target_percentage=99.0,
        warning_threshold=95.0,
        critical_threshold=90.0,
        measurement_window_minutes=60
    )


class TestPrometheusMetricsServiceInitialization:
    """Test metrics service initialization and setup."""

    def test_service_initialization(self, metrics_service):
        """Test that service initializes with all required metrics."""
        assert metrics_service is not None
        assert metrics_service.registry is not None
        assert hasattr(metrics_service, 'pipeline_executions_total')
        assert hasattr(metrics_service, 'slos')
        assert len(metrics_service.slos) == 4  # Default SLOs defined

    def test_metrics_creation(self, metrics_service):
        """Test that all expected metrics are created."""
        # Pipeline metrics
        assert hasattr(metrics_service, 'pipeline_executions_total')
        assert hasattr(metrics_service, 'pipeline_duration_seconds')
        assert hasattr(metrics_service, 'active_pipelines')

        # Business metrics
        assert hasattr(metrics_service, 'games_processed_total')
        assert hasattr(metrics_service, 'opportunities_detected_total')
        assert hasattr(metrics_service, 'strategy_performance_score')

        # System metrics
        assert hasattr(metrics_service, 'data_freshness_seconds')
        assert hasattr(metrics_service, 'system_health_status')

        # SLI metrics
        assert hasattr(metrics_service, 'sli_pipeline_latency_seconds')
        assert hasattr(metrics_service, 'sli_system_availability')

        # Break-glass metrics
        assert hasattr(metrics_service, 'break_glass_activations_total')
        assert hasattr(metrics_service, 'manual_overrides_total')

    def test_slo_definitions(self, metrics_service):
        """Test that default SLO definitions are properly created."""
        expected_slos = ['pipeline_latency', 'system_availability', 'data_freshness', 'error_rate']

        for slo_name in expected_slos:
            assert slo_name in metrics_service.slos
            slo = metrics_service.slos[slo_name]
            assert isinstance(slo, SLODefinition)
            assert slo.name == slo_name
            assert 0 < slo.target_percentage <= 100
            assert slo.warning_threshold < slo.target_percentage
            assert slo.critical_threshold < slo.warning_threshold


class TestPipelineMetrics:
    """Test pipeline execution metrics recording."""

    def test_pipeline_start_recording(self, metrics_service):
        """Test pipeline start is recorded correctly."""
        pipeline_id = "test_pipeline_123"
        pipeline_type = "full_data_collection"

        initial_count = len(metrics_service.pipeline_start_times)

        metrics_service.record_pipeline_start(pipeline_id, pipeline_type)

        # Verify start time is recorded
        assert pipeline_id in metrics_service.pipeline_start_times
        assert len(metrics_service.pipeline_start_times) == initial_count + 1

        # Verify start time is recent
        start_time = metrics_service.pipeline_start_times[pipeline_id]
        assert abs(time.time() - start_time) < 1.0  # Within 1 second

    def test_pipeline_completion_recording(self, metrics_service):
        """Test pipeline completion is recorded with proper metrics."""
        pipeline_id = "test_pipeline_456"
        pipeline_type = "analysis_only"

        # Start pipeline first
        metrics_service.record_pipeline_start(pipeline_id, pipeline_type)
        time.sleep(0.1)  # Small delay to test duration

        # Complete pipeline
        metrics_service.record_pipeline_completion(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            status="success",
            stages_executed=3
        )

        # Verify pipeline is removed from active tracking
        assert pipeline_id not in metrics_service.pipeline_start_times

    def test_pipeline_completion_with_errors(self, metrics_service):
        """Test pipeline completion with error recording."""
        pipeline_id = "test_pipeline_error"
        pipeline_type = "data_collection"

        # Start and complete with errors
        metrics_service.record_pipeline_start(pipeline_id, pipeline_type)

        test_errors = [ValueError("Test error"), RuntimeError("Another error")]
        metrics_service.record_pipeline_completion(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            status="failed",
            errors=test_errors
        )

        # Verify completion (errors are recorded via metrics internally)
        assert pipeline_id not in metrics_service.pipeline_start_times

    def test_stage_execution_recording(self, metrics_service):
        """Test individual stage execution recording."""
        stage = "data_collection"
        duration = 5.2
        status = "success"
        records_processed = 150

        # Should not raise any exceptions
        metrics_service.record_stage_execution(
            stage=stage,
            duration=duration,
            status=status,
            records_processed=records_processed
        )


class TestBusinessMetrics:
    """Test business-specific metrics recording."""

    def test_games_processed_recording(self, metrics_service):
        """Test games processed metric recording."""
        count = 15
        date = "2025-01-25"
        source = "action_network"

        metrics_service.record_games_processed(count, date, source)
        # Metrics are recorded internally - test passes if no exception

    def test_opportunity_detection_recording(self, metrics_service):
        """Test opportunity detection recording."""
        strategy = "sharp_action"
        confidence_level = "high"

        metrics_service.record_opportunity_detected(strategy, confidence_level)
        # Metrics are recorded internally - test passes if no exception

    def test_recommendation_recording(self, metrics_service):
        """Test recommendation made recording."""
        strategy = "line_movement"
        outcome = "pending"

        metrics_service.record_recommendation_made(strategy, outcome)
        # Metrics are recorded internally - test passes if no exception

    def test_strategy_performance_update(self, metrics_service):
        """Test strategy performance score updates."""
        strategy_name = "sharp_consensus"
        score = 0.82

        metrics_service.update_strategy_performance(strategy_name, score)
        # Metrics are updated internally - test passes if no exception

    def test_active_strategies_count(self, metrics_service):
        """Test active strategies count setting."""
        count = 7

        metrics_service.set_active_strategies_count(count)
        # Metric is set internally - test passes if no exception

    def test_value_tracking_updates(self, metrics_service):
        """Test value tracking metrics."""
        value = 1250.75
        timeframe = "daily"

        metrics_service.update_total_value_identified(value, timeframe)

        confidence_score = 0.91
        strategy = "all"

        metrics_service.update_average_confidence_score(confidence_score, strategy)
        # Metrics are updated internally - test passes if no exception


class TestSystemMetrics:
    """Test system health and performance metrics."""

    def test_data_freshness_update(self, metrics_service):
        """Test data freshness metric updates."""
        source = "vsin"
        age_seconds = 45.0

        metrics_service.update_data_freshness(source, age_seconds)
        # Should also update SLI compliance automatically
        # Test passes if no exception

    def test_data_quality_score_update(self, metrics_service):
        """Test data quality score updates."""
        source = "sbd"
        metric = "completeness"
        score = 0.95

        metrics_service.update_data_quality_score(source, metric, score)
        # Metric is updated internally - test passes if no exception

    def test_collection_success_rate_update(self, metrics_service):
        """Test collection success rate updates."""
        source = "odds_api"
        rate = 0.98

        metrics_service.update_collection_success_rate(source, rate)
        # Metric is updated internally - test passes if no exception

    def test_database_query_recording(self, metrics_service):
        """Test database query metrics recording."""
        query_type = "game_lookup"
        duration = 0.045

        metrics_service.record_database_query(query_type, duration)
        # Metric is recorded internally - test passes if no exception

    def test_api_call_recording(self, metrics_service):
        """Test external API call recording."""
        api_name = "action_network"
        endpoint = "/games"
        duration = 1.2

        # Test successful API call
        metrics_service.record_api_call(api_name, endpoint, duration)

        # Test API call with error
        metrics_service.record_api_call(api_name, endpoint, duration, error_code="500")
        # Metrics are recorded internally - test passes if no exception

    def test_system_health_status_update(self, metrics_service):
        """Test system health status updates."""
        valid_statuses = ['healthy', 'warning', 'critical', 'unknown']
        expected_values = [1, 2, 3, 0]

        for status, expected_value in zip(valid_statuses, expected_values, strict=False):
            metrics_service.update_system_health_status(status)
            # Status is mapped and recorded internally

        # Test invalid status defaults to unknown
        metrics_service.update_system_health_status("invalid_status")


class TestBreakGlassMetrics:
    """Test break-glass emergency procedure metrics."""

    def test_break_glass_activation_recording(self, metrics_service):
        """Test break-glass activation recording."""
        procedure_type = "manual_pipeline_execution"
        trigger_reason = "scheduler_failure"

        metrics_service.record_break_glass_activation(procedure_type, trigger_reason)
        # Metric is recorded internally - test passes if no exception

    def test_manual_override_recording(self, metrics_service):
        """Test manual override recording."""
        system_component = "data_collector"
        override_reason = "api_timeout"

        metrics_service.record_manual_override(system_component, override_reason)
        # Metric is recorded internally - test passes if no exception

    def test_emergency_execution_recording(self, metrics_service):
        """Test emergency execution recording."""
        execution_type = "critical_data_collection"

        metrics_service.record_emergency_execution(execution_type)
        # Metric is recorded internally - test passes if no exception


class TestSLOManagement:
    """Test SLO compliance tracking and violation handling."""

    def test_slo_compliance_check(self, metrics_service):
        """Test SLO compliance checking."""
        compliance_results = metrics_service.check_slo_compliance()

        assert isinstance(compliance_results, dict)
        assert len(compliance_results) == len(metrics_service.slos)

        for slo_name, result in compliance_results.items():
            assert 'current_value' in result
            assert 'target' in result
            assert 'status' in result
            assert 'last_violation' in result
            assert result['status'] in ['healthy', 'warning', 'critical']

    def test_slo_violation_recording(self, metrics_service):
        """Test SLO violation recording."""
        slo_name = "pipeline_latency"
        severity = "warning"

        metrics_service.record_slo_violation(slo_name, severity)
        # Violation is recorded internally - test passes if no exception


class TestMetricsExport:
    """Test metrics export and server functionality."""

    def test_metrics_export(self, metrics_service):
        """Test Prometheus metrics export."""
        metrics_data = metrics_service.get_metrics()

        assert isinstance(metrics_data, str)
        assert len(metrics_data) > 0
        # Should contain some metric names
        assert 'mlb_' in metrics_data

    def test_content_type(self, metrics_service):
        """Test Prometheus content type."""
        content_type = metrics_service.get_content_type()

        assert isinstance(content_type, str)
        assert 'text/plain' in content_type

    def test_system_overview(self, metrics_service):
        """Test system overview metrics."""
        overview = metrics_service.get_system_overview()

        assert isinstance(overview, dict)
        required_keys = ['uptime_seconds', 'active_pipelines', 'total_slos', 'slo_compliance', 'last_updated']

        for key in required_keys:
            assert key in overview

        assert isinstance(overview['uptime_seconds'], (int, float))
        assert overview['uptime_seconds'] >= 0
        assert isinstance(overview['active_pipelines'], int)
        assert overview['active_pipelines'] >= 0
        assert overview['total_slos'] == len(metrics_service.slos)

    @patch('src.services.monitoring.prometheus_metrics_service.start_http_server')
    def test_http_server_start(self, mock_start_server, metrics_service):
        """Test HTTP server start functionality."""
        port = 8000

        metrics_service.start_http_server(port)

        mock_start_server.assert_called_once_with(port, registry=metrics_service.registry)


class TestMetricsServiceSingleton:
    """Test global metrics service instance management."""

    @patch('src.services.monitoring.prometheus_metrics_service.PrometheusMetricsService')
    def test_get_metrics_service_singleton(self, mock_metrics_service):
        """Test that get_metrics_service returns singleton instance."""
        # Reset global instance
        import src.services.monitoring.prometheus_metrics_service
        from src.services.monitoring.prometheus_metrics_service import (
            get_metrics_service,
        )
        src.services.monitoring.prometheus_metrics_service._metrics_service = None

        # First call should create instance
        service1 = get_metrics_service()
        mock_metrics_service.assert_called_once()

        # Second call should return same instance
        mock_metrics_service.reset_mock()
        service2 = get_metrics_service()
        mock_metrics_service.assert_not_called()


class TestMetricTypes:
    """Test metric type enumerations."""

    def test_metric_type_enum(self):
        """Test MetricType enumeration values."""
        expected_types = ["pipeline", "business", "system", "sli", "break_glass"]

        for metric_type in expected_types:
            assert hasattr(MetricType, metric_type.upper())
            assert getattr(MetricType, metric_type.upper()) == metric_type

    def test_alert_level_enum(self):
        """Test AlertLevel enumeration values."""
        expected_levels = ["warning", "critical"]

        for level in expected_levels:
            assert hasattr(AlertLevel, level.upper())
            assert getattr(AlertLevel, level.upper()) == level


class TestSLODefinition:
    """Test SLO definition data class."""

    def test_slo_definition_creation(self, sample_slo):
        """Test SLO definition creation with all fields."""
        assert sample_slo.name == "test_slo"
        assert sample_slo.description == "Test SLO for unit testing"
        assert sample_slo.target_percentage == 99.0
        assert sample_slo.warning_threshold == 95.0
        assert sample_slo.critical_threshold == 90.0
        assert sample_slo.measurement_window_minutes == 60

    def test_slo_definition_defaults(self):
        """Test SLO definition with default measurement window."""
        slo = SLODefinition(
            name="test",
            description="test",
            target_percentage=99.0,
            warning_threshold=95.0,
            critical_threshold=90.0
        )

        assert slo.measurement_window_minutes == 60  # Default value


class TestIntegrationWithExistingServices:
    """Test integration points with existing services."""

    def test_settings_integration(self, metrics_service):
        """Test that metrics service integrates with settings."""
        assert hasattr(metrics_service, 'settings')
        assert metrics_service.settings is not None

    def test_logger_integration(self, metrics_service):
        """Test that metrics service integrates with logging."""
        assert hasattr(metrics_service, 'logger')
        assert metrics_service.logger is not None
