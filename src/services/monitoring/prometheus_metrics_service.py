#!/usr/bin/env python3
"""
Prometheus Metrics Service

Production-grade metrics collection and export for the MLB betting system.
Integrates with existing PipelineOrchestrationService and UnifiedMonitoringService
to provide comprehensive observability.

Features:
- Pipeline execution metrics (latency, success rates, error rates)
- Business metrics (opportunities detected, strategy performance)
- System health metrics (resource usage, data freshness)
- SLI/SLO tracking with automatic alerting thresholds
- Break-glass manual override capabilities
"""

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    start_http_server,
)

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.MONITORING)


class MetricType(str, Enum):
    """Metric type enumeration for categorization."""

    PIPELINE = "pipeline"
    BUSINESS = "business"
    SYSTEM = "system"
    SLI = "sli"
    BREAK_GLASS = "break_glass"


class AlertLevel(str, Enum):
    """Alert level for SLO violations."""

    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class SLODefinition:
    """Service Level Objective definition."""

    name: str
    description: str
    target_percentage: float
    warning_threshold: float
    critical_threshold: float
    measurement_window_minutes: int = 60


class PrometheusMetricsService:
    """
    Production-grade Prometheus metrics service for MLB betting system.

    Provides comprehensive observability including:
    - Pipeline execution monitoring
    - Business KPI tracking
    - System health indicators
    - SLI/SLO compliance measurement
    - Break-glass emergency metrics
    """

    def __init__(self, registry: CollectorRegistry | None = None):
        """Initialize the Prometheus metrics service."""
        self.settings = get_settings()
        self.registry = registry or CollectorRegistry()
        self.logger = logger

        # Initialize all metrics
        self._init_pipeline_metrics()
        self._init_business_metrics()
        self._init_system_metrics()
        self._init_sli_metrics()
        self._init_break_glass_metrics()

        # SLO definitions
        self.slos = self._define_slos()

        # State tracking
        self.start_time = time.time()
        self.pipeline_start_times: dict[str, float] = {}

        self.logger.info("Prometheus metrics service initialized")

    def _init_pipeline_metrics(self):
        """Initialize pipeline execution metrics."""
        self._init_pipeline_counters()
        self._init_pipeline_histograms()
        self._init_pipeline_gauges()
        self._init_pipeline_error_tracking()

    def _init_pipeline_counters(self):
        """Initialize pipeline execution counters."""
        self.pipeline_executions_total = Counter(
            "mlb_pipeline_executions_total",
            "Total number of pipeline executions",
            ["pipeline_type", "status"],
            registry=self.registry,
        )

        self.pipeline_stages_total = Counter(
            "mlb_pipeline_stages_total",
            "Total number of pipeline stage executions",
            ["stage", "status"],
            registry=self.registry,
        )

    def _init_pipeline_histograms(self):
        """Initialize pipeline latency tracking histograms."""
        # Get configurable bucket values from monitoring settings
        pipeline_buckets = self.settings.monitoring.pipeline_duration_buckets
        stage_buckets = self.settings.monitoring.pipeline_stage_duration_buckets

        self.pipeline_duration_seconds = Histogram(
            "mlb_pipeline_duration_seconds",
            "Pipeline execution duration in seconds",
            ["pipeline_type", "stage"],
            buckets=pipeline_buckets,
            registry=self.registry,
        )

        self.pipeline_stage_duration_seconds = Histogram(
            "mlb_pipeline_stage_duration_seconds",
            "Pipeline stage execution duration in seconds",
            ["stage"],
            buckets=stage_buckets,
            registry=self.registry,
        )

    def _init_pipeline_gauges(self):
        """Initialize pipeline queue and concurrency gauges."""
        self.active_pipelines = Gauge(
            "mlb_active_pipelines",
            "Number of currently active pipelines",
            ["pipeline_type"],
            registry=self.registry,
        )

        self.pipeline_queue_size = Gauge(
            "mlb_pipeline_queue_size",
            "Number of pipelines waiting in queue",
            registry=self.registry,
        )

    def _init_pipeline_error_tracking(self):
        """Initialize pipeline error tracking metrics."""
        self.pipeline_errors_total = Counter(
            "mlb_pipeline_errors_total",
            "Total number of pipeline errors",
            ["pipeline_type", "stage", "error_type"],
            registry=self.registry,
        )

    def _init_business_metrics(self):
        """Initialize business-specific metrics."""

        # Game and opportunity tracking
        self.games_processed_total = Counter(
            "mlb_games_processed_total",
            "Total number of games processed",
            ["date", "source"],
            registry=self.registry,
        )

        self.opportunities_detected_total = Counter(
            "mlb_opportunities_detected_total",
            "Total number of betting opportunities detected",
            ["strategy", "confidence_level"],
            registry=self.registry,
        )

        self.recommendations_made_total = Counter(
            "mlb_recommendations_made_total",
            "Total number of betting recommendations made",
            ["strategy", "outcome"],
            registry=self.registry,
        )

        # Strategy performance
        self.strategy_performance_score = Gauge(
            "mlb_strategy_performance_score",
            "Current performance score for each strategy",
            ["strategy_name"],
            registry=self.registry,
        )

        self.active_strategies = Gauge(
            "mlb_active_strategies",
            "Number of currently active strategies",
            registry=self.registry,
        )

        # Value tracking
        self.total_value_identified = Gauge(
            "mlb_total_value_identified_dollars",
            "Total betting value identified in dollars",
            ["timeframe"],
            registry=self.registry,
        )

        self.average_confidence_score = Gauge(
            "mlb_average_confidence_score",
            "Average confidence score of recommendations",
            ["strategy"],
            registry=self.registry,
        )

    def _init_system_metrics(self):
        """Initialize system health and performance metrics."""

        # Data freshness
        self.data_freshness_seconds = Gauge(
            "mlb_data_freshness_seconds",
            "Age of latest data in seconds",
            ["source"],
            registry=self.registry,
        )

        self.data_quality_score = Gauge(
            "mlb_data_quality_score",
            "Data quality score (0-1)",
            ["source", "metric"],
            registry=self.registry,
        )

        # Collection success rates
        self.data_collection_success_rate = Gauge(
            "mlb_data_collection_success_rate",
            "Success rate of data collection (0-1)",
            ["source"],
            registry=self.registry,
        )

        # Database metrics
        self.database_connections_active = Gauge(
            "mlb_database_connections_active",
            "Number of active database connections",
            registry=self.registry,
        )

        self.database_query_duration_seconds = Histogram(
            "mlb_database_query_duration_seconds",
            "Database query execution time",
            ["query_type"],
            buckets=self.settings.monitoring.database_query_duration_buckets,
            registry=self.registry,
        )

        # API response times
        self.external_api_duration_seconds = Histogram(
            "mlb_external_api_duration_seconds",
            "External API response time",
            ["api_name", "endpoint"],
            buckets=self.settings.monitoring.api_call_duration_buckets,
            registry=self.registry,
        )

        self.external_api_errors_total = Counter(
            "mlb_external_api_errors_total",
            "Total external API errors",
            ["api_name", "error_code"],
            registry=self.registry,
        )

    def _init_sli_metrics(self):
        """Initialize Service Level Indicator metrics."""

        # SLI: Pipeline Latency
        self.sli_pipeline_latency_seconds = Summary(
            "mlb_sli_pipeline_latency_seconds",
            "SLI: Pipeline execution latency",
            ["pipeline_type"],
            registry=self.registry,
        )

        # SLI: Data Freshness
        self.sli_data_freshness_compliance = Gauge(
            "mlb_sli_data_freshness_compliance",
            "SLI: Data freshness compliance (0-1)",
            ["source"],
            registry=self.registry,
        )

        # SLI: System Availability
        self.sli_system_availability = Gauge(
            "mlb_sli_system_availability",
            "SLI: System availability (0-1)",
            registry=self.registry,
        )

        # SLI: Error Rate
        self.sli_error_rate = Gauge(
            "mlb_sli_error_rate",
            "SLI: Error rate (0-1)",
            ["component"],
            registry=self.registry,
        )

        # SLO violation alerts
        self.slo_violations_total = Counter(
            "mlb_slo_violations_total",
            "Total SLO violations",
            ["slo_name", "severity"],
            registry=self.registry,
        )

    def _init_break_glass_metrics(self):
        """Initialize break-glass emergency metrics."""

        self.break_glass_activations_total = Counter(
            "mlb_break_glass_activations_total",
            "Total break-glass procedure activations",
            ["procedure_type", "trigger_reason"],
            registry=self.registry,
        )

        self.manual_overrides_total = Counter(
            "mlb_manual_overrides_total",
            "Total manual overrides of automated systems",
            ["system_component", "override_reason"],
            registry=self.registry,
        )

        self.emergency_executions_total = Counter(
            "mlb_emergency_executions_total",
            "Total emergency pipeline executions",
            ["execution_type"],
            registry=self.registry,
        )

        # System health status
        self.system_health_status = Gauge(
            "mlb_system_health_status",
            "Overall system health status (0=unknown, 1=healthy, 2=warning, 3=critical)",
            registry=self.registry,
        )

    def _define_slos(self) -> dict[str, SLODefinition]:
        """Define Service Level Objectives for the system."""
        return {
            "pipeline_latency": SLODefinition(
                name="pipeline_latency",
                description="P99 pipeline execution latency < 30 seconds",
                target_percentage=99.0,
                warning_threshold=95.0,
                critical_threshold=90.0,
                measurement_window_minutes=60,
            ),
            "system_availability": SLODefinition(
                name="system_availability",
                description="System availability >= 99.5%",
                target_percentage=99.5,
                warning_threshold=99.0,
                critical_threshold=98.0,
                measurement_window_minutes=60,
            ),
            "data_freshness": SLODefinition(
                name="data_freshness",
                description="95% of data updated within 60 seconds",
                target_percentage=95.0,
                warning_threshold=90.0,
                critical_threshold=80.0,
                measurement_window_minutes=10,
            ),
            "error_rate": SLODefinition(
                name="error_rate",
                description="Error rate < 0.5%",
                target_percentage=99.5,
                warning_threshold=99.0,
                critical_threshold=95.0,
                measurement_window_minutes=15,
            ),
        }

    # Pipeline Metrics Methods

    def record_pipeline_start(self, pipeline_id: str, pipeline_type: str):
        """Record the start of a pipeline execution."""
        self.pipeline_start_times[pipeline_id] = time.time()
        self.active_pipelines.labels(pipeline_type=pipeline_type).inc()

        self.logger.debug(
            "Pipeline execution started",
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
        )

    def record_pipeline_completion(
        self,
        pipeline_id: str,
        pipeline_type: str,
        status: str,
        stages_executed: int = 0,
        errors: list | None = None,
    ):
        """Record the completion of a pipeline execution."""

        # Calculate duration
        start_time = self.pipeline_start_times.pop(pipeline_id, time.time())
        duration = time.time() - start_time

        # Record metrics
        self.pipeline_executions_total.labels(
            pipeline_type=pipeline_type, status=status
        ).inc()

        self.pipeline_duration_seconds.labels(
            pipeline_type=pipeline_type, stage="total"
        ).observe(duration)

        self.sli_pipeline_latency_seconds.labels(pipeline_type=pipeline_type).observe(
            duration
        )

        self.active_pipelines.labels(pipeline_type=pipeline_type).dec()

        # Record errors if any
        if errors:
            for error in errors:
                self.pipeline_errors_total.labels(
                    pipeline_type=pipeline_type,
                    stage="unknown",  # Could be enhanced with stage info
                    error_type=type(error).__name__
                    if isinstance(error, Exception)
                    else "unknown",
                ).inc()

        self.logger.info(
            "Pipeline execution completed",
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            status=status,
            duration_seconds=duration,
            stages_executed=stages_executed,
        )

    def record_stage_execution(
        self,
        stage: str,
        duration: float,
        status: str,
        records_processed: int = 0,
        sample_rate: float | None = None,
    ):
        """Record the execution of a pipeline stage with optional sampling."""
        # Input validation
        if not stage or not stage.strip():
            raise ValueError("Stage cannot be empty or None")

        if not isinstance(duration, (int, float)) or duration < 0:
            raise ValueError(f"Duration must be a non-negative number, got: {duration}")

        if not status or not status.strip():
            raise ValueError("Status cannot be empty or None")

        if not isinstance(records_processed, int) or records_processed < 0:
            raise ValueError(
                f"Records processed must be a non-negative integer, got: {records_processed}"
            )

        # Use configured sample rate if none provided and sampling is enabled
        if sample_rate is None:
            if self.settings.monitoring.enable_metrics_sampling:
                sample_rate = self.settings.monitoring.metrics_sample_rate
            else:
                sample_rate = 1.0

        if not 0.0 <= sample_rate <= 1.0:
            raise ValueError(
                f"Sample rate must be between 0.0 and 1.0, got: {sample_rate}"
            )

        # Apply sampling for high-volume operations
        if sample_rate < 1.0 and random.random() > sample_rate:
            return

        # Sanitize inputs
        stage = stage.strip()
        status = status.strip()

        try:
            self.pipeline_stages_total.labels(stage=stage, status=status).inc()
            self.pipeline_stage_duration_seconds.labels(stage=stage).observe(duration)

            self.logger.debug(
                "Pipeline stage executed",
                stage=stage,
                status=status,
                duration_seconds=duration,
                records_processed=records_processed,
                sampled=sample_rate < 1.0,
                sample_rate=sample_rate,
            )
        except Exception as e:
            self.logger.error(f"Error recording stage execution metric: {e}")
            raise

    # Business Metrics Methods

    def record_games_processed(self, count: int, date: str, source: str):
        """Record games processed with input validation."""
        # Input validation
        if not isinstance(count, int) or count < 0:
            raise ValueError(f"Count must be a non-negative integer, got: {count}")

        if not date or not date.strip():
            raise ValueError("Date cannot be empty or None")

        if not source or not source.strip():
            raise ValueError("Source cannot be empty or None")

        # Sanitize inputs
        date = date.strip()
        source = source.strip()

        try:
            self.games_processed_total.labels(date=date, source=source).inc(count)
        except Exception as e:
            self.logger.error(f"Error recording games processed metric: {e}")
            raise

    def record_opportunity_detected(self, strategy: str, confidence_level: str):
        """Record a betting opportunity detection with input validation."""
        # Input validation
        if not strategy or not strategy.strip():
            raise ValueError("Strategy cannot be empty or None")

        if not confidence_level or not confidence_level.strip():
            raise ValueError("Confidence level cannot be empty or None")

        # Validate confidence level is in expected values
        valid_confidence_levels = ["high", "medium", "low"]
        if confidence_level.lower() not in valid_confidence_levels:
            raise ValueError(
                f"Confidence level must be one of {valid_confidence_levels}, got: {confidence_level}"
            )

        # Sanitize inputs
        strategy = strategy.strip()
        confidence_level = confidence_level.strip().lower()

        try:
            self.opportunities_detected_total.labels(
                strategy=strategy, confidence_level=confidence_level
            ).inc()
        except Exception as e:
            self.logger.error(f"Error recording opportunity detected metric: {e}")
            raise

    def record_recommendation_made(self, strategy: str, outcome: str = "pending"):
        """Record a betting recommendation with input validation."""
        # Input validation
        if not strategy or not strategy.strip():
            raise ValueError("Strategy cannot be empty or None")

        if not outcome or not outcome.strip():
            raise ValueError("Outcome cannot be empty or None")

        # Validate outcome is in expected values
        valid_outcomes = ["pending", "win", "loss", "push", "cancelled"]
        if outcome.lower() not in valid_outcomes:
            raise ValueError(f"Outcome must be one of {valid_outcomes}, got: {outcome}")

        # Sanitize inputs
        strategy = strategy.strip()
        outcome = outcome.strip().lower()

        try:
            self.recommendations_made_total.labels(
                strategy=strategy, outcome=outcome
            ).inc()
        except Exception as e:
            self.logger.error(f"Error recording recommendation made metric: {e}")
            raise

    def update_strategy_performance(self, strategy_name: str, score: float):
        """Update strategy performance score with input validation."""
        # Input validation
        if not strategy_name or not strategy_name.strip():
            raise ValueError("Strategy name cannot be empty or None")

        if not isinstance(score, (int, float)):
            raise ValueError(f"Score must be a number, got: {type(score)}")

        if score < 0.0 or score > 1.0:
            raise ValueError(f"Score must be between 0.0 and 1.0, got: {score}")

        # Sanitize inputs
        strategy_name = strategy_name.strip()

        try:
            self.strategy_performance_score.labels(strategy_name=strategy_name).set(
                score
            )
        except Exception as e:
            self.logger.error(f"Error updating strategy performance metric: {e}")
            raise

    def set_active_strategies_count(self, count: int):
        """Set the number of active strategies."""
        self.active_strategies.set(count)

    def update_total_value_identified(self, value: float, timeframe: str = "daily"):
        """Update total value identified."""
        self.total_value_identified.labels(timeframe=timeframe).set(value)

    def update_average_confidence_score(self, score: float, strategy: str = "all"):
        """Update average confidence score."""
        self.average_confidence_score.labels(strategy=strategy).set(score)

    # System Metrics Methods

    def update_data_freshness(self, source: str, age_seconds: float):
        """Update data freshness metric."""
        self.data_freshness_seconds.labels(source=source).set(age_seconds)

        # Calculate SLI compliance (data is fresh if < 60 seconds old)
        compliance = 1.0 if age_seconds < 60 else 0.0
        self.sli_data_freshness_compliance.labels(source=source).set(compliance)

    def update_data_quality_score(self, source: str, metric: str, score: float):
        """Update data quality score."""
        self.data_quality_score.labels(source=source, metric=metric).set(score)

    def update_collection_success_rate(self, source: str, rate: float):
        """Update data collection success rate."""
        self.data_collection_success_rate.labels(source=source).set(rate)

    def record_database_query(self, query_type: str, duration: float):
        """Record database query execution."""
        self.database_query_duration_seconds.labels(query_type=query_type).observe(
            duration
        )

    def record_api_call(
        self,
        api_name: str,
        endpoint: str,
        duration: float,
        error_code: str | None = None,
    ):
        """Record external API call."""
        self.external_api_duration_seconds.labels(
            api_name=api_name, endpoint=endpoint
        ).observe(duration)

        if error_code:
            self.external_api_errors_total.labels(
                api_name=api_name, error_code=error_code
            ).inc()

    def update_system_health_status(self, status: str):
        """Update overall system health status."""
        status_mapping = {"healthy": 1, "warning": 2, "critical": 3, "unknown": 0}
        self.system_health_status.set(status_mapping.get(status, 0))

    # Break-Glass Methods

    def record_break_glass_activation(self, procedure_type: str, trigger_reason: str):
        """Record break-glass procedure activation."""
        self.break_glass_activations_total.labels(
            procedure_type=procedure_type, trigger_reason=trigger_reason
        ).inc()

        self.logger.warning(
            "Break-glass procedure activated",
            procedure_type=procedure_type,
            trigger_reason=trigger_reason,
        )

    def record_manual_override(self, system_component: str, override_reason: str):
        """Record manual system override."""
        self.manual_overrides_total.labels(
            system_component=system_component, override_reason=override_reason
        ).inc()

        self.logger.warning(
            "Manual override activated",
            system_component=system_component,
            override_reason=override_reason,
        )

    def record_emergency_execution(self, execution_type: str):
        """Record emergency pipeline execution."""
        self.emergency_executions_total.labels(execution_type=execution_type).inc()

        self.logger.warning(
            "Emergency execution triggered", execution_type=execution_type
        )

    # SLO Management

    def check_slo_compliance(self) -> dict[str, dict[str, Any]]:
        """Check SLO compliance and trigger alerts if needed."""
        compliance_results = {}

        for slo_name, slo_def in self.slos.items():
            # This would integrate with actual metric collection
            # For now, return placeholder compliance data
            compliance_results[slo_name] = {
                "current_value": 99.2,  # Placeholder
                "target": slo_def.target_percentage,
                "status": "healthy",  # healthy/warning/critical
                "last_violation": None,
            }

        return compliance_results

    def record_slo_violation(self, slo_name: str, severity: str):
        """Record an SLO violation."""
        self.slo_violations_total.labels(slo_name=slo_name, severity=severity).inc()

        self.logger.error(
            "SLO violation detected", slo_name=slo_name, severity=severity
        )

    # Export Methods

    def get_metrics(self) -> str:
        """Get metrics in Prometheus format."""
        return generate_latest(self.registry).decode("utf-8")

    def get_content_type(self) -> str:
        """Get Prometheus content type."""
        return CONTENT_TYPE_LATEST

    def start_http_server(self, port: int = 8000):
        """Start HTTP server for metrics export."""
        start_http_server(port, registry=self.registry)
        self.logger.info(f"Prometheus metrics server started on port {port}")

    def get_system_overview(self) -> dict[str, Any]:
        """Get high-level system overview metrics."""
        uptime = time.time() - self.start_time

        return {
            "uptime_seconds": uptime,
            "active_pipelines": len(self.pipeline_start_times),
            "total_slos": len(self.slos),
            "slo_compliance": self.check_slo_compliance(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }


# Global metrics service instance
_metrics_service: PrometheusMetricsService | None = None


def get_metrics_service() -> PrometheusMetricsService:
    """Get or create the global metrics service instance."""
    global _metrics_service
    if _metrics_service is None:
        _metrics_service = PrometheusMetricsService()
    return _metrics_service
