#!/usr/bin/env python3
"""
Data Quality Monitoring Integration Service

Integrates data quality validation with the existing monitoring infrastructure.
Publishes metrics to Prometheus, sends alerts, and provides dashboard data.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Any

from ...core.config import UnifiedSettings
from ...core.logging import LogComponent, get_logger
from ..monitoring.prometheus_metrics_service import get_metrics_service
from ..monitoring.unified_monitoring_service import (
    UnifiedMonitoringService, 
    Alert, 
    AlertLevel,
    HealthStatus
)
from .validation_service import (
    DataQualityValidationService,
    PipelineStage,
    QualityReport,
    ValidationStatus,
    QualityMetrics
)
from .metrics_persistence import DataQualityMetricsPersistence

logger = get_logger(__name__, LogComponent.DATA_QUALITY)


class DataQualityMonitoringIntegration:
    """
    Integrates data quality validation with monitoring infrastructure.
    
    Features:
    - Publishes quality metrics to Prometheus
    - Sends alerts for quality violations 
    - Provides dashboard integration data
    - Tracks quality trends and SLO compliance
    """
    
    def __init__(self, config: UnifiedSettings):
        self.config = config
        self.validation_service = DataQualityValidationService(config)
        self.metrics_service = get_metrics_service()
        self.monitoring_service = UnifiedMonitoringService(config)
        self.persistence_service = DataQualityMetricsPersistence(config)
        
        # Quality thresholds for alerting
        self.alert_thresholds = {
            "critical_quality_threshold": 0.85,  # Alert if quality drops below 85%
            "warning_quality_threshold": 0.90,   # Warning if quality drops below 90%
            "consecutive_failures_limit": 3,     # Alert after 3 consecutive failures
        }
        
        # State tracking
        self.consecutive_failures: Dict[str, int] = {}
        self.last_quality_scores: Dict[str, float] = {}
        
    async def initialize(self):
        """Initialize the monitoring integration service."""
        await self.monitoring_service.initialize()
        logger.info("Data quality monitoring integration initialized")
    
    async def run_quality_validation_with_monitoring(self) -> Dict[str, Any]:
        """
        Run full pipeline quality validation with comprehensive monitoring.
        
        Returns:
            Combined validation results and monitoring data
        """
        start_time = datetime.now()
        
        try:
            # Run quality validation
            reports = await self.validation_service.validate_full_pipeline()
            metrics = await self.validation_service.get_quality_metrics()
            gates = await self.validation_service.check_quality_gates()
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Publish metrics to Prometheus
            await self._publish_quality_metrics(reports, metrics, execution_time)
            
            # Persist results to database for historical tracking
            run_id = await self.persistence_service.persist_validation_run(
                reports, metrics, "monitoring_integration", 
                f"Automated validation run at {start_time.isoformat()}"
            )
            
            # Generate and send alerts
            alerts = await self._generate_and_send_alerts(reports, metrics)
            
            # Persist alerts to database
            for alert in alerts:
                await self.persistence_service.persist_quality_alert(
                    alert.level.value, alert.title, alert.message, alert.source,
                    stage=alert.metadata.get('stage'), 
                    rule_name=alert.metadata.get('rule_name'),
                    affected_records=alert.metadata.get('affected_records'),
                    metadata=alert.metadata
                )
            
            # Update monitoring service with quality status
            await self._update_monitoring_status(reports, metrics)
            
            # Track quality trends
            await self._track_quality_trends(reports)
            
            logger.info(f"Quality validation with monitoring completed in {execution_time:.2f}s")
            
            return {
                "validation_reports": reports,
                "quality_metrics": metrics,
                "quality_gates": gates,
                "alerts_generated": len(alerts),
                "execution_time_seconds": execution_time,
                "overall_status": self._determine_overall_health_status(reports),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Quality validation with monitoring failed: {e}")
            
            # Record failure metrics
            self.metrics_service.record_data_quality_validation_duration("overall", execution_time)
            for stage in PipelineStage:
                self.metrics_service.update_data_quality_gate_status(stage.value, "failed")
            
            # Send critical alert
            await self._send_critical_alert(
                "Data Quality Validation Failed", 
                f"Quality validation system failed: {e}"
            )
            
            raise
    
    async def _publish_quality_metrics(self, reports: Dict[PipelineStage, QualityReport], 
                                     metrics: QualityMetrics, execution_time: float):
        """Publish quality metrics to Prometheus."""
        try:
            # Overall metrics
            self.metrics_service.update_data_quality_score("overall", "pipeline_score", metrics.overall_score)
            self.metrics_service.record_data_quality_validation_duration("overall", execution_time)
            
            # Stage-specific metrics
            for stage, report in reports.items():
                stage_name = stage.value
                
                # Overall stage score and status
                self.metrics_service.update_data_quality_validation_score(
                    stage_name, "overall", report.overall_score
                )
                
                status_mapping = {
                    ValidationStatus.PASSED: "passed",
                    ValidationStatus.WARNING: "warning", 
                    ValidationStatus.FAILED: "failed",
                    ValidationStatus.PENDING: "failed"
                }
                self.metrics_service.update_data_quality_gate_status(
                    stage_name, status_mapping[report.overall_status]
                )
                
                # Execution time
                self.metrics_service.record_data_quality_validation_duration(
                    stage_name, report.execution_time_ms / 1000
                )
                
                # Records validated
                self.metrics_service.record_data_quality_records_validated(
                    stage_name, report.total_records
                )
                
                # Individual rule results
                for result in report.validation_results:
                    dimension = result.metadata.get('rule_dimension', 'unknown')
                    
                    # Rule-specific score
                    self.metrics_service.update_data_quality_validation_score(
                        stage_name, dimension, result.score
                    )
                    
                    # Record violations for failed rules
                    if result.status == ValidationStatus.FAILED:
                        severity = result.metadata.get('business_impact', 'medium')
                        self.metrics_service.record_data_quality_rule_violation(
                            stage_name, result.rule_name, severity
                        )
            
            # Business metrics
            self.metrics_service.update_data_quality_score("pipeline", "freshness_score", metrics.data_freshness_score)
            self.metrics_service.update_data_quality_score("pipeline", "anomaly_score", metrics.anomaly_detection_score)
            self.metrics_service.update_data_quality_score("pipeline", "gate_pass_rate", metrics.quality_gate_pass_rate)
            
            logger.debug("Published quality metrics to Prometheus")
            
        except Exception as e:
            logger.error(f"Failed to publish quality metrics: {e}")
    
    async def _generate_and_send_alerts(self, reports: Dict[PipelineStage, QualityReport], 
                                      metrics: QualityMetrics) -> List[Alert]:
        """Generate and send quality-based alerts."""
        alerts = []
        
        try:
            # Generate alerts from validation service
            validation_alerts = await self.validation_service.generate_quality_alerts(reports)
            alerts.extend(validation_alerts)
            
            # Check for consecutive failures
            for stage, report in reports.items():
                stage_name = stage.value
                
                if report.overall_status == ValidationStatus.FAILED:
                    self.consecutive_failures[stage_name] = self.consecutive_failures.get(stage_name, 0) + 1
                    
                    if self.consecutive_failures[stage_name] >= self.alert_thresholds["consecutive_failures_limit"]:
                        alerts.append(Alert(
                            level=AlertLevel.CRITICAL,
                            title=f"Consecutive Quality Failures - {stage_name.upper()}",
                            message=f"Stage {stage_name} has failed quality validation "
                                   f"{self.consecutive_failures[stage_name]} consecutive times",
                            source=f"quality_consecutive_failures_{stage_name}",
                            metadata={
                                "stage": stage_name,
                                "consecutive_failures": self.consecutive_failures[stage_name],
                                "current_score": report.overall_score
                            }
                        ))
                else:
                    # Reset consecutive failures on success
                    self.consecutive_failures[stage_name] = 0
            
            # Overall pipeline quality alerts
            if metrics.overall_score < self.alert_thresholds["critical_quality_threshold"]:
                alerts.append(Alert(
                    level=AlertLevel.CRITICAL,
                    title="Critical Pipeline Quality Degradation",
                    message=f"Overall pipeline quality score {metrics.overall_score:.1%} "
                           f"below critical threshold {self.alert_thresholds['critical_quality_threshold']:.1%}",
                    source="pipeline_quality_critical",
                    metadata={
                        "overall_score": metrics.overall_score,
                        "threshold": self.alert_thresholds["critical_quality_threshold"],
                        "raw_score": metrics.raw_score,
                        "staging_score": metrics.staging_score,
                        "curated_score": metrics.curated_score
                    }
                ))
            elif metrics.overall_score < self.alert_thresholds["warning_quality_threshold"]:
                alerts.append(Alert(
                    level=AlertLevel.WARNING,
                    title="Pipeline Quality Warning",
                    message=f"Overall pipeline quality score {metrics.overall_score:.1%} "
                           f"below warning threshold {self.alert_thresholds['warning_quality_threshold']:.1%}",
                    source="pipeline_quality_warning",
                    metadata={
                        "overall_score": metrics.overall_score,
                        "threshold": self.alert_thresholds["warning_quality_threshold"]
                    }
                ))
            
            # Data freshness alerts
            if metrics.data_freshness_score < 0.8:
                alerts.append(Alert(
                    level=AlertLevel.WARNING,
                    title="Data Freshness Warning",
                    message=f"Data freshness score {metrics.data_freshness_score:.1%} indicates stale data",
                    source="data_freshness_warning",
                    metadata={
                        "freshness_score": metrics.data_freshness_score
                    }
                ))
            
            # Send all alerts through monitoring service
            for alert in alerts:
                await self.monitoring_service.send_alert(alert)
            
            logger.info(f"Generated and sent {len(alerts)} quality alerts")
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to generate/send quality alerts: {e}")
            return []
    
    async def _update_monitoring_status(self, reports: Dict[PipelineStage, QualityReport], 
                                      metrics: QualityMetrics):
        """Update monitoring service with quality status."""
        try:
            # Determine overall health status
            overall_status = self._determine_overall_health_status(reports)
            
            # Update system health status
            self.metrics_service.update_system_health_status(overall_status)
            
            # Create quality health check result
            quality_health_check = {
                "status": HealthStatus.HEALTHY if overall_status == "healthy" else 
                         HealthStatus.WARNING if overall_status == "warning" else HealthStatus.CRITICAL,
                "message": f"Data quality validation: {metrics.overall_score:.1%} overall score",
                "metadata": {
                    "overall_score": metrics.overall_score,
                    "raw_score": metrics.raw_score,
                    "staging_score": metrics.staging_score,
                    "curated_score": metrics.curated_score,
                    "total_records": metrics.total_records_processed,
                    "gate_pass_rate": metrics.quality_gate_pass_rate
                }
            }
            
            # Add to monitoring service health checks
            self.monitoring_service.health_checks["data_quality"] = {
                "name": "Data Quality Validation",
                "check_function": lambda: quality_health_check,
                "interval": 300,  # 5 minutes
                "timeout": 60,
            }
            
            logger.debug("Updated monitoring service with quality status")
            
        except Exception as e:
            logger.error(f"Failed to update monitoring status: {e}")
    
    def _determine_overall_health_status(self, reports: Dict[PipelineStage, QualityReport]) -> str:
        """Determine overall health status from validation reports."""
        if any(report.overall_status == ValidationStatus.FAILED for report in reports.values()):
            return "critical"
        elif any(report.overall_status == ValidationStatus.WARNING for report in reports.values()):
            return "warning"
        else:
            return "healthy"
    
    async def _track_quality_trends(self, reports: Dict[PipelineStage, QualityReport]):
        """Track quality trends for analysis."""
        try:
            for stage, report in reports.items():
                stage_name = stage.value
                current_score = report.overall_score
                previous_score = self.last_quality_scores.get(stage_name, current_score)
                
                # Calculate trend
                trend = current_score - previous_score
                
                # Log significant changes
                if abs(trend) > 0.05:  # 5% change
                    direction = "improved" if trend > 0 else "degraded"
                    logger.info(f"Quality trend alert: {stage_name} quality {direction} "
                               f"by {abs(trend):.1%} (from {previous_score:.1%} to {current_score:.1%})")
                
                # Update tracking
                self.last_quality_scores[stage_name] = current_score
                
        except Exception as e:
            logger.warning(f"Failed to track quality trends: {e}")
    
    async def _send_critical_alert(self, title: str, message: str):
        """Send a critical alert through the monitoring service."""
        try:
            alert = Alert(
                level=AlertLevel.CRITICAL,
                title=title,
                message=message,
                source="data_quality_system",
                metadata={"timestamp": datetime.now().isoformat()}
            )
            await self.monitoring_service.send_alert(alert)
        except Exception as e:
            logger.error(f"Failed to send critical alert: {e}")
    
    async def get_quality_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive data for quality monitoring dashboard."""
        try:
            # Get baseline quality data
            dashboard_data = await self.validation_service.get_quality_dashboard_data()
            
            # Add monitoring integration data
            dashboard_data.update({
                "monitoring_integration": {
                    "consecutive_failures": dict(self.consecutive_failures),
                    "quality_trends": dict(self.last_quality_scores),
                    "alert_thresholds": self.alert_thresholds,
                    "health_status": self._determine_overall_health_status(
                        await self.validation_service.validate_full_pipeline()
                    )
                },
                "prometheus_metrics": {
                    "quality_validation_enabled": True,
                    "metrics_published": True,
                    "slo_compliance": self.metrics_service.check_slo_compliance()
                }
            })
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Failed to get quality dashboard data: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def run_quality_health_check(self) -> Dict[str, Any]:
        """Run a quick quality health check for monitoring."""
        try:
            start_time = datetime.now()
            
            # Run quick validation (subset of rules)
            metrics = await self.validation_service.get_quality_metrics()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Determine health status
            if metrics.overall_score >= 0.95:
                status = "healthy"
            elif metrics.overall_score >= 0.85:
                status = "warning"
            else:
                status = "critical"
            
            return {
                "status": status,
                "overall_score": metrics.overall_score,
                "execution_time_seconds": execution_time,
                "details": {
                    "raw_score": metrics.raw_score,
                    "staging_score": metrics.staging_score,
                    "curated_score": metrics.curated_score,
                    "total_records": metrics.total_records_processed,
                    "freshness_score": metrics.data_freshness_score
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Quality health check failed: {e}")
            return {
                "status": "critical",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def cleanup(self):
        """Cleanup resources."""
        await self.monitoring_service.cleanup()
        logger.info("Data quality monitoring integration cleaned up")