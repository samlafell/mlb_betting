#!/usr/bin/env python3
"""
Collection Health Monitoring API

REST API endpoints for comprehensive collection health monitoring with:
- Real-time health status endpoints
- Historical trend analysis
- Performance metrics and analytics
- Alert management and escalation
- Integration with existing monitoring dashboard

This API addresses Issue #36 by providing comprehensive REST endpoints
for collection health monitoring and management.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Path, Depends, status
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass

from ...core.config import get_settings
from ...core.enhanced_logging import get_contextual_logger, LogComponent  
from ...core.security import require_break_glass_auth
from ...data.collection.orchestrator import CollectionOrchestrator
from ...services.monitoring.collector_health_service import (
    HealthMonitoringOrchestrator,
    HealthStatus,
    CollectorHealthStatus,
    AlertSeverity,
)
from ...services.monitoring.realtime_collection_health_service import (
    get_realtime_health_service,
    LiveHealthMetrics,
    PerformanceDegradation,
    FailurePattern,
    HealthTrend,
)
from ...services.monitoring.enhanced_alerting_service import (
    get_enhanced_alerting_service,
    Alert,
    AlertChannel,
    AlertPriority,
)

logger = get_contextual_logger(__name__, LogComponent.API_CLIENT)
settings = get_settings()


# Pydantic models for API responses

class HealthStatusResponse(BaseModel):
    """Health status response model."""
    
    collector_name: str
    current_status: str
    success_rate_1h: float = Field(..., ge=0.0, le=1.0, description="1-hour success rate")
    success_rate_24h: float = Field(..., ge=0.0, le=1.0, description="24-hour success rate")  
    avg_response_time_1h: float = Field(..., ge=0.0, description="Average response time in seconds")
    data_quality_score: float = Field(..., ge=0.0, le=1.0, description="Data quality score")
    last_successful_collection: Optional[datetime] = None
    last_failed_collection: Optional[datetime] = None
    consecutive_failures: int = Field(..., ge=0, description="Consecutive failure count")
    consecutive_successes: int = Field(..., ge=0, description="Consecutive success count")
    health_trend: str
    predicted_failure_probability: float = Field(..., ge=0.0, le=1.0, description="Predicted failure probability")
    last_updated: datetime


class PerformanceDegradationResponse(BaseModel):
    """Performance degradation response model."""
    
    collector_name: str
    degradation_type: str
    severity: str
    current_value: float
    baseline_value: float
    deviation_percentage: float
    detection_time: datetime
    recommended_actions: List[str]


class FailurePatternResponse(BaseModel):
    """Failure pattern response model."""
    
    collector_name: str
    pattern_type: str
    frequency: int
    time_window: str
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Pattern confidence score")
    first_occurrence: datetime
    last_occurrence: datetime
    impact_assessment: str


class AlertResponse(BaseModel):
    """Alert response model."""
    
    id: str
    alert_type: str
    severity: str
    priority: str
    title: str
    message: str
    collector_name: Optional[str] = None
    business_impact_score: float = Field(..., ge=0.0, le=1.0, description="Business impact score")
    affected_services: List[str]
    correlation_group: Optional[str] = None
    timestamp: datetime
    escalation_level: str
    escalation_count: int = Field(..., ge=0, description="Number of escalations")
    acknowledged: bool
    resolved: bool
    resolved_at: Optional[datetime] = None


class HealthTrendsResponse(BaseModel):
    """Health trends response model."""
    
    collector_name: str
    time_period: str
    success_rate_trend: List[Dict[str, Any]]
    response_time_trend: List[Dict[str, Any]]
    data_quality_trend: List[Dict[str, Any]]
    failure_events: List[Dict[str, Any]]
    trend_analysis: Dict[str, Any]


class SystemHealthSummaryResponse(BaseModel):
    """System health summary response model."""
    
    overall_health_score: float = Field(..., ge=0.0, le=1.0, description="Overall system health score")
    total_collectors: int
    healthy_collectors: int
    degraded_collectors: int
    critical_collectors: int
    active_alerts: int
    critical_alerts: int
    avg_success_rate: float = Field(..., ge=0.0, le=1.0, description="Average success rate")
    avg_response_time: float = Field(..., ge=0.0, description="Average response time in seconds")
    system_uptime_hours: float
    last_updated: datetime


class RecoveryActionRequest(BaseModel):
    """Recovery action request model."""
    
    collector_name: str = Field(..., description="Name of collector to recover")
    action_type: Optional[str] = Field("auto", description="Type of recovery action")
    force: bool = Field(False, description="Force recovery even if not recommended")
    reason: str = Field(..., description="Reason for manual recovery")


class RecoveryActionResponse(BaseModel):
    """Recovery action response model."""
    
    collector_name: str
    action_type: str
    success: bool
    execution_time: float
    result_message: str
    timestamp: datetime


# Initialize API router
router = APIRouter(prefix="/api/health", tags=["Collection Health"])

# Global service instances (initialized in startup)
health_orchestrator: Optional[HealthMonitoringOrchestrator] = None
collection_orchestrator: Optional[CollectionOrchestrator] = None


@router.on_event("startup")
async def initialize_health_services():
    """Initialize health monitoring services."""
    global health_orchestrator, collection_orchestrator
    
    try:
        # Initialize collection orchestrator
        collection_orchestrator = CollectionOrchestrator(settings)
        await collection_orchestrator.initialize_collectors()
        
        # Initialize health monitoring
        health_orchestrator = HealthMonitoringOrchestrator(settings)
        for collector in collection_orchestrator.collectors.values():
            health_orchestrator.register_collector(collector)
            
        # Initialize real-time health service
        realtime_service = get_realtime_health_service(settings)
        await realtime_service.initialize(health_orchestrator)
        
        # Initialize enhanced alerting
        alerting_service = get_enhanced_alerting_service(settings)
        await alerting_service.initialize()
        
        logger.info("Collection health API services initialized")
        
    except Exception as e:
        logger.error("Failed to initialize health services", error=str(e))
        raise


# Health Status Endpoints

@router.get(
    "/status",
    response_model=Dict[str, HealthStatusResponse],
    summary="Get health status for all collectors",
    description="Retrieve real-time health status for all registered data collectors"
)
async def get_all_health_status():
    """Get real-time health status for all collectors."""
    try:
        realtime_service = get_realtime_health_service()
        all_health = await realtime_service.get_all_live_health_status()
        
        response = {}
        for collector_name, metrics in all_health.items():
            response[collector_name] = HealthStatusResponse(
                collector_name=metrics.collector_name,
                current_status=metrics.current_status.value,
                success_rate_1h=metrics.success_rate_1h,
                success_rate_24h=metrics.success_rate_24h,
                avg_response_time_1h=metrics.avg_response_time_1h,
                data_quality_score=metrics.data_quality_score,
                last_successful_collection=metrics.last_successful_collection,
                last_failed_collection=metrics.last_failed_collection,
                consecutive_failures=metrics.consecutive_failures,
                consecutive_successes=metrics.consecutive_successes,
                health_trend=metrics.health_trend.value,
                predicted_failure_probability=metrics.predicted_failure_probability,
                last_updated=metrics.last_updated
            )
            
        return response
        
    except Exception as e:
        logger.error("Failed to get all health status", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve health status: {str(e)}"
        )


@router.get(
    "/status/{collector_name}",
    response_model=HealthStatusResponse,
    summary="Get health status for specific collector",
    description="Retrieve detailed real-time health status for a specific data collector"
)
async def get_collector_health_status(
    collector_name: str = Path(..., description="Name of the collector to check")
):
    """Get real-time health status for a specific collector."""
    try:
        realtime_service = get_realtime_health_service()
        metrics = await realtime_service.get_live_health_status(collector_name)
        
        if not metrics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collector '{collector_name}' not found or not monitored"
            )
            
        return HealthStatusResponse(
            collector_name=metrics.collector_name,
            current_status=metrics.current_status.value,
            success_rate_1h=metrics.success_rate_1h,
            success_rate_24h=metrics.success_rate_24h,
            avg_response_time_1h=metrics.avg_response_time_1h,
            data_quality_score=metrics.data_quality_score,
            last_successful_collection=metrics.last_successful_collection,
            last_failed_collection=metrics.last_failed_collection,
            consecutive_failures=metrics.consecutive_failures,
            consecutive_successes=metrics.consecutive_successes,
            health_trend=metrics.health_trend.value,
            predicted_failure_probability=metrics.predicted_failure_probability,
            last_updated=metrics.last_updated
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get collector health status", collector=collector_name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve health status: {str(e)}"
        )


@router.get(
    "/summary",
    response_model=SystemHealthSummaryResponse,
    summary="Get system health summary",
    description="Retrieve overall system health summary with key metrics"
)
async def get_system_health_summary():
    """Get comprehensive system health summary."""
    try:
        realtime_service = get_realtime_health_service()
        alerting_service = get_enhanced_alerting_service()
        
        all_health = await realtime_service.get_all_live_health_status()
        alert_stats = await alerting_service.get_alert_statistics()
        
        if not all_health:
            return SystemHealthSummaryResponse(
                overall_health_score=0.0,
                total_collectors=0,
                healthy_collectors=0,
                degraded_collectors=0,
                critical_collectors=0,
                active_alerts=0,
                critical_alerts=0,
                avg_success_rate=0.0,
                avg_response_time=0.0,
                system_uptime_hours=0.0,
                last_updated=datetime.now()
            )
        
        # Calculate summary statistics
        total_collectors = len(all_health)
        healthy_collectors = sum(1 for m in all_health.values() if m.current_status == HealthStatus.HEALTHY)
        degraded_collectors = sum(1 for m in all_health.values() if m.current_status == HealthStatus.DEGRADED)
        critical_collectors = sum(1 for m in all_health.values() if m.current_status == HealthStatus.CRITICAL)
        
        # Calculate averages
        success_rates = [m.success_rate_24h for m in all_health.values()]
        response_times = [m.avg_response_time_1h for m in all_health.values() if m.avg_response_time_1h > 0]
        
        avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0.0
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0.0
        
        # Calculate overall health score
        health_score = (healthy_collectors * 1.0 + degraded_collectors * 0.5) / total_collectors if total_collectors > 0 else 0.0
        
        # Estimate system uptime (simplified)
        system_uptime_hours = 24.0 * health_score  # Simplified calculation
        
        return SystemHealthSummaryResponse(
            overall_health_score=health_score,
            total_collectors=total_collectors,
            healthy_collectors=healthy_collectors,
            degraded_collectors=degraded_collectors,
            critical_collectors=critical_collectors,
            active_alerts=alert_stats.get("active_alerts", 0),
            critical_alerts=alert_stats.get("alerts_by_severity", {}).get("critical", 0),
            avg_success_rate=avg_success_rate,
            avg_response_time=avg_response_time,
            system_uptime_hours=system_uptime_hours,
            last_updated=datetime.now()
        )
        
    except Exception as e:
        logger.error("Failed to get system health summary", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve system health summary: {str(e)}"
        )


# Performance Analysis Endpoints

@router.get(
    "/degradation",
    response_model=List[PerformanceDegradationResponse],
    summary="Get performance degradations",
    description="Retrieve current performance degradations across all collectors"
)
async def get_performance_degradations(
    collector_name: Optional[str] = Query(None, description="Filter by specific collector")
):
    """Get current performance degradations."""
    try:
        realtime_service = get_realtime_health_service()
        degradations = await realtime_service.detect_performance_degradation()
        
        if collector_name:
            degradations = [d for d in degradations if d.collector_name == collector_name]
        
        return [
            PerformanceDegradationResponse(
                collector_name=d.collector_name,
                degradation_type=d.degradation_type,
                severity=d.severity.value,
                current_value=d.current_value,
                baseline_value=d.baseline_value,
                deviation_percentage=d.deviation_percentage,
                detection_time=d.detection_time,
                recommended_actions=d.recommended_actions
            )
            for d in degradations
        ]
        
    except Exception as e:
        logger.error("Failed to get performance degradations", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve performance degradations: {str(e)}"
        )


@router.get(
    "/patterns",
    response_model=List[FailurePatternResponse],
    summary="Get failure patterns",
    description="Retrieve detected failure patterns across collectors"
)
async def get_failure_patterns(
    collector_name: Optional[str] = Query(None, description="Filter by specific collector"),
    min_confidence: float = Query(0.5, ge=0.0, le=1.0, description="Minimum confidence score")
):
    """Get detected failure patterns."""
    try:
        realtime_service = get_realtime_health_service()
        patterns = await realtime_service.analyze_failure_patterns()
        
        # Apply filters
        if collector_name:
            patterns = [p for p in patterns if p.collector_name == collector_name]
        
        patterns = [p for p in patterns if p.confidence_score >= min_confidence]
        
        return [
            FailurePatternResponse(
                collector_name=p.collector_name,
                pattern_type=p.pattern_type,
                frequency=p.frequency,
                time_window=p.time_window,
                confidence_score=p.confidence_score,
                first_occurrence=p.first_occurrence,
                last_occurrence=p.last_occurrence,
                impact_assessment=p.impact_assessment
            )
            for p in patterns
        ]
        
    except Exception as e:
        logger.error("Failed to get failure patterns", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve failure patterns: {str(e)}"
        )


# Alert Management Endpoints

@router.get(
    "/alerts",
    response_model=List[AlertResponse],
    summary="Get active alerts",
    description="Retrieve active health monitoring alerts with filtering options"
)
async def get_active_alerts(
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    collector_name: Optional[str] = Query(None, description="Filter by collector name"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of alerts to return")
):
    """Get active alerts with optional filtering."""
    try:
        alerting_service = get_enhanced_alerting_service()
        
        # Convert severity string to enum
        severity_filter = None
        if severity:
            try:
                severity_filter = AlertSeverity(severity.lower())
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid severity '{severity}'. Valid values: info, warning, critical"
                )
        
        alerts = await alerting_service.get_active_alerts(
            severity=severity_filter,
            collector_name=collector_name
        )
        
        # Apply limit
        alerts = alerts[:limit]
        
        return [
            AlertResponse(
                id=alert.id,
                alert_type=alert.alert_type,
                severity=alert.severity.value,
                priority=alert.priority.value,
                title=alert.title,
                message=alert.message,
                collector_name=alert.collector_name,
                business_impact_score=alert.business_impact_score,
                affected_services=alert.affected_services,
                correlation_group=alert.correlation_group,
                timestamp=alert.timestamp,
                escalation_level=alert.escalation_level.value,
                escalation_count=alert.escalation_count,
                acknowledged=alert.acknowledged,
                resolved=alert.resolved,
                resolved_at=alert.resolved_at
            )
            for alert in alerts
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get active alerts", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve active alerts: {str(e)}"
        )


@router.post(
    "/alerts/{alert_id}/acknowledge",
    summary="Acknowledge alert",
    description="Acknowledge an active alert to prevent escalation"
)
async def acknowledge_alert(
    alert_id: str = Path(..., description="Alert ID to acknowledge"),
    acknowledged_by: str = Query(..., description="Username or identifier of person acknowledging")
):
    """Acknowledge an active alert."""
    try:
        alerting_service = get_enhanced_alerting_service()
        success = await alerting_service.acknowledge_alert(alert_id, acknowledged_by)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Alert '{alert_id}' not found or already resolved"
            )
        
        return {"success": True, "message": f"Alert {alert_id} acknowledged by {acknowledged_by}"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to acknowledge alert", alert_id=alert_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to acknowledge alert: {str(e)}"
        )


@router.post(
    "/alerts/{alert_id}/resolve",
    summary="Resolve alert",
    description="Resolve an active alert with optional resolution note"
)
async def resolve_alert(
    alert_id: str = Path(..., description="Alert ID to resolve"),
    resolved_by: str = Query(..., description="Username or identifier of person resolving"),
    resolution_note: Optional[str] = Query(None, description="Optional resolution note")
):
    """Resolve an active alert."""
    try:
        alerting_service = get_enhanced_alerting_service()
        success = await alerting_service.resolve_alert(alert_id, resolved_by, resolution_note)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Alert '{alert_id}' not found or already resolved"
            )
        
        return {"success": True, "message": f"Alert {alert_id} resolved by {resolved_by}"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to resolve alert", alert_id=alert_id, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resolve alert: {str(e)}"
        )


# Recovery Actions Endpoints

@router.post(
    "/recovery",
    response_model=List[RecoveryActionResponse],
    summary="Trigger collector recovery",
    description="Manually trigger recovery actions for a failing collector"
)
async def trigger_collector_recovery(
    request: RecoveryActionRequest,
    credentials: HTTPAuthorizationCredentials = Depends(require_break_glass_auth)
):
    """Trigger automated recovery actions for a collector."""
    try:
        realtime_service = get_realtime_health_service()
        
        # Verify collector exists
        health_status = await realtime_service.get_live_health_status(request.collector_name)
        if not health_status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collector '{request.collector_name}' not found"
            )
        
        # Check if recovery is recommended (unless forced)
        if not request.force and health_status.current_status == HealthStatus.HEALTHY:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Collector '{request.collector_name}' is healthy. Use 'force=true' to proceed anyway"
            )
        
        # Trigger recovery
        failure_context = {
            "manual_recovery": True,
            "requested_by": getattr(credentials, 'username', 'unknown'),
            "reason": request.reason,
            "force": request.force
        }
        
        recovery_results = await realtime_service.trigger_recovery_actions(
            request.collector_name,
            failure_context
        )
        
        # Send recovery alert
        alerting_service = get_enhanced_alerting_service()
        await alerting_service.send_recovery_alert(
            request.collector_name,
            any(r.success for r in recovery_results),
            {
                "manual_trigger": True,
                "actions_performed": len(recovery_results),
                "successful_actions": sum(1 for r in recovery_results if r.success)
            }
        )
        
        return [
            RecoveryActionResponse(
                collector_name=action.collector_name,
                action_type=action.action_type,
                success=action.success,
                execution_time=action.execution_time,
                result_message=action.result_message,
                timestamp=action.timestamp
            )
            for action in recovery_results
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to trigger recovery", collector=request.collector_name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger recovery: {str(e)}"
        )


@router.get(
    "/statistics",
    summary="Get health monitoring statistics",
    description="Retrieve comprehensive health monitoring and alerting statistics"
)
async def get_health_statistics():
    """Get comprehensive health monitoring statistics."""
    try:
        alerting_service = get_enhanced_alerting_service()
        alert_stats = await alerting_service.get_alert_statistics()
        
        # Add health-specific statistics
        realtime_service = get_realtime_health_service()
        all_health = await realtime_service.get_all_live_health_status()
        
        health_stats = {
            "collectors_monitored": len(all_health),
            "healthy_collectors": sum(1 for m in all_health.values() if m.current_status == HealthStatus.HEALTHY),
            "degraded_collectors": sum(1 for m in all_health.values() if m.current_status == HealthStatus.DEGRADED),
            "critical_collectors": sum(1 for m in all_health.values() if m.current_status == HealthStatus.CRITICAL),
            "avg_success_rate_1h": sum(m.success_rate_1h for m in all_health.values()) / len(all_health) if all_health else 0.0,
            "avg_response_time_1h": sum(m.avg_response_time_1h for m in all_health.values()) / len(all_health) if all_health else 0.0,
        }
        
        return {
            "health_statistics": health_stats,
            "alert_statistics": alert_stats,
            "last_updated": datetime.now()
        }
        
    except Exception as e:
        logger.error("Failed to get health statistics", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve health statistics: {str(e)}"
        )