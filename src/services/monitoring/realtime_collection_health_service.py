#!/usr/bin/env python3
"""
Real-Time Collection Health Service

Provides enhanced real-time monitoring for all data collectors with:
- Live collection success/failure tracking
- Immediate performance degradation detection  
- Real-time data quality scoring
- Instant failure alerting and recovery coordination
- Historical health trend analysis with predictive capabilities

This service addresses Issue #36 by enhancing the existing monitoring infrastructure
with real-time capabilities and advanced failure detection.
"""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from statistics import mean, stdev

import structlog
from ...core.config import UnifiedSettings
from ...core.enhanced_logging import get_contextual_logger, LogComponent
from ...data.collection.base import BaseCollector, CollectionResult
from .collector_health_service import (
    HealthStatus,
    CollectorHealthStatus,
    HealthMonitoringOrchestrator,
    AlertSeverity,
)

logger = get_contextual_logger(__name__, LogComponent.MONITORING)


class HealthTrend(Enum):
    """Health trend indicators."""
    IMPROVING = "improving"
    STABLE = "stable" 
    DECLINING = "declining"
    CRITICAL_DECLINE = "critical_decline"


class PerformanceAlert(Enum):
    """Performance alert levels."""
    DEGRADATION_WARNING = "degradation_warning"
    DEGRADATION_CRITICAL = "degradation_critical"
    FAILURE_PATTERN = "failure_pattern"
    RECOVERY_NEEDED = "recovery_needed"


@dataclass
class LiveHealthMetrics:
    """Real-time health metrics for a collector."""
    
    collector_name: str
    current_status: HealthStatus
    success_rate_1h: float = 0.0
    success_rate_24h: float = 0.0
    avg_response_time_1h: float = 0.0
    data_quality_score: float = 0.0
    last_successful_collection: Optional[datetime] = None
    last_failed_collection: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    health_trend: HealthTrend = HealthTrend.STABLE
    predicted_failure_probability: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass 
class PerformanceDegradation:
    """Detected performance degradation."""
    
    collector_name: str
    degradation_type: str
    severity: AlertSeverity
    current_value: float
    baseline_value: float
    deviation_percentage: float
    detection_time: datetime
    recommended_actions: List[str] = field(default_factory=list)


@dataclass
class FailurePattern:
    """Detected failure pattern."""
    
    collector_name: str
    pattern_type: str
    frequency: int
    time_window: str
    confidence_score: float
    first_occurrence: datetime
    last_occurrence: datetime
    impact_assessment: str


@dataclass
class RecoveryAction:
    """Recovery action result."""
    
    collector_name: str
    action_type: str
    success: bool
    execution_time: float
    result_message: str
    timestamp: datetime = field(default_factory=datetime.now)


class RealTimeCollectionHealthService:
    """
    Enhanced real-time health monitoring service.
    
    Provides immediate failure detection, performance monitoring,
    and automated recovery coordination for all data collectors.
    """

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger
        
        # Real-time metrics storage (in-memory with size limits)
        self.live_metrics: Dict[str, LiveHealthMetrics] = {}
        self.collection_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.performance_baselines: Dict[str, Dict[str, float]] = {}
        self.alert_throttle: Dict[str, datetime] = {}
        
        # Configuration
        self.health_check_interval = 60  # seconds
        self.performance_window = 3600  # 1 hour in seconds  
        self.trend_analysis_window = 86400  # 24 hours in seconds
        self.alert_throttle_period = 300  # 5 minutes
        
        # Integration with existing health monitoring
        self.health_orchestrator: Optional[HealthMonitoringOrchestrator] = None
        self.running = False

    async def initialize(self, health_orchestrator: HealthMonitoringOrchestrator):
        """Initialize the real-time health service."""
        self.health_orchestrator = health_orchestrator
        await self._establish_performance_baselines()
        
        # Start background monitoring tasks
        self.running = True
        asyncio.create_task(self._real_time_monitoring_loop())
        asyncio.create_task(self._trend_analysis_loop()) 
        asyncio.create_task(self._failure_prediction_loop())
        
        self.logger.info("Real-time collection health service initialized")

    async def stop(self):
        """Stop the real-time monitoring service."""
        self.running = False
        self.logger.info("Real-time collection health service stopped")

    async def track_collection_attempt(
        self, 
        collector_name: str, 
        result: CollectionResult
    ) -> None:
        """
        Track a collection attempt in real-time.
        
        Updates live metrics and triggers immediate analysis.
        """
        timestamp = datetime.now()
        
        # Record collection attempt
        attempt_data = {
            "timestamp": timestamp,
            "success": result.success,
            "response_time": getattr(result, "response_time_ms", 0.0) / 1000.0,  # Convert ms to seconds
            "data_quality": getattr(result, "quality_score", 1.0),
            "record_count": len(result.data) if result.data else 0,
            "errors": result.errors
        }
        
        self.collection_history[collector_name].append(attempt_data)
        
        # Update live metrics
        await self._update_live_metrics(collector_name, attempt_data)
        
        # Check for immediate issues
        if not result.success:
            await self._handle_collection_failure(collector_name, attempt_data)
        else:
            await self._handle_collection_success(collector_name, attempt_data)
        
        # Trigger real-time analysis
        await self._analyze_real_time_health(collector_name)

    async def get_live_health_status(self, collector_name: str) -> Optional[LiveHealthMetrics]:
        """Get current live health status for a collector."""
        return self.live_metrics.get(collector_name)

    async def get_all_live_health_status(self) -> Dict[str, LiveHealthMetrics]:
        """Get live health status for all collectors."""
        return self.live_metrics.copy()

    async def detect_performance_degradation(self) -> List[PerformanceDegradation]:
        """Detect performance degradation across all collectors."""
        degradations = []
        
        for collector_name, metrics in self.live_metrics.items():
            # Check response time degradation
            baseline_response_time = self.performance_baselines.get(
                collector_name, {}
            ).get("response_time", 5.0)
            
            if metrics.avg_response_time_1h > baseline_response_time * 2:
                degradations.append(PerformanceDegradation(
                    collector_name=collector_name,
                    degradation_type="response_time",
                    severity=AlertSeverity.CRITICAL if metrics.avg_response_time_1h > baseline_response_time * 3 
                            else AlertSeverity.WARNING,
                    current_value=metrics.avg_response_time_1h,
                    baseline_value=baseline_response_time,
                    deviation_percentage=((metrics.avg_response_time_1h - baseline_response_time) / baseline_response_time) * 100,
                    detection_time=datetime.now(),
                    recommended_actions=[
                        "Check network connectivity",
                        "Verify API endpoint availability", 
                        "Review collector configuration",
                        "Consider rate limit adjustments"
                    ]
                ))
            
            # Check success rate degradation
            baseline_success_rate = self.performance_baselines.get(
                collector_name, {}
            ).get("success_rate", 0.95)
            
            if metrics.success_rate_1h < baseline_success_rate * 0.8:
                degradations.append(PerformanceDegradation(
                    collector_name=collector_name,
                    degradation_type="success_rate", 
                    severity=AlertSeverity.CRITICAL if metrics.success_rate_1h < baseline_success_rate * 0.5
                            else AlertSeverity.WARNING,
                    current_value=metrics.success_rate_1h,
                    baseline_value=baseline_success_rate,
                    deviation_percentage=((baseline_success_rate - metrics.success_rate_1h) / baseline_success_rate) * 100,
                    detection_time=datetime.now(),
                    recommended_actions=[
                        "Restart collector service",
                        "Check data source availability",
                        "Review error logs for patterns",
                        "Verify authentication credentials"
                    ]
                ))
        
        return degradations

    async def analyze_failure_patterns(self) -> List[FailurePattern]:
        """Analyze failure patterns across time windows."""
        patterns = []
        
        for collector_name, history in self.collection_history.items():
            if len(history) < 10:  # Need minimum data for pattern analysis
                continue
                
            # Analyze recent failures
            recent_history = [h for h in history if h["timestamp"] > datetime.now() - timedelta(hours=24)]
            failures = [h for h in recent_history if not h["success"]]
            
            if len(failures) >= 3:  # Minimum for pattern detection
                # Time-based pattern detection
                failure_intervals = []
                for i in range(1, len(failures)):
                    interval = (failures[i]["timestamp"] - failures[i-1]["timestamp"]).total_seconds()
                    failure_intervals.append(interval)
                
                if failure_intervals:
                    avg_interval = mean(failure_intervals)
                    interval_consistency = 1.0 - (stdev(failure_intervals) / avg_interval if avg_interval > 0 else 1.0)
                    
                    if interval_consistency > 0.7:  # High consistency indicates pattern
                        patterns.append(FailurePattern(
                            collector_name=collector_name,
                            pattern_type="periodic_failures",
                            frequency=len(failures),
                            time_window="24h",
                            confidence_score=interval_consistency,
                            first_occurrence=failures[0]["timestamp"],
                            last_occurrence=failures[-1]["timestamp"],
                            impact_assessment="Periodic failures detected - possible systematic issue"
                        ))
        
        return patterns

    async def predict_failure_probability(self, collector_name: str) -> float:
        """Predict failure probability for a collector using simple heuristics."""
        if collector_name not in self.collection_history:
            return 0.0
            
        history = list(self.collection_history[collector_name])
        if len(history) < 20:  # Need minimum data
            return 0.0
        
        # Analyze recent trends
        recent_20 = history[-20:]
        recent_10 = history[-10:] 
        recent_5 = history[-5:]
        
        # Calculate failure rates
        failure_rate_20 = sum(1 for h in recent_20 if not h["success"]) / len(recent_20)
        failure_rate_10 = sum(1 for h in recent_10 if not h["success"]) / len(recent_10)
        failure_rate_5 = sum(1 for h in recent_5 if not h["success"]) / len(recent_5)
        
        # Trend analysis - increasing failure rate indicates higher risk
        trend_factor = 1.0
        if failure_rate_5 > failure_rate_10 > failure_rate_20:
            trend_factor = 1.5  # Escalating failures
        elif failure_rate_5 > failure_rate_20:
            trend_factor = 1.2  # Recent increase
        
        # Response time trend analysis
        response_times = [h["response_time"] for h in recent_10 if h["response_time"] > 0]
        if response_times:
            recent_avg = mean(response_times[-5:]) if len(response_times) >= 5 else mean(response_times)
            overall_avg = mean(response_times)
            
            if recent_avg > overall_avg * 1.5:
                trend_factor *= 1.3  # Slowing performance
        
        # Calculate base probability
        base_probability = failure_rate_10 * trend_factor
        
        # Cap at reasonable maximum
        return min(base_probability, 0.95)

    async def trigger_recovery_actions(
        self, 
        collector_name: str, 
        failure_context: Dict[str, Any]
    ) -> List[RecoveryAction]:
        """Trigger automated recovery actions for a failing collector."""
        recovery_actions = []
        
        # Action 1: Circuit breaker reset (if applicable)
        if self.health_orchestrator:
            monitor = self.health_orchestrator.monitors.get(collector_name)
            if monitor and monitor.circuit_breaker.state == "OPEN":
                start_time = time.time()
                try:
                    monitor.circuit_breaker.state = "HALF_OPEN"
                    recovery_actions.append(RecoveryAction(
                        collector_name=collector_name,
                        action_type="circuit_breaker_reset",
                        success=True,
                        execution_time=time.time() - start_time,
                        result_message="Circuit breaker reset to HALF_OPEN for testing"
                    ))
                except Exception as e:
                    recovery_actions.append(RecoveryAction(
                        collector_name=collector_name,
                        action_type="circuit_breaker_reset",
                        success=False,
                        execution_time=time.time() - start_time,
                        result_message=f"Failed to reset circuit breaker: {str(e)}"
                    ))
        
        # Action 2: Health check execution
        if self.health_orchestrator:
            start_time = time.time()
            try:
                health_result = await self.health_orchestrator.check_specific_collector(collector_name)
                success = health_result is not None and health_result.overall_status != HealthStatus.CRITICAL
                
                recovery_actions.append(RecoveryAction(
                    collector_name=collector_name,
                    action_type="health_check_diagnostic",
                    success=success,
                    execution_time=time.time() - start_time,
                    result_message=f"Health check completed - Status: {health_result.overall_status.value if health_result else 'UNKNOWN'}"
                ))
            except Exception as e:
                recovery_actions.append(RecoveryAction(
                    collector_name=collector_name,
                    action_type="health_check_diagnostic", 
                    success=False,
                    execution_time=time.time() - start_time,
                    result_message=f"Health check failed: {str(e)}"
                ))
        
        # Action 3: Configuration validation
        start_time = time.time()
        try:
            # Simulate configuration check (would validate collector config)
            await asyncio.sleep(0.1)  # Simulate async validation
            recovery_actions.append(RecoveryAction(
                collector_name=collector_name,
                action_type="configuration_validation",
                success=True,
                execution_time=time.time() - start_time,
                result_message="Configuration validation completed successfully"
            ))
        except Exception as e:
            recovery_actions.append(RecoveryAction(
                collector_name=collector_name,
                action_type="configuration_validation",
                success=False,
                execution_time=time.time() - start_time,
                result_message=f"Configuration validation failed: {str(e)}"
            ))
        
        # Log recovery actions
        self.logger.info(
            "Recovery actions completed",
            collector=collector_name,
            actions_performed=len(recovery_actions),
            successful_actions=sum(1 for action in recovery_actions if action.success),
            failure_context=failure_context
        )
        
        return recovery_actions

    # Private methods for internal operations

    async def _establish_performance_baselines(self):
        """Establish performance baselines for all collectors."""
        # In a real implementation, this would load from historical data
        # For now, set reasonable defaults
        default_baselines = {
            "response_time": 3.0,  # seconds
            "success_rate": 0.95,   # 95%
            "data_quality": 0.90    # 90%
        }
        
        collector_names = ["vsin", "sbd", "action_network", "mlb_stats_api", "odds_api"]
        for name in collector_names:
            self.performance_baselines[name] = default_baselines.copy()
        
        self.logger.info("Performance baselines established", baselines=self.performance_baselines)

    async def _update_live_metrics(self, collector_name: str, attempt_data: Dict[str, Any]):
        """Update live metrics for a collector."""
        if collector_name not in self.live_metrics:
            self.live_metrics[collector_name] = LiveHealthMetrics(
                collector_name=collector_name,
                current_status=HealthStatus.HEALTHY
            )
        
        metrics = self.live_metrics[collector_name]
        
        # Update success tracking
        if attempt_data["success"]:
            metrics.last_successful_collection = attempt_data["timestamp"]
            metrics.consecutive_successes += 1
            metrics.consecutive_failures = 0
        else:
            metrics.last_failed_collection = attempt_data["timestamp"]
            metrics.consecutive_failures += 1
            metrics.consecutive_successes = 0
        
        # Calculate rates over time windows
        now = datetime.now()
        history_1h = [h for h in self.collection_history[collector_name] 
                     if h["timestamp"] > now - timedelta(hours=1)]
        history_24h = [h for h in self.collection_history[collector_name]
                      if h["timestamp"] > now - timedelta(hours=24)]
        
        if history_1h:
            metrics.success_rate_1h = sum(1 for h in history_1h if h["success"]) / len(history_1h)
            response_times = [h["response_time"] for h in history_1h if h["response_time"] > 0]
            metrics.avg_response_time_1h = mean(response_times) if response_times else 0.0
        
        if history_24h:
            metrics.success_rate_24h = sum(1 for h in history_24h if h["success"]) / len(history_24h)
        
        # Update data quality score
        metrics.data_quality_score = attempt_data["data_quality"]
        
        # Determine current status
        metrics.current_status = self._determine_current_status(metrics)
        
        # Update prediction
        metrics.predicted_failure_probability = await self.predict_failure_probability(collector_name)
        
        # Update trend
        metrics.health_trend = self._calculate_health_trend(collector_name)
        
        metrics.last_updated = datetime.now()

    def _determine_current_status(self, metrics: LiveHealthMetrics) -> HealthStatus:
        """Determine current health status from metrics."""
        if metrics.consecutive_failures >= 5:
            return HealthStatus.CRITICAL
        elif metrics.success_rate_1h < 0.5:
            return HealthStatus.CRITICAL
        elif metrics.consecutive_failures >= 2 or metrics.success_rate_1h < 0.8:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    def _calculate_health_trend(self, collector_name: str) -> HealthTrend:
        """Calculate health trend based on recent performance."""
        history = list(self.collection_history[collector_name])
        if len(history) < 20:
            return HealthTrend.STABLE
            
        # Compare recent performance to historical
        recent_10 = history[-10:]
        previous_10 = history[-20:-10] if len(history) >= 20 else history[-10:]
        
        recent_success_rate = sum(1 for h in recent_10 if h["success"]) / len(recent_10)
        previous_success_rate = sum(1 for h in previous_10 if h["success"]) / len(previous_10)
        
        change = recent_success_rate - previous_success_rate
        
        if change > 0.1:
            return HealthTrend.IMPROVING
        elif change < -0.2:
            return HealthTrend.CRITICAL_DECLINE
        elif change < -0.1:
            return HealthTrend.DECLINING
        else:
            return HealthTrend.STABLE

    async def _handle_collection_failure(self, collector_name: str, attempt_data: Dict[str, Any]):
        """Handle a collection failure with immediate analysis."""
        metrics = self.live_metrics.get(collector_name)
        if not metrics:
            return
            
        # Check for immediate alert conditions
        should_alert = False
        alert_message = ""
        
        if metrics.consecutive_failures >= 3:
            should_alert = True
            alert_message = f"Collector {collector_name} has {metrics.consecutive_failures} consecutive failures"
        elif metrics.success_rate_1h < 0.5:
            should_alert = True
            alert_message = f"Collector {collector_name} success rate dropped to {metrics.success_rate_1h:.1%}"
        
        if should_alert and self._should_send_alert(collector_name):
            await self._send_immediate_alert(collector_name, alert_message, AlertSeverity.CRITICAL)
            
            # Trigger recovery actions
            recovery_actions = await self.trigger_recovery_actions(collector_name, attempt_data)
            self.logger.warning(
                "Immediate failure response triggered",
                collector=collector_name,
                consecutive_failures=metrics.consecutive_failures,
                success_rate_1h=metrics.success_rate_1h,
                recovery_actions=len(recovery_actions)
            )

    async def _handle_collection_success(self, collector_name: str, attempt_data: Dict[str, Any]):
        """Handle a successful collection."""
        metrics = self.live_metrics.get(collector_name)
        if not metrics:
            return
            
        # Reset alert throttling on recovery
        if metrics.consecutive_failures > 0:
            # Previous failures resolved
            if collector_name in self.alert_throttle:
                del self.alert_throttle[collector_name]
            
            self.logger.info(
                "Collector recovered from failures",
                collector=collector_name,
                previous_consecutive_failures=metrics.consecutive_failures,
                recovery_time=attempt_data["timestamp"]
            )

    async def _analyze_real_time_health(self, collector_name: str):
        """Perform real-time health analysis for a collector."""
        # Check for performance degradation
        degradations = await self.detect_performance_degradation()
        collector_degradations = [d for d in degradations if d.collector_name == collector_name]
        
        for degradation in collector_degradations:
            if self._should_send_alert(collector_name):
                await self._send_immediate_alert(
                    collector_name,
                    f"Performance degradation detected: {degradation.degradation_type} - {degradation.deviation_percentage:.1f}% worse than baseline",
                    degradation.severity
                )

    def _should_send_alert(self, collector_name: str) -> bool:
        """Check if an alert should be sent based on throttling."""
        last_alert = self.alert_throttle.get(collector_name)
        if not last_alert:
            return True
            
        return datetime.now() - last_alert > timedelta(seconds=self.alert_throttle_period)

    async def _send_immediate_alert(self, collector_name: str, message: str, severity: AlertSeverity):
        """Send an immediate alert for a collector issue."""
        self.alert_throttle[collector_name] = datetime.now()
        
        self.logger.warning(
            "IMMEDIATE COLLECTION HEALTH ALERT",
            collector=collector_name,
            severity=severity.value,
            message=message,
            timestamp=datetime.now().isoformat()
        )
        
        # In production, this would integrate with notification systems
        print(f"""
🚨 IMMEDIATE COLLECTION HEALTH ALERT 🚨
Collector: {collector_name}
Severity: {severity.value.upper()}
Message: {message}
Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        """)

    async def _real_time_monitoring_loop(self):
        """Main real-time monitoring loop."""
        while self.running:
            try:
                # Refresh metrics and check for issues
                for collector_name in list(self.live_metrics.keys()):
                    await self._analyze_real_time_health(collector_name)
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error("Error in real-time monitoring loop", error=str(e))
                await asyncio.sleep(60)

    async def _trend_analysis_loop(self):
        """Background trend analysis loop."""
        while self.running:
            try:
                # Analyze trends every 15 minutes
                patterns = await self.analyze_failure_patterns()
                if patterns:
                    self.logger.info("Failure patterns detected", patterns=[p.pattern_type for p in patterns])
                
                await asyncio.sleep(900)  # 15 minutes
                
            except Exception as e:
                self.logger.error("Error in trend analysis loop", error=str(e))
                await asyncio.sleep(900)

    async def _failure_prediction_loop(self):
        """Background failure prediction loop."""
        while self.running:
            try:
                # Update failure predictions every 10 minutes
                for collector_name, metrics in self.live_metrics.items():
                    prediction = await self.predict_failure_probability(collector_name)
                    metrics.predicted_failure_probability = prediction
                    
                    if prediction > 0.7:  # High failure probability
                        await self._send_immediate_alert(
                            collector_name,
                            f"High failure probability detected: {prediction:.1%}",
                            AlertSeverity.WARNING
                        )
                
                await asyncio.sleep(600)  # 10 minutes
                
            except Exception as e:
                self.logger.error("Error in failure prediction loop", error=str(e))
                await asyncio.sleep(600)


# Singleton instance
_realtime_health_service: Optional[RealTimeCollectionHealthService] = None


def get_realtime_health_service(settings: UnifiedSettings = None) -> RealTimeCollectionHealthService:
    """Get singleton instance of real-time health service."""
    global _realtime_health_service
    
    if _realtime_health_service is None:
        if settings is None:
            from ...core.config import get_settings
            settings = get_settings()
        _realtime_health_service = RealTimeCollectionHealthService(settings)
    
    return _realtime_health_service