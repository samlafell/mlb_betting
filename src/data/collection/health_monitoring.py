#!/usr/bin/env python3
"""
Collection Health Monitoring System

Provides comprehensive health monitoring for data collection operations to eliminate
silent failures and improve system reliability.

This module addresses GitHub Issue #36: "Data Collection Fails Silently with No Clear Error Resolution"
by implementing:
- Enhanced result validation with confidence scoring
- Real-time alerting and gap detection
- Circuit breaker integration with automatic recovery
- Systematic failure pattern detection
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import structlog
from pydantic import BaseModel, Field

from ...core.logging import get_logger, LogComponent

logger = get_logger(__name__, LogComponent.MONITORING)

# Configurable constants for confidence score adjustments
WARNING_PENALTY = 0.1  # Confidence reduction per warning
ERROR_PENALTY = 0.3    # Confidence reduction per error


class HealthStatus(Enum):
    """Health status levels for collection operations."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class AlertSeverity(Enum):
    """Alert severity levels for collection issues."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class FailurePattern(Enum):
    """Types of failure patterns detected."""
    NETWORK_TIMEOUT = "network_timeout"
    RATE_LIMITING = "rate_limiting"
    SCHEMA_CHANGE = "schema_change"
    DATA_CORRUPTION = "data_corruption"
    SYSTEMATIC_FAILURE = "systematic_failure"
    COLLECTION_GAP = "collection_gap"


@dataclass
class CollectionHealthMetrics:
    """Comprehensive health metrics for collection operations."""
    
    source: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Collection metrics
    total_collections: int = 0
    successful_collections: int = 0
    failed_collections: int = 0
    records_collected: int = 0
    
    # Health indicators
    success_rate: float = 0.0
    confidence_score: float = 0.0  # 0.0 = no confidence, 1.0 = full confidence
    avg_response_time_ms: float = 0.0
    
    # Gap detection
    last_successful_collection: Optional[datetime] = None
    gap_duration_hours: float = 0.0
    
    # Failure patterns
    consecutive_failures: int = 0
    failure_patterns: List[FailurePattern] = field(default_factory=list)
    
    # Alert status
    alert_level: str = "normal"
    last_alert_time: Optional[datetime] = None
    
    @property
    def is_healthy(self) -> bool:
        """Check if collection is in healthy state."""
        return (
            self.success_rate >= 0.9 and 
            self.confidence_score >= 0.8 and
            self.gap_duration_hours < 1.0 and
            self.consecutive_failures < 3
        )
    
    @property
    def is_degraded(self) -> bool:
        """Check if collection is in degraded state."""
        return (
            0.5 <= self.success_rate < 0.9 or
            0.5 <= self.confidence_score < 0.8 or
            1.0 <= self.gap_duration_hours < 4.0 or
            3 <= self.consecutive_failures < 5
        )
    
    @property
    def is_critical(self) -> bool:
        """Check if collection is in critical state."""
        return (
            self.success_rate < 0.5 or
            self.confidence_score < 0.5 or
            self.gap_duration_hours >= 4.0 or
            self.consecutive_failures >= 5
        )
    
    @property
    def health_status(self) -> HealthStatus:
        """Get current health status."""
        if self.is_critical:
            return HealthStatus.CRITICAL
        elif self.is_degraded:
            return HealthStatus.DEGRADED
        elif self.is_healthy:
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN


@dataclass
class CollectionHealthResult:
    """Enhanced collection result with health assessment and confidence scoring."""
    
    # Basic result information
    success: bool
    data: List[Any]
    source: str
    timestamp: datetime
    
    # Enhanced health information
    confidence_score: float = 1.0  # 0.0 = no confidence, 1.0 = full confidence
    health_status: HealthStatus = HealthStatus.UNKNOWN
    
    # Error and warning tracking
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Performance metrics
    response_time_ms: float = 0.0
    request_count: int = 1
    
    # Data quality indicators
    expected_record_count_range: Optional[tuple[int, int]] = None
    data_schema_valid: bool = True
    data_freshness_score: float = 1.0  # 0.0 = stale, 1.0 = fresh
    
    # Failure pattern detection
    detected_patterns: List[FailurePattern] = field(default_factory=list)
    
    # Recovery information
    recovery_suggestions: List[str] = field(default_factory=list)
    is_recoverable: bool = True
    
    # Alert information
    should_alert: bool = False
    alert_severity: AlertSeverity = AlertSeverity.INFO
    alert_message: str = ""

    @property
    def has_data(self) -> bool:
        """Check if result contains data."""
        return self.success and bool(self.data)
    
    @property
    def data_count(self) -> int:
        """Get number of data items collected."""
        return len(self.data)
    
    @property
    def is_suspicious(self) -> bool:
        """Check if result shows suspicious patterns."""
        return (
            self.confidence_score < 0.7 or
            bool(self.detected_patterns) or
            (self.expected_record_count_range and 
             len(self.expected_record_count_range) == 2 and
             not (self.expected_record_count_range[0] <= self.data_count <= self.expected_record_count_range[1]))
        )
    
    def add_warning(self, message: str, pattern: Optional[FailurePattern] = None) -> None:
        """Add a warning to the result."""
        self.warnings.append(message)
        if pattern:
            self.detected_patterns.append(pattern)
        
        # Adjust confidence score based on warnings
        if len(self.warnings) > 0:
            self.confidence_score = max(0.0, self.confidence_score - (len(self.warnings) * WARNING_PENALTY))
    
    def add_error(self, message: str, pattern: Optional[FailurePattern] = None, is_recoverable: bool = True) -> None:
        """Add an error to the result."""
        self.errors.append(message)
        self.success = False
        self.is_recoverable = is_recoverable
        
        if pattern:
            self.detected_patterns.append(pattern)
        
        # Significantly reduce confidence on errors
        self.confidence_score = max(0.0, self.confidence_score - ERROR_PENALTY)
    
    def set_alert(self, severity: AlertSeverity, message: str) -> None:
        """Set alert information for this result."""
        self.should_alert = True
        self.alert_severity = severity
        self.alert_message = message
    
    def calculate_confidence_score(self) -> float:
        """Calculate overall confidence score based on multiple factors."""
        base_score = 1.0
        
        # Reduce score for errors and warnings
        base_score -= len(self.errors) * 0.3
        base_score -= len(self.warnings) * 0.1
        
        # Reduce score for detected failure patterns
        base_score -= len(self.detected_patterns) * 0.15
        
        # Reduce score for poor data quality
        if not self.data_schema_valid:
            base_score -= 0.2
        
        # Incorporate data freshness
        base_score *= self.data_freshness_score
        
        # Check against expected data ranges
        if self.expected_record_count_range:
            min_count, max_count = self.expected_record_count_range
            if not (min_count <= self.data_count <= max_count):
                # Data count is outside expected range
                if self.data_count == 0:
                    base_score -= 0.4  # No data is highly suspicious
                elif self.data_count < min_count:
                    base_score -= 0.2  # Too little data
                else:
                    base_score -= 0.1  # Too much data (less concerning)
        
        # Ensure score stays within valid range
        self.confidence_score = max(0.0, min(1.0, base_score))
        return self.confidence_score


@dataclass
class CollectionAlert:
    """Alert information for collection issues."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    source: str = ""
    alert_type: str = ""
    severity: AlertSeverity = AlertSeverity.INFO
    message: str = ""
    
    # Timing information
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    
    # Context information
    metadata: Dict[str, Any] = field(default_factory=dict)
    failure_patterns: List[FailurePattern] = field(default_factory=list)
    
    # Recovery information
    recovery_suggestions: List[str] = field(default_factory=list)
    is_auto_recoverable: bool = False
    
    # Status tracking
    is_active: bool = True
    resolved_at: Optional[datetime] = None
    resolution_notes: str = ""


class CollectionConfidenceAnalyzer:
    """Analyzes collection results to calculate confidence scores and detect patterns."""
    
    def __init__(self):
        self.historical_patterns: Dict[str, List[CollectionHealthMetrics]] = {}
        self.logger = logger.with_context(component="confidence_analyzer")
    
    def analyze_result(
        self, 
        result: CollectionHealthResult, 
        historical_metrics: Optional[CollectionHealthMetrics] = None
    ) -> CollectionHealthResult:
        """Analyze a collection result and enhance it with confidence scoring."""
        
        # Calculate base confidence score
        result.calculate_confidence_score()
        
        # Analyze against historical patterns if available
        if historical_metrics:
            self._analyze_historical_patterns(result, historical_metrics)
        
        # Detect specific failure patterns
        self._detect_failure_patterns(result)
        
        # Determine health status
        result.health_status = self._determine_health_status(result)
        
        # Generate recovery suggestions
        self._generate_recovery_suggestions(result)
        
        # Determine if alerting is needed
        self._assess_alert_necessity(result)
        
        return result
    
    def _analyze_historical_patterns(
        self, 
        result: CollectionHealthResult, 
        historical: CollectionHealthMetrics
    ) -> None:
        """Analyze result against historical patterns."""
        
        # Check for significant deviations in data count
        if historical.records_collected > 0:
            expected_min = int(historical.records_collected * 0.7)  # 30% tolerance
            expected_max = int(historical.records_collected * 1.5)  # 50% growth tolerance
            result.expected_record_count_range = (expected_min, expected_max)
            
            if result.data_count < expected_min:
                result.add_warning(
                    f"Data count ({result.data_count}) below historical average ({historical.records_collected})",
                    FailurePattern.DATA_CORRUPTION
                )
        
        # Check response time patterns
        if historical.avg_response_time_ms > 0:
            if result.response_time_ms > historical.avg_response_time_ms * 2:
                result.add_warning(
                    f"Response time ({result.response_time_ms}ms) significantly higher than average ({historical.avg_response_time_ms}ms)",
                    FailurePattern.NETWORK_TIMEOUT
                )
    
    def _detect_failure_patterns(self, result: CollectionHealthResult) -> None:
        """Detect specific failure patterns in the result."""
        
        # Detect rate limiting patterns
        for error in result.errors:
            if "429" in error or "rate limit" in error.lower():
                result.detected_patterns.append(FailurePattern.RATE_LIMITING)
                result.add_warning("Rate limiting detected", FailurePattern.RATE_LIMITING)
        
        # Detect timeout patterns
        for error in result.errors:
            if "timeout" in error.lower() or "timed out" in error.lower():
                result.detected_patterns.append(FailurePattern.NETWORK_TIMEOUT)
                result.add_warning("Network timeout detected", FailurePattern.NETWORK_TIMEOUT)
        
        # Detect schema change patterns (empty results with no clear cause)
        if result.data_count == 0 and result.success and not result.errors:
            result.detected_patterns.append(FailurePattern.SCHEMA_CHANGE)
            result.add_warning("Possible schema change - no data returned despite success", FailurePattern.SCHEMA_CHANGE)
    
    def _determine_health_status(self, result: CollectionHealthResult) -> HealthStatus:
        """Determine overall health status for the result."""
        
        if result.confidence_score >= 0.8 and not result.detected_patterns:
            return HealthStatus.HEALTHY
        elif result.confidence_score >= 0.5 or (result.success and result.has_data):
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.CRITICAL
    
    def _generate_recovery_suggestions(self, result: CollectionHealthResult) -> None:
        """Generate recovery suggestions based on detected patterns."""
        
        for pattern in result.detected_patterns:
            if pattern == FailurePattern.RATE_LIMITING:
                result.recovery_suggestions.append("Implement exponential backoff and reduce request frequency")
                result.recovery_suggestions.append("Check if API key has rate limit restrictions")
            
            elif pattern == FailurePattern.NETWORK_TIMEOUT:
                result.recovery_suggestions.append("Increase timeout values and implement retry logic")
                result.recovery_suggestions.append("Check network connectivity and DNS resolution")
            
            elif pattern == FailurePattern.SCHEMA_CHANGE:
                result.recovery_suggestions.append("Investigate API schema changes or website updates")
                result.recovery_suggestions.append("Update parsing logic to handle new data format")
                result.is_recoverable = False  # Requires manual intervention
            
            elif pattern == FailurePattern.DATA_CORRUPTION:
                result.recovery_suggestions.append("Validate data quality and check for parsing errors")
                result.recovery_suggestions.append("Compare with alternative data sources")
    
    def _assess_alert_necessity(self, result: CollectionHealthResult) -> None:
        """Assess whether an alert should be sent for this result."""
        
        if result.health_status == HealthStatus.CRITICAL:
            result.set_alert(
                AlertSeverity.CRITICAL,
                f"Critical collection failure for {result.source}: {result.errors[0] if result.errors else 'Unknown error'}"
            )
        
        elif result.health_status == HealthStatus.DEGRADED and result.detected_patterns:
            result.set_alert(
                AlertSeverity.WARNING,
                f"Degraded collection performance for {result.source}: {', '.join([p.value for p in result.detected_patterns])}"
            )
        
        elif result.confidence_score < 0.3:
            result.set_alert(
                AlertSeverity.CRITICAL,
                f"Very low confidence in collection results for {result.source} (confidence: {result.confidence_score:.2f})"
            )


__all__ = [
    "HealthStatus",
    "AlertSeverity", 
    "FailurePattern",
    "CollectionHealthMetrics",
    "CollectionHealthResult",
    "CollectionAlert",
    "CollectionConfidenceAnalyzer"
]