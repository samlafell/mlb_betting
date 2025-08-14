#!/usr/bin/env python3
"""
Enhanced Automated Alerting Service

Provides comprehensive multi-channel alerting with:
- Smart alert throttling and escalation  
- Multi-channel notifications (console, webhook, email)
- Alert correlation and grouping
- Business impact assessment
- Integration with existing monitoring infrastructure

This service enhances Issue #36 by providing sophisticated alerting
capabilities that prevent alert fatigue and ensure critical issues
are properly escalated.
"""

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable
from urllib.parse import urljoin

import aiohttp
import structlog
from ...core.config import UnifiedSettings
from ...core.enhanced_logging import get_contextual_logger, LogComponent
from .collector_health_service import AlertSeverity
from .realtime_collection_health_service import PerformanceDegradation, FailurePattern

logger = get_contextual_logger(__name__, LogComponent.MONITORING)


class AlertChannel(Enum):
    """Available alert channels."""
    CONSOLE = "console"
    WEBHOOK = "webhook"
    EMAIL = "email"
    SLACK = "slack"


class AlertPriority(Enum):
    """Alert priority levels for escalation."""
    LOW = 1
    MEDIUM = 2  
    HIGH = 3
    CRITICAL = 4
    EMERGENCY = 5


class EscalationLevel(Enum):
    """Escalation levels for alerts."""
    TEAM = "team"
    MANAGER = "manager"
    EXECUTIVE = "executive"
    EMERGENCY = "emergency"


@dataclass
class AlertRule:
    """Configuration for alert rules."""
    
    alert_type: str
    severity_threshold: AlertSeverity
    channels: List[AlertChannel]
    throttle_minutes: int = 5
    escalation_minutes: int = 30
    max_escalations: int = 3
    business_impact_threshold: float = 0.7
    auto_recovery_enabled: bool = True


@dataclass 
class Alert:
    """Enhanced alert with correlation and business impact."""
    
    id: str
    alert_type: str
    severity: AlertSeverity
    priority: AlertPriority
    title: str
    message: str
    collector_name: Optional[str] = None
    business_impact_score: float = 0.0
    affected_services: List[str] = field(default_factory=list)
    correlation_group: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    escalation_level: EscalationLevel = EscalationLevel.TEAM
    escalation_count: int = 0
    last_escalated: Optional[datetime] = None
    acknowledged: bool = False
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    
@dataclass
class AlertDeliveryResult:
    """Result of alert delivery attempt."""
    
    channel: AlertChannel
    success: bool
    delivery_time: float
    response_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class BusinessImpactAssessment:
    """Business impact assessment for alerts."""
    
    collector_name: str
    impact_score: float  # 0.0 to 1.0
    affected_data_sources: List[str]
    affected_betting_strategies: List[str]
    estimated_revenue_impact: float
    critical_path_affected: bool
    assessment_timestamp: datetime = field(default_factory=datetime.now)


class EnhancedAlertingService:
    """
    Enhanced alerting service with multi-channel delivery and intelligent escalation.
    
    Provides sophisticated alerting capabilities that prevent alert fatigue
    while ensuring critical issues receive appropriate attention.
    """

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="EnhancedAlertingService")
        
        # Alert management
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.alert_throttling: Dict[str, datetime] = {}
        self.correlation_groups: Dict[str, List[str]] = defaultdict(list)
        
        # Configuration
        self.alert_rules = self._initialize_alert_rules()
        self.notification_channels = self._initialize_notification_channels()
        self.escalation_policies = self._initialize_escalation_policies()
        
        # Statistics
        self.delivery_stats = defaultdict(lambda: {"sent": 0, "failed": 0, "avg_time": 0.0})
        
        # Background tasks
        self.running = False

    async def initialize(self):
        """Initialize the enhanced alerting service."""
        self.running = True
        
        # Start background tasks
        asyncio.create_task(self._alert_escalation_loop())
        asyncio.create_task(self._alert_correlation_loop())
        asyncio.create_task(self._alert_cleanup_loop())
        
        self.logger.info("Enhanced alerting service initialized", 
                        alert_rules=len(self.alert_rules),
                        notification_channels=list(self.notification_channels.keys()))

    async def stop(self):
        """Stop the enhanced alerting service."""
        self.running = False
        self.logger.info("Enhanced alerting service stopped")

    async def send_alert(
        self, 
        alert_type: str,
        severity: AlertSeverity,
        title: str,
        message: str,
        collector_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[AlertDeliveryResult]:
        """
        Send an alert through configured channels with intelligent routing.
        """
        # Create alert
        alert_id = f"{alert_type}_{collector_name}_{int(time.time())}"
        alert = Alert(
            id=alert_id,
            alert_type=alert_type,
            severity=severity,
            priority=self._determine_priority(severity, alert_type),
            title=title,
            message=message,
            collector_name=collector_name,
            metadata=metadata or {}
        )
        
        # Assess business impact
        if collector_name:
            impact = await self._assess_business_impact(collector_name, alert_type, severity)
            alert.business_impact_score = impact.impact_score
            alert.affected_services = impact.affected_data_sources + impact.affected_betting_strategies
        
        # Check for alert correlation
        correlation_group = await self._find_correlation_group(alert)
        if correlation_group:
            alert.correlation_group = correlation_group
        
        # Apply throttling rules
        if await self._should_throttle_alert(alert):
            self.logger.info("Alert throttled", alert_id=alert_id, alert_type=alert_type)
            return []
        
        # Store alert
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Determine delivery channels
        rule = self.alert_rules.get(alert_type, self.alert_rules.get("default"))
        if not rule or severity.value not in [s.value for s in [rule.severity_threshold, AlertSeverity.CRITICAL] if severity == AlertSeverity.CRITICAL]:
            channels = [AlertChannel.CONSOLE]  # Fallback
        else:
            channels = rule.channels
        
        # Deliver alert
        delivery_results = await self._deliver_alert(alert, channels)
        
        # Log delivery results
        successful_deliveries = [r for r in delivery_results if r.success]
        self.logger.info(
            "Alert sent",
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity.value,
            channels_attempted=len(delivery_results),
            channels_successful=len(successful_deliveries),
            business_impact=alert.business_impact_score,
            correlation_group=correlation_group
        )
        
        return delivery_results

    async def send_performance_degradation_alert(
        self, 
        degradation: PerformanceDegradation
    ) -> List[AlertDeliveryResult]:
        """Send alert for performance degradation."""
        return await self.send_alert(
            alert_type="performance_degradation",
            severity=degradation.severity,
            title=f"Performance Degradation: {degradation.collector_name}",
            message=f"{degradation.degradation_type} degraded by {degradation.deviation_percentage:.1f}% - Current: {degradation.current_value}, Baseline: {degradation.baseline_value}",
            collector_name=degradation.collector_name,
            metadata={
                "degradation_type": degradation.degradation_type,
                "current_value": degradation.current_value,
                "baseline_value": degradation.baseline_value,
                "deviation_percentage": degradation.deviation_percentage,
                "recommended_actions": degradation.recommended_actions
            }
        )

    async def send_failure_pattern_alert(
        self, 
        pattern: FailurePattern
    ) -> List[AlertDeliveryResult]:
        """Send alert for detected failure pattern."""
        severity = AlertSeverity.CRITICAL if pattern.confidence_score > 0.8 else AlertSeverity.WARNING
        
        return await self.send_alert(
            alert_type="failure_pattern", 
            severity=severity,
            title=f"Failure Pattern Detected: {pattern.collector_name}",
            message=f"{pattern.pattern_type} detected with {pattern.confidence_score:.1%} confidence - {pattern.frequency} failures in {pattern.time_window}",
            collector_name=pattern.collector_name,
            metadata={
                "pattern_type": pattern.pattern_type,
                "frequency": pattern.frequency,
                "time_window": pattern.time_window,
                "confidence_score": pattern.confidence_score,
                "impact_assessment": pattern.impact_assessment
            }
        )

    async def send_recovery_alert(
        self, 
        collector_name: str, 
        recovery_success: bool, 
        recovery_details: Dict[str, Any]
    ) -> List[AlertDeliveryResult]:
        """Send alert for recovery attempts."""
        severity = AlertSeverity.INFO if recovery_success else AlertSeverity.WARNING
        status = "successful" if recovery_success else "failed"
        
        return await self.send_alert(
            alert_type="recovery_attempt",
            severity=severity,
            title=f"Recovery {status.title()}: {collector_name}",
            message=f"Automated recovery {'succeeded' if recovery_success else 'failed'} for {collector_name}",
            collector_name=collector_name,
            metadata=recovery_details
        )

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.acknowledged = True
            alert.metadata["acknowledged_by"] = acknowledged_by
            alert.metadata["acknowledged_at"] = datetime.now().isoformat()
            
            self.logger.info(
                "Alert acknowledged",
                alert_id=alert_id,
                acknowledged_by=acknowledged_by,
                alert_type=alert.alert_type
            )
            return True
        return False

    async def resolve_alert(self, alert_id: str, resolved_by: str, resolution_note: Optional[str] = None) -> bool:
        """Resolve an active alert.""" 
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()
            alert.metadata["resolved_by"] = resolved_by
            if resolution_note:
                alert.metadata["resolution_note"] = resolution_note
                
            # Remove from active alerts
            del self.active_alerts[alert_id]
            
            self.logger.info(
                "Alert resolved",
                alert_id=alert_id,
                resolved_by=resolved_by,
                alert_type=alert.alert_type,
                resolution_note=resolution_note
            )
            return True
        return False

    async def get_active_alerts(
        self, 
        severity: Optional[AlertSeverity] = None,
        collector_name: Optional[str] = None
    ) -> List[Alert]:
        """Get active alerts with optional filtering."""
        alerts = list(self.active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
            
        if collector_name:
            alerts = [a for a in alerts if a.collector_name == collector_name]
            
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)

    async def get_alert_statistics(self) -> Dict[str, Any]:
        """Get comprehensive alert statistics."""
        active_count = len(self.active_alerts)
        total_count = len(self.alert_history)
        
        # Count by severity
        severity_counts = defaultdict(int)
        for alert in self.active_alerts.values():
            severity_counts[alert.severity.value] += 1
            
        # Count by type
        type_counts = defaultdict(int)
        for alert in self.alert_history[-100:]:  # Last 100 alerts
            type_counts[alert.alert_type] += 1
            
        # Delivery statistics
        delivery_stats = dict(self.delivery_stats)
        
        return {
            "active_alerts": active_count,
            "total_alerts_24h": total_count,
            "alerts_by_severity": dict(severity_counts),
            "alerts_by_type": dict(type_counts),
            "delivery_statistics": delivery_stats,
            "correlation_groups": len(self.correlation_groups),
            "escalated_alerts": len([a for a in self.active_alerts.values() if a.escalation_count > 0])
        }

    # Private methods

    def _initialize_alert_rules(self) -> Dict[str, AlertRule]:
        """Initialize alert rules configuration."""
        return {
            "collection_failure": AlertRule(
                alert_type="collection_failure",
                severity_threshold=AlertSeverity.WARNING,
                channels=[AlertChannel.CONSOLE, AlertChannel.WEBHOOK],
                throttle_minutes=5,
                escalation_minutes=15,
                max_escalations=2
            ),
            "performance_degradation": AlertRule(
                alert_type="performance_degradation", 
                severity_threshold=AlertSeverity.WARNING,
                channels=[AlertChannel.CONSOLE, AlertChannel.WEBHOOK],
                throttle_minutes=10,
                escalation_minutes=30,
                max_escalations=1
            ),
            "failure_pattern": AlertRule(
                alert_type="failure_pattern",
                severity_threshold=AlertSeverity.CRITICAL,
                channels=[AlertChannel.CONSOLE, AlertChannel.WEBHOOK],
                throttle_minutes=15,
                escalation_minutes=20,
                max_escalations=3
            ),
            "system_health": AlertRule(
                alert_type="system_health",
                severity_threshold=AlertSeverity.CRITICAL,
                channels=[AlertChannel.CONSOLE, AlertChannel.WEBHOOK],
                throttle_minutes=5,
                escalation_minutes=10,
                max_escalations=3
            ),
            "default": AlertRule(
                alert_type="default",
                severity_threshold=AlertSeverity.WARNING,
                channels=[AlertChannel.CONSOLE],
                throttle_minutes=10,
                escalation_minutes=30,
                max_escalations=1
            )
        }

    def _initialize_notification_channels(self) -> Dict[AlertChannel, Dict[str, Any]]:
        """Initialize notification channel configurations."""
        return {
            AlertChannel.CONSOLE: {
                "enabled": True,
                "format": "rich_text"
            },
            AlertChannel.WEBHOOK: {
                "enabled": getattr(self.settings, 'webhook_alerts_enabled', False),
                "url": getattr(self.settings, 'webhook_alert_url', None),
                "timeout": 10.0,
                "retry_count": 3
            },
            AlertChannel.EMAIL: {
                "enabled": False,  # Would configure based on settings
                "smtp_config": {}
            },
            AlertChannel.SLACK: {
                "enabled": False,  # Would configure based on settings
                "webhook_url": None
            }
        }

    def _initialize_escalation_policies(self) -> Dict[AlertPriority, Dict[str, Any]]:
        """Initialize escalation policies."""
        return {
            AlertPriority.LOW: {
                "initial_delay_minutes": 60,
                "escalation_levels": [EscalationLevel.TEAM],
                "max_escalations": 1
            },
            AlertPriority.MEDIUM: {
                "initial_delay_minutes": 30,
                "escalation_levels": [EscalationLevel.TEAM, EscalationLevel.MANAGER],
                "max_escalations": 2
            },
            AlertPriority.HIGH: {
                "initial_delay_minutes": 15,
                "escalation_levels": [EscalationLevel.TEAM, EscalationLevel.MANAGER],
                "max_escalations": 2
            },
            AlertPriority.CRITICAL: {
                "initial_delay_minutes": 5,
                "escalation_levels": [EscalationLevel.TEAM, EscalationLevel.MANAGER, EscalationLevel.EXECUTIVE],
                "max_escalations": 3
            },
            AlertPriority.EMERGENCY: {
                "initial_delay_minutes": 0,
                "escalation_levels": [EscalationLevel.EMERGENCY],
                "max_escalations": 5
            }
        }

    def _determine_priority(self, severity: AlertSeverity, alert_type: str) -> AlertPriority:
        """Determine alert priority based on severity and type."""
        if severity == AlertSeverity.CRITICAL:
            if alert_type in ["system_health", "failure_pattern"]:
                return AlertPriority.EMERGENCY
            else:
                return AlertPriority.CRITICAL
        elif severity == AlertSeverity.WARNING:
            if alert_type in ["collection_failure", "performance_degradation"]:
                return AlertPriority.HIGH
            else:
                return AlertPriority.MEDIUM
        else:
            return AlertPriority.LOW

    async def _assess_business_impact(
        self, 
        collector_name: str, 
        alert_type: str, 
        severity: AlertSeverity
    ) -> BusinessImpactAssessment:
        """Assess business impact of an alert."""
        # Simplified business impact assessment
        # In production, this would integrate with business logic
        
        impact_scores = {
            "action_network": 0.9,  # High impact - primary betting data
            "vsin": 0.7,           # Medium-high impact - sharp action data
            "sbd": 0.6,            # Medium impact - additional odds data  
            "mlb_stats_api": 0.4,  # Lower impact - supplementary data
            "odds_api": 0.5        # Medium impact - odds comparison
        }
        
        base_impact = impact_scores.get(collector_name, 0.3)
        
        # Adjust for severity
        severity_multiplier = {
            AlertSeverity.CRITICAL: 1.0,
            AlertSeverity.WARNING: 0.7,
            AlertSeverity.INFO: 0.3
        }
        
        final_impact = base_impact * severity_multiplier.get(severity, 0.5)
        
        # Determine affected services
        affected_data_sources = [collector_name] if collector_name else []
        affected_strategies = []
        
        if collector_name == "action_network":
            affected_strategies = ["sharp_action", "line_movement", "consensus_betting"]
        elif collector_name == "vsin": 
            affected_strategies = ["sharp_action", "reverse_line_movement"]
        elif collector_name in ["sbd", "odds_api"]:
            affected_strategies = ["odds_comparison", "arbitrage_detection"]
        
        return BusinessImpactAssessment(
            collector_name=collector_name,
            impact_score=final_impact,
            affected_data_sources=affected_data_sources,
            affected_betting_strategies=affected_strategies,
            estimated_revenue_impact=final_impact * 1000,  # Simplified calculation
            critical_path_affected=final_impact > 0.7
        )

    async def _find_correlation_group(self, alert: Alert) -> Optional[str]:
        """Find correlation group for alert."""
        # Simple correlation based on time proximity and collector
        correlation_window = timedelta(minutes=10)
        current_time = alert.timestamp
        
        for group_id, alert_ids in self.correlation_groups.items():
            for alert_id in alert_ids:
                if alert_id in self.active_alerts:
                    existing_alert = self.active_alerts[alert_id]
                    time_diff = abs((current_time - existing_alert.timestamp).total_seconds())
                    
                    # Correlate if same collector or related alert types within time window
                    if (time_diff < correlation_window.total_seconds() and 
                        (existing_alert.collector_name == alert.collector_name or
                         existing_alert.alert_type == alert.alert_type)):
                        return group_id
        
        # Create new correlation group if no existing group found
        if alert.severity in [AlertSeverity.CRITICAL, AlertSeverity.WARNING]:
            group_id = f"corr_{alert.collector_name}_{int(time.time())}"
            self.correlation_groups[group_id].append(alert.id)
            return group_id
        
        return None

    async def _should_throttle_alert(self, alert: Alert) -> bool:
        """Check if alert should be throttled."""
        rule = self.alert_rules.get(alert.alert_type, self.alert_rules.get("default"))
        if not rule:
            return False
            
        throttle_key = f"{alert.alert_type}_{alert.collector_name}"
        last_sent = self.alert_throttling.get(throttle_key)
        
        if last_sent:
            time_since_last = datetime.now() - last_sent
            if time_since_last.total_seconds() < rule.throttle_minutes * 60:
                return True
                
        self.alert_throttling[throttle_key] = datetime.now()
        return False

    async def _deliver_alert(self, alert: Alert, channels: List[AlertChannel]) -> List[AlertDeliveryResult]:
        """Deliver alert through specified channels."""
        delivery_results = []
        
        for channel in channels:
            if channel not in self.notification_channels:
                continue
                
            channel_config = self.notification_channels[channel]
            if not channel_config.get("enabled", False):
                continue
                
            start_time = time.time()
            
            try:
                if channel == AlertChannel.CONSOLE:
                    result = await self._deliver_console_alert(alert)
                elif channel == AlertChannel.WEBHOOK:
                    result = await self._deliver_webhook_alert(alert, channel_config)
                elif channel == AlertChannel.EMAIL:
                    result = await self._deliver_email_alert(alert, channel_config)
                elif channel == AlertChannel.SLACK:
                    result = await self._deliver_slack_alert(alert, channel_config)
                else:
                    result = AlertDeliveryResult(
                        channel=channel,
                        success=False,
                        delivery_time=0.0,
                        error_message="Unsupported channel"
                    )
                    
            except Exception as e:
                result = AlertDeliveryResult(
                    channel=channel,
                    success=False,
                    delivery_time=time.time() - start_time,
                    error_message=str(e)
                )
            
            delivery_results.append(result)
            
            # Update delivery statistics
            stats = self.delivery_stats[channel.value]
            if result.success:
                stats["sent"] += 1
            else:
                stats["failed"] += 1
            
            # Update average delivery time
            if result.delivery_time > 0:
                current_avg = stats["avg_time"]
                total_sent = stats["sent"] + stats["failed"]
                stats["avg_time"] = ((current_avg * (total_sent - 1)) + result.delivery_time) / total_sent
        
        return delivery_results

    async def _deliver_console_alert(self, alert: Alert) -> AlertDeliveryResult:
        """Deliver alert to console."""
        start_time = time.time()
        
        try:
            # Format alert for console output
            severity_emoji = {
                AlertSeverity.CRITICAL: "🔴",
                AlertSeverity.WARNING: "🟡", 
                AlertSeverity.INFO: "🔵"
            }
            
            emoji = severity_emoji.get(alert.severity, "⚪")
            
            console_message = f"""
{emoji} COLLECTION HEALTH ALERT {emoji}
Alert ID: {alert.id}
Type: {alert.alert_type}
Severity: {alert.severity.value.upper()}
Priority: {alert.priority.value}
Collector: {alert.collector_name or 'System'}
Business Impact: {alert.business_impact_score:.1%}
Title: {alert.title}
Message: {alert.message}
Timestamp: {alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")}
Correlation Group: {alert.correlation_group or 'None'}
            """
            
            print(console_message)
            
            return AlertDeliveryResult(
                channel=AlertChannel.CONSOLE,
                success=True,
                delivery_time=time.time() - start_time
            )
            
        except Exception as e:
            return AlertDeliveryResult(
                channel=AlertChannel.CONSOLE,
                success=False,
                delivery_time=time.time() - start_time,
                error_message=str(e)
            )

    async def _deliver_webhook_alert(self, alert: Alert, config: Dict[str, Any]) -> AlertDeliveryResult:
        """Deliver alert via webhook."""
        start_time = time.time()
        
        webhook_url = config.get("url")
        if not webhook_url:
            return AlertDeliveryResult(
                channel=AlertChannel.WEBHOOK,
                success=False,
                delivery_time=0.0,
                error_message="No webhook URL configured"
            )
        
        try:
            payload = {
                "alert_id": alert.id,
                "alert_type": alert.alert_type,
                "severity": alert.severity.value,
                "priority": alert.priority.value,
                "title": alert.title,
                "message": alert.message,
                "collector_name": alert.collector_name,
                "business_impact_score": alert.business_impact_score,
                "affected_services": alert.affected_services,
                "timestamp": alert.timestamp.isoformat(),
                "correlation_group": alert.correlation_group,
                "metadata": alert.metadata
            }
            
            timeout = aiohttp.ClientTimeout(total=config.get("timeout", 10.0))
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    response_data = await response.json() if response.content_type == "application/json" else await response.text()
                    
                    return AlertDeliveryResult(
                        channel=AlertChannel.WEBHOOK,
                        success=response.status < 400,
                        delivery_time=time.time() - start_time,
                        response_data={"status": response.status, "data": response_data}
                    )
                    
        except Exception as e:
            return AlertDeliveryResult(
                channel=AlertChannel.WEBHOOK,
                success=False,
                delivery_time=time.time() - start_time,
                error_message=str(e)
            )

    async def _deliver_email_alert(self, alert: Alert, config: Dict[str, Any]) -> AlertDeliveryResult:
        """Deliver alert via email (placeholder implementation)."""
        # In production, this would integrate with an email service
        return AlertDeliveryResult(
            channel=AlertChannel.EMAIL,
            success=False,
            delivery_time=0.0,
            error_message="Email delivery not implemented"
        )

    async def _deliver_slack_alert(self, alert: Alert, config: Dict[str, Any]) -> AlertDeliveryResult:
        """Deliver alert via Slack (placeholder implementation)."""
        # In production, this would integrate with Slack webhook
        return AlertDeliveryResult(
            channel=AlertChannel.SLACK,
            success=False,
            delivery_time=0.0,
            error_message="Slack delivery not implemented"
        )

    async def _alert_escalation_loop(self):
        """Background loop for alert escalation."""
        while self.running:
            try:
                current_time = datetime.now()
                
                for alert in list(self.active_alerts.values()):
                    if alert.acknowledged or alert.resolved:
                        continue
                        
                    # Check if escalation is needed
                    escalation_policy = self.escalation_policies.get(alert.priority)
                    if not escalation_policy:
                        continue
                        
                    time_since_created = (current_time - alert.timestamp).total_seconds() / 60
                    initial_delay = escalation_policy["initial_delay_minutes"]
                    
                    if (time_since_created > initial_delay and 
                        alert.escalation_count < escalation_policy["max_escalations"]):
                        
                        await self._escalate_alert(alert)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error("Error in escalation loop", error=str(e))
                await asyncio.sleep(60)

    async def _escalate_alert(self, alert: Alert):
        """Escalate an alert to the next level."""
        alert.escalation_count += 1
        alert.last_escalated = datetime.now()
        
        escalation_policy = self.escalation_policies.get(alert.priority)
        if escalation_policy and alert.escalation_count <= len(escalation_policy["escalation_levels"]):
            alert.escalation_level = escalation_policy["escalation_levels"][alert.escalation_count - 1]
        
        # Send escalation alert
        escalation_message = f"ESCALATED ALERT (Level {alert.escalation_count}): {alert.title}"
        
        await self.send_alert(
            alert_type="alert_escalation",
            severity=AlertSeverity.CRITICAL,
            title="Alert Escalation",
            message=escalation_message,
            collector_name=alert.collector_name,
            metadata={
                "original_alert_id": alert.id,
                "escalation_level": alert.escalation_level.value,
                "escalation_count": alert.escalation_count
            }
        )
        
        self.logger.warning(
            "Alert escalated",
            alert_id=alert.id,
            escalation_count=alert.escalation_count,
            escalation_level=alert.escalation_level.value
        )

    async def _alert_correlation_loop(self):
        """Background loop for alert correlation maintenance."""
        while self.running:
            try:
                # Clean up old correlation groups
                current_time = datetime.now()
                expired_groups = []
                
                for group_id, alert_ids in self.correlation_groups.items():
                    # Remove resolved alerts from correlation groups
                    active_alert_ids = [aid for aid in alert_ids if aid in self.active_alerts]
                    self.correlation_groups[group_id] = active_alert_ids
                    
                    # Mark empty groups for removal
                    if not active_alert_ids:
                        expired_groups.append(group_id)
                
                for group_id in expired_groups:
                    del self.correlation_groups[group_id]
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error("Error in correlation loop", error=str(e))
                await asyncio.sleep(300)

    async def _alert_cleanup_loop(self):
        """Background loop for alert cleanup and maintenance."""
        while self.running:
            try:
                # Clean up old throttling entries
                current_time = datetime.now()
                expired_throttles = []
                
                for key, last_sent in self.alert_throttling.items():
                    if (current_time - last_sent).total_seconds() > 3600:  # 1 hour
                        expired_throttles.append(key)
                
                for key in expired_throttles:
                    del self.alert_throttling[key]
                
                # Limit alert history size
                if len(self.alert_history) > 10000:
                    self.alert_history = self.alert_history[-5000:]  # Keep last 5000
                    
                self.logger.debug(
                    "Alert cleanup completed",
                    throttles_cleaned=len(expired_throttles),
                    history_size=len(self.alert_history)
                )
                
                await asyncio.sleep(3600)  # Check every hour
                
            except Exception as e:
                self.logger.error("Error in cleanup loop", error=str(e))
                await asyncio.sleep(3600)


# Singleton instance
_enhanced_alerting_service: Optional[EnhancedAlertingService] = None


def get_enhanced_alerting_service(settings: UnifiedSettings = None) -> EnhancedAlertingService:
    """Get singleton instance of enhanced alerting service."""
    global _enhanced_alerting_service
    
    if _enhanced_alerting_service is None:
        if settings is None:
            from ...core.config import get_settings
            settings = get_settings()
        _enhanced_alerting_service = EnhancedAlertingService(settings)
    
    return _enhanced_alerting_service