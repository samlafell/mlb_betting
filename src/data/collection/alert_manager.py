#!/usr/bin/env python3
"""
Collection Alert Manager

Provides real-time alerting and notification system for data collection failures.
Implements comprehensive alerting strategy to address silent failure modes.

This module is part of the solution for GitHub Issue #36: "Data Collection Fails Silently"
"""

import asyncio
import json
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

import aiohttp
import asyncpg
import structlog
from pydantic import BaseModel

from ...core.config import get_settings
from ...core.logging import get_logger, LogComponent
from .health_monitoring import (
    AlertSeverity,
    CollectionAlert,
    CollectionHealthMetrics,
    CollectionHealthResult,
    FailurePattern,
    HealthStatus
)

logger = get_logger(__name__, LogComponent.MONITORING)


@dataclass
class AlertRule:
    """Configuration for alert rules."""
    
    id: str
    name: str
    condition: str  # e.g., "confidence_score < 0.5"
    severity: AlertSeverity
    enabled: bool = True
    
    # Thresholds
    failure_count_threshold: int = 3
    time_window_minutes: int = 15
    gap_hours_threshold: float = 4.0
    
    # Rate limiting
    cooldown_minutes: int = 30
    max_alerts_per_hour: int = 10
    
    # Notification preferences
    notify_email: bool = True
    notify_slack: bool = False
    notify_webhook: bool = False


@dataclass
class AlertHistory:
    """Track alert history for rate limiting and analysis."""
    
    rule_id: str
    source: str
    last_alert_time: datetime
    alert_count_last_hour: int = 0
    total_alerts: int = 0
    
    def can_send_alert(self, rule: AlertRule) -> bool:
        """Check if alert can be sent based on rate limiting rules."""
        now = datetime.now()
        
        # Check cooldown period
        if self.last_alert_time and (now - self.last_alert_time).total_seconds() < rule.cooldown_minutes * 60:
            return False
        
        # Check hourly rate limit
        if self.alert_count_last_hour >= rule.max_alerts_per_hour:
            return False
        
        return True
    
    def record_alert(self) -> None:
        """Record that an alert was sent."""
        now = datetime.now()
        
        # Reset hourly counter if needed
        if self.last_alert_time and (now - self.last_alert_time).total_seconds() > 3600:
            self.alert_count_last_hour = 0
        
        self.last_alert_time = now
        self.alert_count_last_hour += 1
        self.total_alerts += 1


class CollectionAlertManager:
    """
    Comprehensive alert manager for data collection health monitoring.
    
    Features:
    - Real-time collection gap detection
    - Dead tuple accumulation monitoring  
    - Cascade failure detection
    - Multi-channel alerting (email, Slack, webhook)
    - Alert rate limiting and deduplication
    - Automatic recovery coordination
    """
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.settings = get_settings()
        self.logger = logger.with_context(component="alert_manager")
        
        # Alert state management
        self.alert_rules: Dict[str, AlertRule] = {}
        self.alert_history: Dict[str, AlertHistory] = {}
        self.active_alerts: Dict[str, CollectionAlert] = {}
        
        # Notification handlers
        self.notification_handlers: Dict[str, Callable] = {
            "email": self._send_email_alert,
            "slack": self._send_slack_alert,
            "webhook": self._send_webhook_alert,
        }
        
        # Database connection for gap detection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Initialize default alert rules
        self._initialize_default_rules()
        
        self.logger.info("Collection Alert Manager initialized")
    
    def _initialize_default_rules(self) -> None:
        """Initialize default alerting rules for common failure scenarios."""
        
        # Critical failure rule
        self.add_rule(AlertRule(
            id="critical_failure",
            name="Critical Collection Failure",
            condition="confidence_score < 0.3 OR consecutive_failures >= 5",
            severity=AlertSeverity.CRITICAL,
            failure_count_threshold=1,
            time_window_minutes=5,
            cooldown_minutes=15
        ))
        
        # Collection gap rule
        self.add_rule(AlertRule(
            id="collection_gap",
            name="Collection Gap Detected",
            condition="gap_duration_hours >= 4.0",
            severity=AlertSeverity.CRITICAL,
            gap_hours_threshold=4.0,
            cooldown_minutes=60
        ))
        
        # Degraded performance rule
        self.add_rule(AlertRule(
            id="degraded_performance",
            name="Degraded Collection Performance",
            condition="success_rate < 0.8 AND consecutive_failures >= 3",
            severity=AlertSeverity.WARNING,
            failure_count_threshold=3,
            time_window_minutes=30,
            cooldown_minutes=30
        ))
        
        # Dead tuple accumulation rule
        self.add_rule(AlertRule(
            id="dead_tuple_accumulation",
            name="Database Dead Tuple Accumulation",
            condition="dead_tuple_ratio > 0.5",
            severity=AlertSeverity.WARNING,
            cooldown_minutes=120  # 2 hours
        ))
        
        # Rate limiting detection rule
        self.add_rule(AlertRule(
            id="rate_limiting",
            name="API Rate Limiting Detected",
            condition="failure_patterns contains 'rate_limiting'",
            severity=AlertSeverity.WARNING,
            failure_count_threshold=2,
            time_window_minutes=10,
            cooldown_minutes=60
        ))
    
    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.alert_rules[rule.id] = rule
        self.logger.info("Alert rule added", rule_id=rule.id, name=rule.name)
    
    def remove_rule(self, rule_id: str) -> None:
        """Remove an alert rule."""
        if rule_id in self.alert_rules:
            del self.alert_rules[rule_id]
            self.logger.info("Alert rule removed", rule_id=rule_id)
    
    def enable_rule(self, rule_id: str) -> None:
        """Enable an alert rule."""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = True
            self.logger.info("Alert rule enabled", rule_id=rule_id)
    
    def disable_rule(self, rule_id: str) -> None:
        """Disable an alert rule."""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = False
            self.logger.info("Alert rule disabled", rule_id=rule_id)
    
    async def initialize_db_connection(self) -> None:
        """Initialize database connection for gap detection queries."""
        try:
            dsn = f"postgresql://{self.settings.database.user}:{self.settings.database.password}@{self.settings.database.host}:{self.settings.database.port}/{self.settings.database.database}"
            self.db_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
            self.logger.info("Database connection pool initialized for alert manager")
        except Exception as e:
            self.logger.error("Failed to initialize database connection", error=str(e))
    
    async def check_collection_gaps(self, source: str, max_gap_hours: float = 4.0) -> Optional[CollectionAlert]:
        """
        Detect gaps in data collection that might indicate silent failures.
        
        Args:
            source: Data source to check
            max_gap_hours: Maximum allowed gap in hours
            
        Returns:
            CollectionAlert if gap detected, None otherwise
        """
        if not self.db_pool:
            await self.initialize_db_connection()
        
        if not self.db_pool:
            self.logger.warning("Cannot check collection gaps - no database connection")
            return None
        
        try:
            async with self.db_pool.acquire() as conn:
                # Check for gaps in collection timestamps
                query = """
                SELECT 
                    source,
                    MAX(collected_at) as last_collection,
                    EXTRACT(EPOCH FROM (NOW() - MAX(collected_at))) / 3600 as hours_since_last
                FROM raw_data.collection_log 
                WHERE source = $1
                GROUP BY source
                HAVING EXTRACT(EPOCH FROM (NOW() - MAX(collected_at))) / 3600 > $2
                """
                
                # Try multiple possible collection log tables
                for table in ["raw_data.collection_log", "operational.collection_history", "curated.collection_metadata"]:
                    try:
                        result = await conn.fetchrow(query.replace("raw_data.collection_log", table), source, max_gap_hours)
                        if result:
                            gap_hours = result['hours_since_last']
                            
                            alert = CollectionAlert(
                                source=source,
                                alert_type="collection_gap",
                                severity=AlertSeverity.CRITICAL if gap_hours >= 8.0 else AlertSeverity.WARNING,
                                message=f"Collection gap detected for {source}: {gap_hours:.1f} hours since last collection",
                                metadata={
                                    "gap_hours": gap_hours,
                                    "last_collection": result['last_collection'].isoformat() if result['last_collection'] else None,
                                    "threshold_hours": max_gap_hours
                                },
                                recovery_suggestions=[
                                    "Check collector health and restart if necessary",
                                    "Verify network connectivity and API endpoints",
                                    "Review collector logs for error patterns"
                                ],
                                is_auto_recoverable=True
                            )
                            
                            return alert
                    except asyncpg.UndefinedTableError:
                        continue  # Try next table
                        
        except Exception as e:
            self.logger.error("Error checking collection gaps", source=source, error=str(e))
        
        return None
    
    async def check_dead_tuple_accumulation(self, threshold: float = 0.5) -> List[CollectionAlert]:
        """
        Monitor database dead tuple accumulation which indicates data corruption.
        
        Args:
            threshold: Dead tuple ratio threshold (0.0-1.0)
            
        Returns:
            List of alerts for tables with high dead tuple ratios
        """
        if not self.db_pool:
            await self.initialize_db_connection()
        
        if not self.db_pool:
            return []
        
        alerts = []
        
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                SELECT 
                    schemaname,
                    tablename,
                    n_live_tup,
                    n_dead_tup,
                    CASE 
                        WHEN n_live_tup = 0 THEN 
                            CASE WHEN n_dead_tup > 0 THEN 1.0 ELSE 0.0 END
                        ELSE n_dead_tup::float / n_live_tup::float 
                    END as dead_tuple_ratio
                FROM pg_stat_user_tables
                WHERE schemaname IN ('raw_data', 'staging', 'curated', 'operational')
                AND n_dead_tup > 10
                HAVING CASE 
                    WHEN n_live_tup = 0 THEN 
                        CASE WHEN n_dead_tup > 0 THEN 1.0 ELSE 0.0 END
                    ELSE n_dead_tup::float / n_live_tup::float 
                END > $1
                ORDER BY dead_tuple_ratio DESC
                """
                
                results = await conn.fetch(query, threshold)
                
                for row in results:
                    table_name = f"{row['schemaname']}.{row['tablename']}"
                    ratio = row['dead_tuple_ratio']
                    
                    alert = CollectionAlert(
                        source=table_name,
                        alert_type="dead_tuple_accumulation",
                        severity=AlertSeverity.CRITICAL if ratio > 0.8 else AlertSeverity.WARNING,
                        message=f"High dead tuple ratio in {table_name}: {ratio:.1%}",
                        metadata={
                            "table": table_name,
                            "dead_tuple_ratio": ratio,
                            "live_tuples": row['n_live_tup'],
                            "dead_tuples": row['n_dead_tup']
                        },
                        recovery_suggestions=[
                            f"Run VACUUM FULL on {table_name}",
                            "Investigate transaction patterns causing dead tuples",
                            "Check for long-running transactions or failed commits"
                        ],
                        is_auto_recoverable=False  # Requires manual intervention
                    )
                    
                    alerts.append(alert)
                    
        except Exception as e:
            self.logger.error("Error checking dead tuple accumulation", error=str(e))
        
        return alerts
    
    async def check_cascade_failures(self, failure_count: int = 3, time_window_minutes: int = 15) -> Optional[CollectionAlert]:
        """
        Detect cascade failures across multiple collectors.
        
        Args:
            failure_count: Number of sources that must fail
            time_window_minutes: Time window to check for failures
            
        Returns:
            CollectionAlert if cascade failure detected
        """
        if not self.db_pool:
            await self.initialize_db_connection()
        
        if not self.db_pool:
            return None
        
        try:
            async with self.db_pool.acquire() as conn:
                # Check for multiple source failures in recent time window
                query = """
                SELECT 
                    COUNT(DISTINCT source) as failed_sources,
                    array_agg(DISTINCT source) as sources,
                    MIN(timestamp) as first_failure,
                    MAX(timestamp) as last_failure
                FROM operational.collection_alerts
                WHERE severity IN ('WARNING', 'CRITICAL')
                AND timestamp > NOW() - INTERVAL '%s minutes'
                AND is_active = true
                HAVING COUNT(DISTINCT source) >= %s
                """
                
                # Try the query with different possible alert tables
                for table in ["operational.collection_alerts", "monitoring.alerts", "curated.system_alerts"]:
                    try:
                        result = await conn.fetchrow(
                            query.replace("operational.collection_alerts", table),
                            time_window_minutes,
                            failure_count
                        )
                        
                        if result:
                            failed_sources = result['sources']
                            
                            alert = CollectionAlert(
                                source="system",
                                alert_type="cascade_failure",
                                severity=AlertSeverity.CRITICAL,
                                message=f"Cascade failure detected: {result['failed_sources']} sources failing simultaneously",
                                metadata={
                                    "failed_sources": failed_sources,
                                    "failure_count": result['failed_sources'],
                                    "time_window_minutes": time_window_minutes,
                                    "first_failure": result['first_failure'].isoformat() if result['first_failure'] else None,
                                    "last_failure": result['last_failure'].isoformat() if result['last_failure'] else None
                                },
                                recovery_suggestions=[
                                    "Check network connectivity and DNS resolution",
                                    "Verify if external APIs are experiencing outages",
                                    "Review system resources (CPU, memory, disk)",
                                    "Check database connection pool status"
                                ],
                                is_auto_recoverable=True
                            )
                            
                            return alert
                            
                    except asyncpg.UndefinedTableError:
                        continue  # Try next table
                        
        except Exception as e:
            self.logger.error("Error checking cascade failures", error=str(e))
        
        return None
    
    async def evaluate_health_result(self, result: CollectionHealthResult) -> List[CollectionAlert]:
        """
        Evaluate a health result against all alert rules.
        
        Args:
            result: Collection health result to evaluate
            
        Returns:
            List of alerts triggered by the result
        """
        alerts = []
        
        for rule in self.alert_rules.values():
            if not rule.enabled:
                continue
            
            # Check if rule conditions are met
            if self._evaluate_rule_condition(rule, result):
                # Check rate limiting
                history_key = f"{rule.id}:{result.source}"
                if history_key not in self.alert_history:
                    self.alert_history[history_key] = AlertHistory(rule.id, result.source, datetime.min)
                
                history = self.alert_history[history_key]
                if history.can_send_alert(rule):
                    alert = CollectionAlert(
                        source=result.source,
                        alert_type=rule.id,
                        severity=rule.severity,
                        message=f"{rule.name}: {result.alert_message if result.alert_message else 'Condition met'}",
                        metadata={
                            "rule_id": rule.id,
                            "rule_name": rule.name,
                            "confidence_score": result.confidence_score,
                            "health_status": result.health_status.value,
                            "detected_patterns": [p.value for p in result.detected_patterns]
                        },
                        failure_patterns=result.detected_patterns,
                        recovery_suggestions=result.recovery_suggestions,
                        is_auto_recoverable=result.is_recoverable
                    )
                    
                    alerts.append(alert)
                    history.record_alert()
        
        return alerts
    
    def _evaluate_rule_condition(self, rule: AlertRule, result: CollectionHealthResult) -> bool:
        """Evaluate if a rule condition is met by a health result."""
        
        # Simple condition evaluation (could be enhanced with proper parser)
        condition = rule.condition.lower()
        
        if "confidence_score <" in condition:
            threshold = float(condition.split("confidence_score <")[1].strip().split()[0])
            if result.confidence_score < threshold:
                return True
        
        if "gap_duration_hours >=" in condition:
            # This would need to be checked separately with database queries
            return False  # Handled by check_collection_gaps
        
        if "success_rate <" in condition:
            # Would need historical success rate data
            return False  # Handled by separate metrics tracking
        
        if "failure_patterns contains" in condition:
            pattern_name = condition.split("'")[1]
            return any(p.value == pattern_name for p in result.detected_patterns)
        
        return False
    
    async def send_alert(self, alert: CollectionAlert) -> bool:
        """
        Send an alert through configured notification channels.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if alert was sent successfully
        """
        sent_successfully = False
        
        # Store alert for tracking
        self.active_alerts[alert.id] = alert
        
        # Send through configured channels
        for channel, handler in self.notification_handlers.items():
            try:
                await handler(alert)
                sent_successfully = True
                self.logger.info("Alert sent", alert_id=alert.id, channel=channel, source=alert.source)
            except Exception as e:
                self.logger.error("Failed to send alert", alert_id=alert.id, channel=channel, error=str(e))
        
        return sent_successfully
    
    async def _send_email_alert(self, alert: CollectionAlert) -> None:
        """Send alert via email."""
        # Email configuration would come from settings
        # This is a basic implementation
        email_config = self.config.get("email", {})
        if not email_config.get("enabled", False):
            return
        
        # Create email message
        msg = MIMEMultipart()
        msg['From'] = email_config.get("from", "noreply@example.com")
        msg['To'] = email_config.get("to", "admin@example.com")
        msg['Subject'] = f"[{alert.severity.value.upper()}] Collection Alert: {alert.source}"
        
        body = f"""
Collection Alert Details:
- Source: {alert.source}
- Alert Type: {alert.alert_type}
- Severity: {alert.severity.value.upper()}
- Message: {alert.message}
- Time: {alert.created_at}

Recovery Suggestions:
{chr(10).join(['- ' + suggestion for suggestion in alert.recovery_suggestions])}

Metadata:
{json.dumps(alert.metadata, indent=2, default=str)}
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email (would need proper SMTP configuration)
        self.logger.info("Email alert prepared", alert_id=alert.id)
    
    async def _send_slack_alert(self, alert: CollectionAlert) -> None:
        """Send alert via Slack webhook."""
        slack_config = self.config.get("slack", {})
        if not slack_config.get("enabled", False):
            return
        
        webhook_url = slack_config.get("webhook_url")
        if not webhook_url:
            return
        
        # Create Slack message
        color = {
            AlertSeverity.INFO: "good",
            AlertSeverity.WARNING: "warning", 
            AlertSeverity.CRITICAL: "danger"
        }.get(alert.severity, "warning")
        
        payload = {
            "attachments": [{
                "color": color,
                "title": f"Collection Alert: {alert.source}",
                "text": alert.message,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                    {"title": "Alert Type", "value": alert.alert_type, "short": True},
                    {"title": "Time", "value": alert.created_at.isoformat(), "short": True}
                ]
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                if response.status == 200:
                    self.logger.info("Slack alert sent", alert_id=alert.id)
                else:
                    self.logger.error("Failed to send Slack alert", alert_id=alert.id, status=response.status)
    
    async def _send_webhook_alert(self, alert: CollectionAlert) -> None:
        """Send alert via generic webhook."""
        webhook_config = self.config.get("webhook", {})
        if not webhook_config.get("enabled", False):
            return
        
        webhook_url = webhook_config.get("url")
        if not webhook_url:
            return
        
        # Create webhook payload
        payload = {
            "alert_id": alert.id,
            "source": alert.source,
            "alert_type": alert.alert_type,
            "severity": alert.severity.value,
            "message": alert.message,
            "created_at": alert.created_at.isoformat(),
            "metadata": alert.metadata,
            "recovery_suggestions": alert.recovery_suggestions
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "MLB-Betting-Alert-Manager/1.0"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload, headers=headers) as response:
                if response.status == 200:
                    self.logger.info("Webhook alert sent", alert_id=alert.id, url=webhook_url)
                else:
                    self.logger.error("Failed to send webhook alert", alert_id=alert.id, status=response.status, url=webhook_url)
    
    async def resolve_alert(self, alert_id: str, resolution_notes: str = "") -> bool:
        """
        Resolve an active alert.
        
        Args:
            alert_id: ID of alert to resolve
            resolution_notes: Notes about how the alert was resolved
            
        Returns:
            True if alert was resolved successfully
        """
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.is_active = False
            alert.resolved_at = datetime.now()
            alert.resolution_notes = resolution_notes
            
            self.logger.info("Alert resolved", alert_id=alert_id, source=alert.source, notes=resolution_notes)
            return True
        
        return False
    
    def get_active_alerts(self, source: Optional[str] = None) -> List[CollectionAlert]:
        """Get list of active alerts, optionally filtered by source."""
        alerts = [alert for alert in self.active_alerts.values() if alert.is_active]
        
        if source:
            alerts = [alert for alert in alerts if alert.source == source]
        
        return sorted(alerts, key=lambda a: a.created_at, reverse=True)
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert manager status."""
        active_alerts = self.get_active_alerts()
        
        return {
            "active_alerts": len(active_alerts),
            "critical_alerts": len([a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]),
            "warning_alerts": len([a for a in active_alerts if a.severity == AlertSeverity.WARNING]),
            "alert_rules": len(self.alert_rules),
            "enabled_rules": len([r for r in self.alert_rules.values() if r.enabled]),
            "notification_channels": list(self.notification_handlers.keys()),
            "last_alert_time": max([a.created_at for a in active_alerts], default=None)
        }


__all__ = [
    "AlertRule",
    "AlertHistory", 
    "CollectionAlertManager"
]