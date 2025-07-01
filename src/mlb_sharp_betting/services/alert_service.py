"""
Alert and Notification Service

Handles:
1. Real-time performance alerts
2. Threshold breach notifications
3. Daily/weekly reporting
4. Email/SMS/Slack integration (extensible)
5. Risk management alerts
"""

import asyncio
import json
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import structlog

from ..core.logging import get_logger


logger = get_logger(__name__)


# Dataclasses for compatibility with deprecated enhanced_backtesting_service
@dataclass
class BacktestingResults:
    """Legacy BacktestingResults class for compatibility."""
    backtest_date: datetime
    total_strategies_analyzed: int
    strategies_with_adequate_data: int
    profitable_strategies: int
    declining_strategies: int = 0
    stable_strategies: int = 0
    threshold_recommendations: List[Dict[str, Any]] = None
    strategy_alerts: List[Dict[str, Any]] = None
    strategy_metrics: List = None
    data_completeness_pct: float = 100.0
    game_outcome_freshness_hours: float = 0.0
    execution_time_seconds: float = 0.0
    created_at: datetime = None
    
    def __post_init__(self):
        if self.threshold_recommendations is None:
            self.threshold_recommendations = []
        if self.strategy_alerts is None:
            self.strategy_alerts = []
        if self.strategy_metrics is None:
            self.strategy_metrics = []
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"  
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertType(Enum):
    """Types of alerts that can be generated."""
    PERFORMANCE_DECLINE = "PERFORMANCE_DECLINE"
    THRESHOLD_BREACH = "THRESHOLD_BREACH"
    HIGH_PERFORMANCE = "HIGH_PERFORMANCE"
    DATA_QUALITY = "DATA_QUALITY"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    THRESHOLD_RECOMMENDATION = "THRESHOLD_RECOMMENDATION"
    DAILY_SUMMARY = "DAILY_SUMMARY"
    WEEKLY_SUMMARY = "WEEKLY_SUMMARY"


@dataclass
class Alert:
    """Structured alert object."""
    id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    data: Dict[str, Any]
    
    # Metadata
    strategy_name: Optional[str] = None
    source_book_type: Optional[str] = None
    
    # Timing
    created_at: datetime = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    
    # Actions
    action_required: bool = False
    action_description: Optional[str] = None
    
    # Escalation
    escalated: bool = False
    escalation_level: int = 0


@dataclass
class NotificationChannel:
    """Configuration for notification delivery."""
    channel_type: str  # 'email', 'sms', 'slack', 'webhook', 'console'
    enabled: bool
    config: Dict[str, Any]
    severity_filter: List[AlertSeverity]


class AlertService:
    """Comprehensive alert and notification service."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the alert service."""
        self.logger = logger.bind(service="alerts")
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Notification channels
        self.notification_channels = self._setup_notification_channels()
        
        # Alert history and state
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        
        # Rate limiting
        self.alert_cooldowns: Dict[str, datetime] = {}
        
        # Performance tracking
        self.metrics = {
            "alerts_generated": 0,
            "alerts_acknowledged": 0,
            "alerts_resolved": 0,
            "notifications_sent": 0,
            "errors": 0
        }
    
    def _load_config(self, config_path: Optional[Path]) -> Dict[str, Any]:
        """Load alert service configuration."""
        default_config = {
            "alert_retention_days": 30,
            "rate_limit_minutes": 15,  # Same alert type cooldown
            "escalation_enabled": True,
            "escalation_delay_minutes": 60,
            "max_active_alerts": 100,
            "notification_retry_attempts": 3,
            "notification_retry_delay_seconds": 30,
            
            # Thresholds
            "critical_win_rate_threshold": 0.45,
            "high_alert_win_rate_drop": 0.10,
            "data_freshness_alert_hours": 12,
            "consecutive_losses_alert": 5,
            
            # Notification settings
            "daily_report_hour": 8,  # 8 AM EST
            "weekly_report_day": 1,  # Monday
            "console_notifications": True,
            "email_notifications": False,
            "slack_notifications": False
        }
        
        if config_path and config_path.exists():
            try:
                with open(config_path) as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                self.logger.warning("Failed to load config, using defaults", error=str(e))
        
        return default_config
    
    def _setup_notification_channels(self) -> List[NotificationChannel]:
        """Setup notification channels based on configuration."""
        channels = []
        
        # Console notifications (always enabled for development)
        if self.config.get("console_notifications", True):
            channels.append(NotificationChannel(
                channel_type="console",
                enabled=True,
                config={},
                severity_filter=[AlertSeverity.MEDIUM, AlertSeverity.HIGH, AlertSeverity.CRITICAL]
            ))
        
        # Email notifications
        if self.config.get("email_notifications", False):
            email_config = self.config.get("email_config", {})
            channels.append(NotificationChannel(
                channel_type="email",
                enabled=bool(email_config.get("smtp_server")),
                config=email_config,
                severity_filter=[AlertSeverity.HIGH, AlertSeverity.CRITICAL]
            ))
        
        # Slack notifications
        if self.config.get("slack_notifications", False):
            slack_config = self.config.get("slack_config", {})
            channels.append(NotificationChannel(
                channel_type="slack",
                enabled=bool(slack_config.get("webhook_url")),
                config=slack_config,
                severity_filter=[AlertSeverity.MEDIUM, AlertSeverity.HIGH, AlertSeverity.CRITICAL]
            ))
        
        # Webhook notifications
        webhook_config = self.config.get("webhook_config", {})
        if webhook_config.get("url"):
            channels.append(NotificationChannel(
                channel_type="webhook",
                enabled=True,
                config=webhook_config,
                severity_filter=[AlertSeverity.HIGH, AlertSeverity.CRITICAL]
            ))
        
        return channels
    
    async def process_backtesting_results(self, results: BacktestingResults) -> List[Alert]:
        """Process backtesting results and generate appropriate alerts."""
        alerts = []
        
        try:
            # Data quality alerts
            if results.data_completeness_pct < 95.0:
                alert = self._create_alert(
                    alert_type=AlertType.DATA_QUALITY,
                    severity=AlertSeverity.HIGH,
                    title="Data Completeness Below Threshold",
                    message=f"Data completeness is {results.data_completeness_pct:.1f}%, below the 95% threshold",
                    data={"completeness_pct": results.data_completeness_pct},
                    action_required=True,
                    action_description="Check data collection pipeline and ensure game outcomes are being recorded"
                )
                alerts.append(alert)
            
            if results.game_outcome_freshness_hours > self.config["data_freshness_alert_hours"]:
                alert = self._create_alert(
                    alert_type=AlertType.DATA_QUALITY,
                    severity=AlertSeverity.MEDIUM,
                    title="Stale Data Detected",
                    message=f"Game outcome data is {results.game_outcome_freshness_hours:.1f} hours old",
                    data={"freshness_hours": results.game_outcome_freshness_hours},
                    action_required=True,
                    action_description="Update game outcomes from MLB API"
                )
                alerts.append(alert)
            
            # Strategy performance alerts
            for alert_data in results.strategy_alerts:
                alert = self._create_alert(
                    alert_type=AlertType(alert_data["type"]),
                    severity=AlertSeverity(alert_data["severity"]),
                    title=f"Strategy Alert: {alert_data['strategy']}",
                    message=alert_data["message"],
                    data=alert_data["data"],
                    strategy_name=alert_data["strategy"],
                    action_required=alert_data["severity"] in ["HIGH", "CRITICAL"]
                )
                alerts.append(alert)
            
            # Threshold recommendation alerts
            for rec in results.threshold_recommendations:
                alert = self._create_alert(
                    alert_type=AlertType.THRESHOLD_RECOMMENDATION,
                    severity=AlertSeverity.MEDIUM,
                    title=f"Threshold Adjustment Recommended: {rec.strategy_name}",
                    message=f"Recommend changing threshold from {rec.current_threshold} to {rec.recommended_threshold}",
                    data=asdict(rec),
                    strategy_name=rec.strategy_name,
                    action_required=True,
                    action_description=f"Review and apply threshold change in {rec.file_path}"
                )
                alerts.append(alert)
            
            # Daily summary alert
            summary_alert = self._create_daily_summary_alert(results)
            alerts.append(summary_alert)
            
            # Process and send all alerts
            for alert in alerts:
                await self._process_alert(alert)
            
            return alerts
            
        except Exception as e:
            self.logger.error("Failed to process backtesting results", error=str(e))
            error_alert = self._create_alert(
                alert_type=AlertType.SYSTEM_ERROR,
                severity=AlertSeverity.HIGH,
                title="Alert Processing Failed",
                message=f"Failed to process backtesting results: {str(e)}",
                data={"error": str(e)},
                action_required=True
            )
            await self._process_alert(error_alert)
            return [error_alert]
    
    def _create_alert(self, alert_type: AlertType, severity: AlertSeverity,
                     title: str, message: str, data: Dict[str, Any],
                     strategy_name: Optional[str] = None,
                     source_book_type: Optional[str] = None,
                     action_required: bool = False,
                     action_description: Optional[str] = None) -> Alert:
        """Create a new alert object."""
        alert_id = f"{alert_type.value}_{datetime.now(timezone.utc).isoformat()}"
        
        return Alert(
            id=alert_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            data=data,
            strategy_name=strategy_name,
            source_book_type=source_book_type,
            created_at=datetime.now(timezone.utc),
            action_required=action_required,
            action_description=action_description
        )
    
    def _create_daily_summary_alert(self, results: BacktestingResults) -> Alert:
        """Create a daily summary alert."""
        summary_data = {
            "date": results.backtest_date.strftime("%Y-%m-%d"),
            "strategies_analyzed": results.total_strategies_analyzed,
            "profitable_strategies": results.profitable_strategies,
            "declining_strategies": results.declining_strategies,
            "recommendations": len(results.threshold_recommendations),
            "data_quality": {
                "completeness": results.data_completeness_pct,
                "freshness_hours": results.game_outcome_freshness_hours
            },
            "execution_time": results.execution_time_seconds
        }
        
        # Determine overall health
        if results.declining_strategies > results.profitable_strategies:
            severity = AlertSeverity.HIGH
            health_status = "⚠️ DECLINING"
        elif results.data_completeness_pct < 95:
            severity = AlertSeverity.MEDIUM
            health_status = "🟡 DATA ISSUES"
        else:
            severity = AlertSeverity.INFO
            health_status = "✅ HEALTHY"
        
        message = f"""Daily Backtesting Summary - {health_status}
        
📊 Strategy Performance:
• {results.profitable_strategies} profitable strategies (>{52.4:.1f}% win rate)
• {results.declining_strategies} declining strategies
• {len(results.threshold_recommendations)} threshold recommendations

📈 Data Quality:
• Completeness: {results.data_completeness_pct:.1f}%
• Freshness: {results.game_outcome_freshness_hours:.1f} hours

⏱️ Execution: {results.execution_time_seconds:.1f}s"""
        
        return self._create_alert(
            alert_type=AlertType.DAILY_SUMMARY,
            severity=severity,
            title=f"Daily Summary - {results.backtest_date.strftime('%Y-%m-%d')}",
            message=message,
            data=summary_data
        )
    
    async def _process_alert(self, alert: Alert) -> None:
        """Process and potentially send an alert."""
        try:
            # Check rate limiting
            cooldown_key = f"{alert.alert_type.value}_{alert.strategy_name or 'global'}"
            now = datetime.now(timezone.utc)
            
            if cooldown_key in self.alert_cooldowns:
                time_since_last = now - self.alert_cooldowns[cooldown_key]
                cooldown_minutes = self.config["rate_limit_minutes"]
                
                if time_since_last.total_seconds() < cooldown_minutes * 60:
                    self.logger.debug("Alert rate limited", alert_type=alert.alert_type.value)
                    return
            
            # Update cooldown
            self.alert_cooldowns[cooldown_key] = now
            
            # Add to active alerts
            self.active_alerts[alert.id] = alert
            self.alert_history.append(alert)
            
            # Clean up old alerts
            self._cleanup_old_alerts()
            
            # Send notifications
            await self._send_notifications(alert)
            
            # Update metrics
            self.metrics["alerts_generated"] += 1
            
            self.logger.info("Alert processed successfully",
                           alert_id=alert.id,
                           alert_type=alert.alert_type.value,
                           severity=alert.severity.value)
            
        except Exception as e:
            self.logger.error("Failed to process alert", 
                            alert_id=alert.id, error=str(e))
            self.metrics["errors"] += 1
    
    async def _send_notifications(self, alert: Alert) -> None:
        """Send notifications through configured channels."""
        for channel in self.notification_channels:
            if not channel.enabled:
                continue
            
            # Check severity filter
            if alert.severity not in channel.severity_filter:
                continue
            
            try:
                if channel.channel_type == "console":
                    await self._send_console_notification(alert)
                elif channel.channel_type == "email":
                    await self._send_email_notification(alert, channel.config)
                elif channel.channel_type == "slack":
                    await self._send_slack_notification(alert, channel.config)
                elif channel.channel_type == "webhook":
                    await self._send_webhook_notification(alert, channel.config)
                
                self.metrics["notifications_sent"] += 1
                
            except Exception as e:
                self.logger.error("Failed to send notification",
                                channel=channel.channel_type,
                                alert_id=alert.id,
                                error=str(e))
    
    async def _send_console_notification(self, alert: Alert) -> None:
        """Send notification to console."""
        severity_emojis = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.LOW: "🟦",
            AlertSeverity.MEDIUM: "🟡", 
            AlertSeverity.HIGH: "🟠",
            AlertSeverity.CRITICAL: "🔴"
        }
        
        emoji = severity_emojis.get(alert.severity, "❓")
        timestamp = alert.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        print(f"\n{emoji} [{timestamp}] {alert.severity.value} ALERT")
        print(f"📋 {alert.title}")
        print(f"💬 {alert.message}")
        
        if alert.strategy_name:
            print(f"📊 Strategy: {alert.strategy_name}")
        
        if alert.action_required and alert.action_description:
            print(f"🎯 Action Required: {alert.action_description}")
        
        print("─" * 60)
    
    async def _send_email_notification(self, alert: Alert, config: Dict[str, Any]) -> None:
        """Send email notification."""
        if not all(k in config for k in ["smtp_server", "smtp_port", "username", "password", "from_email", "to_emails"]):
            self.logger.warning("Incomplete email configuration")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = config["from_email"]
            msg['To'] = ", ".join(config["to_emails"])
            msg['Subject'] = f"[MLB Betting] {alert.severity.value}: {alert.title}"
            
            body = f"""
{alert.message}

Alert Details:
- Type: {alert.alert_type.value}
- Severity: {alert.severity.value}
- Created: {alert.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
            
            if alert.strategy_name:
                body += f"- Strategy: {alert.strategy_name}\n"
            
            if alert.action_required:
                body += f"\nAction Required: {alert.action_description or 'Manual review needed'}\n"
            
            body += "\n---\nGenerated by MLB Sharp Betting Analytics Platform\nGeneral Balls"
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
            server.starttls()
            server.login(config["username"], config["password"])
            server.send_message(msg)
            server.quit()
            
            self.logger.info("Email notification sent", alert_id=alert.id)
            
        except Exception as e:
            self.logger.error("Failed to send email", error=str(e))
            raise
    
    async def _send_slack_notification(self, alert: Alert, config: Dict[str, Any]) -> None:
        """Send Slack notification."""
        # Implementation would use requests or aiohttp to send to webhook
        # Placeholder for now
        self.logger.info("Slack notification would be sent", alert_id=alert.id)
    
    async def _send_webhook_notification(self, alert: Alert, config: Dict[str, Any]) -> None:
        """Send webhook notification.""" 
        # Implementation would use aiohttp to POST alert data
        # Placeholder for now
        self.logger.info("Webhook notification would be sent", alert_id=alert.id)
    
    def _cleanup_old_alerts(self) -> None:
        """Clean up old alerts from memory."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.config["alert_retention_days"])
        
        # Remove old alerts from history
        self.alert_history = [a for a in self.alert_history if a.created_at > cutoff_date]
        
        # Remove resolved alerts from active list
        resolved_alerts = [aid for aid, alert in self.active_alerts.items() 
                          if alert.resolved_at and alert.resolved_at < cutoff_date]
        
        for aid in resolved_alerts:
            del self.active_alerts[aid]
        
        # Clean up old cooldowns
        self.alert_cooldowns = {k: v for k, v in self.alert_cooldowns.items() 
                               if v > cutoff_date}
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged_at = datetime.now(timezone.utc)
            self.metrics["alerts_acknowledged"] += 1
            self.logger.info("Alert acknowledged", alert_id=alert_id)
            return True
        return False
    
    async def resolve_alert(self, alert_id: str) -> bool:
        """Mark an alert as resolved."""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].resolved_at = datetime.now(timezone.utc)
            self.metrics["alerts_resolved"] += 1
            self.logger.info("Alert resolved", alert_id=alert_id)
            return True
        return False
    
    async def get_active_alerts(self, severity_filter: Optional[List[AlertSeverity]] = None) -> List[Alert]:
        """Get currently active alerts."""
        alerts = list(self.active_alerts.values())
        
        if severity_filter:
            alerts = [a for a in alerts if a.severity in severity_filter]
        
        # Sort by severity and creation time
        severity_order = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.HIGH: 1,
            AlertSeverity.MEDIUM: 2,
            AlertSeverity.LOW: 3,
            AlertSeverity.INFO: 4
        }
        
        alerts.sort(key=lambda a: (severity_order[a.severity], a.created_at), reverse=True)
        return alerts
    
    async def generate_weekly_summary(self) -> str:
        """Generate a weekly summary report."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        
        # Filter alerts from the last week
        week_alerts = [a for a in self.alert_history 
                      if start_date <= a.created_at <= end_date]
        
        # Categorize alerts
        critical_alerts = [a for a in week_alerts if a.severity == AlertSeverity.CRITICAL]
        high_alerts = [a for a in week_alerts if a.severity == AlertSeverity.HIGH]
        performance_alerts = [a for a in week_alerts if a.alert_type == AlertType.PERFORMANCE_DECLINE]
        
        report = f"""# 📊 WEEKLY ALERT SUMMARY
**Period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}

## 🚨 Alert Overview
- **Total Alerts:** {len(week_alerts)}
- **Critical:** {len(critical_alerts)}
- **High Priority:** {len(high_alerts)}
- **Performance Issues:** {len(performance_alerts)}

## 📈 System Health
"""
        
        if critical_alerts:
            report += "### 🔴 Critical Issues Detected\n"
            for alert in critical_alerts[:5]:  # Top 5
                report += f"- {alert.title}: {alert.message}\n"
        
        if performance_alerts:
            report += "### 📉 Performance Alerts\n"
            strategy_issues = {}
            for alert in performance_alerts:
                strategy = alert.strategy_name or "Unknown"
                if strategy not in strategy_issues:
                    strategy_issues[strategy] = 0
                strategy_issues[strategy] += 1
            
            for strategy, count in sorted(strategy_issues.items(), key=lambda x: x[1], reverse=True):
                report += f"- {strategy}: {count} alerts\n"
        
        report += f"""
## 📊 Alert Metrics
- **Alerts Generated:** {self.metrics['alerts_generated']}
- **Alerts Acknowledged:** {self.metrics['alerts_acknowledged']}
- **Alerts Resolved:** {self.metrics['alerts_resolved']}
- **Notifications Sent:** {self.metrics['notifications_sent']}

---
*Generated by MLB Sharp Betting Analytics Platform*
*General Balls*
"""
        
        return report
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get alert service metrics."""
        return {
            **self.metrics,
            "active_alerts_count": len(self.active_alerts),
            "alert_history_count": len(self.alert_history),
            "notification_channels": len(self.notification_channels)
        }


async def main():
    """Test the alert service."""
    service = AlertService()
    
    # Create a test alert
    test_alert = service._create_alert(
        alert_type=AlertType.HIGH_PERFORMANCE,
        severity=AlertSeverity.MEDIUM,
        title="Test Alert",
        message="This is a test alert for demonstration",
        data={"test": True},
        strategy_name="test_strategy"
    )
    
    await service._process_alert(test_alert)
    
    # Show active alerts
    active_alerts = await service.get_active_alerts()
    print(f"Active alerts: {len(active_alerts)}")


if __name__ == "__main__":
    asyncio.run(main()) 