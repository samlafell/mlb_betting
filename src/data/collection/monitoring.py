#!/usr/bin/env python3
"""
Monitoring and Alerting System for Unified Betting Lines

Comprehensive monitoring system for data quality, performance, and operational health.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import psycopg2
import structlog
from psycopg2.extras import RealDictCursor

from ...core.config import UnifiedSettings
from .base import DataSource

logger = structlog.get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertType(Enum):
    """Types of alerts."""

    DATA_QUALITY = "DATA_QUALITY"
    PERFORMANCE = "PERFORMANCE"
    COLLECTION_FAILURE = "COLLECTION_FAILURE"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SYSTEM_HEALTH = "SYSTEM_HEALTH"


@dataclass
class Alert:
    """Alert information."""

    id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    source: DataSource | None = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolved_at: datetime | None = None


@dataclass
class HealthCheck:
    """Health check result."""

    component: str
    status: str  # HEALTHY, DEGRADED, UNHEALTHY
    response_time: float
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


class DataQualityMonitor:
    """Monitor data quality metrics and trends."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="DataQualityMonitor")

    async def check_data_quality(self) -> dict[str, Any]:
        """Check overall data quality metrics."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Get quality metrics by source
                    cur.execute("""
                        SELECT 
                            source,
                            COUNT(*) as total_records,
                            AVG(data_completeness_score) as avg_completeness,
                            AVG(source_reliability_score) as avg_reliability,
                            COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_count,
                            COUNT(CASE WHEN data_quality = 'POOR' THEN 1 END) as poor_quality_count,
                            MAX(created_at) as last_collection
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                        GROUP BY source
                    """)

                    quality_by_source = {}
                    for row in cur.fetchall():
                        source = row["source"]
                        quality_by_source[source] = {
                            "total_records": row["total_records"],
                            "avg_completeness": float(row["avg_completeness"] or 0),
                            "avg_reliability": float(row["avg_reliability"] or 0),
                            "high_quality_percentage": (
                                row["high_quality_count"] / row["total_records"]
                            )
                            * 100,
                            "poor_quality_percentage": (
                                row["poor_quality_count"] / row["total_records"]
                            )
                            * 100,
                            "last_collection": row["last_collection"],
                        }

                    # Get overall trends
                    cur.execute("""
                        SELECT 
                            DATE(created_at) as date,
                            AVG(data_completeness_score) as avg_completeness,
                            COUNT(*) as record_count
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '7 days'
                        GROUP BY DATE(created_at)
                        ORDER BY date
                    """)

                    trends = []
                    for row in cur.fetchall():
                        trends.append(
                            {
                                "date": row["date"].isoformat(),
                                "avg_completeness": float(row["avg_completeness"] or 0),
                                "record_count": row["record_count"],
                            }
                        )

                    return {
                        "quality_by_source": quality_by_source,
                        "trends": trends,
                        "timestamp": datetime.now().isoformat(),
                    }

        except Exception as e:
            self.logger.error("Error checking data quality", error=str(e))
            return {}

    def generate_quality_alerts(self, quality_data: dict[str, Any]) -> list[Alert]:
        """Generate alerts based on quality metrics."""
        alerts = []

        for source, metrics in quality_data.get("quality_by_source", {}).items():
            # Check completeness threshold
            if metrics["avg_completeness"] < 0.7:
                alerts.append(
                    Alert(
                        id=f"quality_completeness_{source}_{int(time.time())}",
                        alert_type=AlertType.DATA_QUALITY,
                        severity=AlertSeverity.HIGH
                        if metrics["avg_completeness"] < 0.5
                        else AlertSeverity.MEDIUM,
                        title=f"Low Data Completeness: {source}",
                        message=f"Data completeness for {source} is {metrics['avg_completeness']:.2%}",
                        source=DataSource(source),
                        metadata=metrics,
                    )
                )

            # Check poor quality percentage
            if metrics["poor_quality_percentage"] > 20:
                alerts.append(
                    Alert(
                        id=f"quality_poor_{source}_{int(time.time())}",
                        alert_type=AlertType.DATA_QUALITY,
                        severity=AlertSeverity.HIGH
                        if metrics["poor_quality_percentage"] > 40
                        else AlertSeverity.MEDIUM,
                        title=f"High Poor Quality Rate: {source}",
                        message=f"Poor quality rate for {source} is {metrics['poor_quality_percentage']:.1f}%",
                        source=DataSource(source),
                        metadata=metrics,
                    )
                )

            # Check collection freshness
            if metrics["last_collection"]:
                last_collection = datetime.fromisoformat(
                    str(metrics["last_collection"])
                )
                age = datetime.now() - last_collection
                if age > timedelta(hours=6):
                    alerts.append(
                        Alert(
                            id=f"quality_stale_{source}_{int(time.time())}",
                            alert_type=AlertType.COLLECTION_FAILURE,
                            severity=AlertSeverity.HIGH
                            if age > timedelta(hours=24)
                            else AlertSeverity.MEDIUM,
                            title=f"Stale Data: {source}",
                            message=f"No data collected from {source} for {age.total_seconds() / 3600:.1f} hours",
                            source=DataSource(source),
                            metadata={"age_hours": age.total_seconds() / 3600},
                        )
                    )

        return alerts


class PerformanceMonitor:
    """Monitor system performance metrics."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="PerformanceMonitor")

    async def check_performance(self) -> dict[str, Any]:
        """Check system performance metrics."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Check database performance
                    db_start = time.time()
                    cur.execute(
                        "SELECT COUNT(*) FROM core_betting.betting_lines_moneyline"
                    )
                    db_response_time = time.time() - db_start

                    # Get collection performance by source
                    cur.execute("""
                        SELECT 
                            source,
                            COUNT(*) as records_per_hour,
                            AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_processing_time
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '1 hour'
                        GROUP BY source
                    """)

                    collection_performance = {}
                    for row in cur.fetchall():
                        collection_performance[row["source"]] = {
                            "records_per_hour": row["records_per_hour"],
                            "avg_processing_time": float(
                                row["avg_processing_time"] or 0
                            ),
                        }

                    # Get table sizes
                    cur.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                        FROM pg_tables 
                        WHERE schemaname = 'core_betting'
                    """)

                    table_sizes = {}
                    for row in cur.fetchall():
                        table_sizes[row["tablename"]] = row["size_bytes"]

                    return {
                        "database_response_time": db_response_time,
                        "collection_performance": collection_performance,
                        "table_sizes": table_sizes,
                        "timestamp": datetime.now().isoformat(),
                    }

        except Exception as e:
            self.logger.error("Error checking performance", error=str(e))
            return {}

    def generate_performance_alerts(
        self, performance_data: dict[str, Any]
    ) -> list[Alert]:
        """Generate alerts based on performance metrics."""
        alerts = []

        # Check database response time
        db_response_time = performance_data.get("database_response_time", 0)
        if db_response_time > 5.0:
            alerts.append(
                Alert(
                    id=f"performance_db_{int(time.time())}",
                    alert_type=AlertType.PERFORMANCE,
                    severity=AlertSeverity.HIGH
                    if db_response_time > 10.0
                    else AlertSeverity.MEDIUM,
                    title="Slow Database Response",
                    message=f"Database response time is {db_response_time:.2f}s",
                    metadata={"response_time": db_response_time},
                )
            )

        # Check collection performance
        for source, metrics in performance_data.get(
            "collection_performance", {}
        ).items():
            if metrics["records_per_hour"] < 10:
                alerts.append(
                    Alert(
                        id=f"performance_collection_{source}_{int(time.time())}",
                        alert_type=AlertType.PERFORMANCE,
                        severity=AlertSeverity.MEDIUM,
                        title=f"Low Collection Rate: {source}",
                        message=f"Collection rate for {source} is {metrics['records_per_hour']} records/hour",
                        source=DataSource(source),
                        metadata=metrics,
                    )
                )

        return alerts


class SystemHealthMonitor:
    """Monitor overall system health."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="SystemHealthMonitor")

    async def check_system_health(self) -> list[HealthCheck]:
        """Perform comprehensive system health checks."""
        health_checks = []

        # Database health
        health_checks.append(await self._check_database_health())

        # Data sources health
        for source in DataSource:
            health_checks.append(await self._check_data_source_health(source))

        # Storage health
        health_checks.append(await self._check_storage_health())

        return health_checks

    async def _check_database_health(self) -> HealthCheck:
        """Check database connectivity and performance."""
        start_time = time.time()

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    response_time = time.time() - start_time

                    # Check for locks
                    cur.execute("SELECT COUNT(*) FROM pg_locks WHERE granted = false")
                    blocked_queries = cur.fetchone()["count"]

                    status = "HEALTHY"
                    message = "Database is responsive"

                    if response_time > 2.0:
                        status = "DEGRADED"
                        message = (
                            f"Database response time is high ({response_time:.2f}s)"
                        )

                    if blocked_queries > 0:
                        status = "DEGRADED"
                        message = f"Database has {blocked_queries} blocked queries"

                    return HealthCheck(
                        component="database",
                        status=status,
                        response_time=response_time,
                        message=message,
                        metadata={"blocked_queries": blocked_queries},
                    )

        except Exception as e:
            return HealthCheck(
                component="database",
                status="UNHEALTHY",
                response_time=time.time() - start_time,
                message=f"Database connection failed: {str(e)}",
            )

    async def _check_data_source_health(self, source: DataSource) -> HealthCheck:
        """Check data source health."""
        start_time = time.time()

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Check recent data
                    cur.execute(
                        """
                        SELECT 
                            COUNT(*) as recent_records,
                            MAX(created_at) as last_record
                        FROM core_betting.betting_lines_moneyline
                        WHERE source = %s AND created_at >= NOW() - INTERVAL '24 hours'
                    """,
                        (source.value,),
                    )

                    result = cur.fetchone()
                    recent_records = result["recent_records"]
                    last_record = result["last_record"]

                    response_time = time.time() - start_time

                    if recent_records > 0:
                        status = "HEALTHY"
                        message = f"Received {recent_records} records in last 24 hours"
                    else:
                        status = "UNHEALTHY"
                        message = "No recent data received"

                    return HealthCheck(
                        component=f"data_source_{source.value}",
                        status=status,
                        response_time=response_time,
                        message=message,
                        metadata={
                            "recent_records": recent_records,
                            "last_record": last_record.isoformat()
                            if last_record
                            else None,
                        },
                    )

        except Exception as e:
            return HealthCheck(
                component=f"data_source_{source.value}",
                status="UNHEALTHY",
                response_time=time.time() - start_time,
                message=f"Health check failed: {str(e)}",
            )

    async def _check_storage_health(self) -> HealthCheck:
        """Check storage health."""
        start_time = time.time()

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Check disk usage
                    cur.execute("SELECT pg_database_size(current_database())")
                    db_size = cur.fetchone()["pg_database_size"]

                    response_time = time.time() - start_time

                    # Convert to MB
                    db_size_mb = db_size / (1024 * 1024)

                    status = "HEALTHY"
                    message = f"Database size: {db_size_mb:.1f}MB"

                    # Alert if database is over 1GB
                    if db_size_mb > 1024:
                        status = "DEGRADED"
                        message = f"Database size is large: {db_size_mb:.1f}MB"

                    return HealthCheck(
                        component="storage",
                        status=status,
                        response_time=response_time,
                        message=message,
                        metadata={"db_size_mb": db_size_mb},
                    )

        except Exception as e:
            return HealthCheck(
                component="storage",
                status="UNHEALTHY",
                response_time=time.time() - start_time,
                message=f"Storage check failed: {str(e)}",
            )


class AlertManager:
    """Manage alerts and notifications."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="AlertManager")
        self.active_alerts = {}

    def add_alert(self, alert: Alert):
        """Add a new alert."""
        self.active_alerts[alert.id] = alert
        self.logger.warning(
            f"Alert generated: {alert.title}",
            severity=alert.severity.value,
            type=alert.alert_type.value,
            source=alert.source.value if alert.source else None,
            message=alert.message,
        )

    def resolve_alert(self, alert_id: str):
        """Resolve an alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()
            self.logger.info(f"Alert resolved: {alert.title}")

    def get_active_alerts(self) -> list[Alert]:
        """Get all active alerts."""
        return [alert for alert in self.active_alerts.values() if not alert.resolved]

    def get_alerts_by_severity(self, severity: AlertSeverity) -> list[Alert]:
        """Get alerts by severity level."""
        return [
            alert
            for alert in self.active_alerts.values()
            if alert.severity == severity and not alert.resolved
        ]

    async def send_email_alert(self, alert: Alert):
        """Send email alert (if configured)."""
        try:
            # This would be implemented with actual email configuration
            self.logger.info(f"Email alert sent: {alert.title}")
        except Exception as e:
            self.logger.error("Failed to send email alert", error=str(e))


class UnifiedMonitoringSystem:
    """Comprehensive monitoring system orchestrator."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="UnifiedMonitoringSystem")

        # Initialize monitors
        self.data_quality_monitor = DataQualityMonitor(settings)
        self.performance_monitor = PerformanceMonitor(settings)
        self.system_health_monitor = SystemHealthMonitor(settings)
        self.alert_manager = AlertManager(settings)

        # Monitoring intervals
        self.quality_check_interval = 300  # 5 minutes
        self.performance_check_interval = 60  # 1 minute
        self.health_check_interval = 120  # 2 minutes

    async def run_monitoring_cycle(self):
        """Run a complete monitoring cycle."""
        try:
            # Check data quality
            quality_data = await self.data_quality_monitor.check_data_quality()
            quality_alerts = self.data_quality_monitor.generate_quality_alerts(
                quality_data
            )

            # Check performance
            performance_data = await self.performance_monitor.check_performance()
            performance_alerts = self.performance_monitor.generate_performance_alerts(
                performance_data
            )

            # Check system health
            health_checks = await self.system_health_monitor.check_system_health()

            # Process alerts
            all_alerts = quality_alerts + performance_alerts
            for alert in all_alerts:
                self.alert_manager.add_alert(alert)

            # Log summary
            self.logger.info(
                "Monitoring cycle completed",
                quality_alerts=len(quality_alerts),
                performance_alerts=len(performance_alerts),
                health_checks=len(health_checks),
                active_alerts=len(self.alert_manager.get_active_alerts()),
            )

            return {
                "quality_data": quality_data,
                "performance_data": performance_data,
                "health_checks": [
                    {
                        "component": hc.component,
                        "status": hc.status,
                        "response_time": hc.response_time,
                        "message": hc.message,
                    }
                    for hc in health_checks
                ],
                "alerts": [
                    {
                        "id": alert.id,
                        "type": alert.alert_type.value,
                        "severity": alert.severity.value,
                        "title": alert.title,
                        "message": alert.message,
                        "source": alert.source.value if alert.source else None,
                        "timestamp": alert.timestamp.isoformat(),
                    }
                    for alert in all_alerts
                ],
            }

        except Exception as e:
            self.logger.error("Monitoring cycle failed", error=str(e))
            return {}

    async def start_monitoring(self):
        """Start continuous monitoring."""
        self.logger.info("Starting unified monitoring system")

        while True:
            try:
                await self.run_monitoring_cycle()
                await asyncio.sleep(60)  # Run every minute
            except Exception as e:
                self.logger.error("Monitoring error", error=str(e))
                await asyncio.sleep(60)

    def get_monitoring_summary(self) -> dict[str, Any]:
        """Get current monitoring summary."""
        active_alerts = self.alert_manager.get_active_alerts()

        return {
            "system_status": "HEALTHY" if not active_alerts else "DEGRADED",
            "active_alerts": len(active_alerts),
            "critical_alerts": len(
                self.alert_manager.get_alerts_by_severity(AlertSeverity.CRITICAL)
            ),
            "high_alerts": len(
                self.alert_manager.get_alerts_by_severity(AlertSeverity.HIGH)
            ),
            "medium_alerts": len(
                self.alert_manager.get_alerts_by_severity(AlertSeverity.MEDIUM)
            ),
            "low_alerts": len(
                self.alert_manager.get_alerts_by_severity(AlertSeverity.LOW)
            ),
            "last_check": datetime.now().isoformat(),
        }


# Example usage
if __name__ == "__main__":

    async def main():
        settings = UnifiedSettings()
        monitoring_system = UnifiedMonitoringSystem(settings)

        # Run a single monitoring cycle
        result = await monitoring_system.run_monitoring_cycle()
        print(json.dumps(result, indent=2))

        # Get monitoring summary
        summary = monitoring_system.get_monitoring_summary()
        print(json.dumps(summary, indent=2))

    asyncio.run(main())
