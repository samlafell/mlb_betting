"""
Data Collector Monitoring Services

Provides comprehensive health monitoring and alerting capabilities for data collectors.

Addresses the inherent brittleness of web scraping through:
- Multi-type health checks (connectivity, parsing, schema, performance)
- Intelligent alerting with escalation policies
- Automatic recovery mechanisms
- Performance analytics and trend analysis
"""

from .collector_health_service import (
    AlertSeverity,
    CollectorHealthMonitor,
    CollectorHealthStatus,
    HealthMonitoringOrchestrator,
    HealthStatus,
)

__all__ = [
    'HealthMonitoringOrchestrator',
    'CollectorHealthMonitor',
    'HealthStatus',
    'AlertSeverity',
    'CollectorHealthStatus'
]
