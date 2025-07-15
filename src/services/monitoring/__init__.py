"""
Unified Monitoring Services Package

Consolidates all monitoring functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/alert_service.py → AlertService
- Various health check scripts → UnifiedMonitoringService
- Performance tracking utilities → MetricsCollectionService
- System status commands → SystemHealthService

New Unified Services:
- UnifiedMonitoringService: Main monitoring engine with health checks and performance tracking
- AlertService: Notifications and alerting system
- MetricsCollectionService: Metrics collection and aggregation
- SystemHealthService: System health monitoring and diagnostics
- PerformanceMonitoringService: Performance monitoring and optimization
"""

from .alert_service import AlertService
from .metrics_collection_service import MetricsCollectionService
from .performance_monitoring_service import PerformanceMonitoringService
from .system_health_service import SystemHealthService
from .unified_monitoring_service import UnifiedMonitoringService

__all__ = [
    "UnifiedMonitoringService",
    "AlertService",
    "MetricsCollectionService",
    "SystemHealthService",
    "PerformanceMonitoringService",
]
