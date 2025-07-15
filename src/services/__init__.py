"""
Unified Services Package

This package consolidates all services from the legacy modules:
- src/mlb_sharp_betting/services/ (39+ services)
- sportsbookreview/services/ (7 services)
- action/services/ (1 service)

Into a single, organized service structure with clear categorization:

📊 Data Services:
- UnifiedDataService: Data collection, validation, and management
- DataQualityService: Data quality assessment and improvement
- DataDeduplicationService: Duplicate detection and removal

📈 Analysis Services:
- UnifiedAnalysisService: Strategy analysis and opportunity detection
- StrategyOrchestrationService: Strategy execution and coordination
- PerformanceAnalysisService: Performance tracking and optimization

🧪 Backtesting Services:
- UnifiedBacktestingService: Comprehensive backtesting engine
- StrategyValidationService: Strategy validation and testing
- PerformanceMetricsService: Performance measurement and reporting

📡 Monitoring Services:
- UnifiedMonitoringService: System health and performance monitoring
- AlertService: Notifications and alerting
- MetricsCollectionService: Metrics collection and aggregation

📋 Reporting Services:
- UnifiedReportingService: Report generation and distribution
- DashboardService: Real-time dashboard data
- ExportService: Data export and formatting

🔧 System Services:
- ConfigurationService: Configuration management
- SchedulerService: Task scheduling and automation
- MaintenanceService: System maintenance and optimization

Phase 4 Migration: Service Consolidation
✅ Eliminates service duplication across modules
✅ Provides unified service interfaces
✅ Implements consistent async patterns
✅ Enables service discovery and dependency injection
"""

# Data Services
from .data import UnifiedDataService

# Analysis Services - Temporarily commented out until implemented
# from .analysis import (
#     UnifiedAnalysisService,
#     StrategyOrchestrationService,
#     PerformanceAnalysisService
# )

# Backtesting Services - Temporarily commented out until implemented
# from .backtesting import (
#     UnifiedBacktestingService,
#     StrategyValidationService,
#     PerformanceMetricsService
# )

# Monitoring Services - Temporarily commented out until implemented
# from .monitoring import (
#     UnifiedMonitoringService,
#     AlertService,
#     MetricsCollectionService
# )

# Reporting Services - Temporarily commented out until implemented
# from .reporting import (
#     UnifiedReportingService,
#     DashboardService,
#     ExportService
# )

# System Services - Temporarily commented out until implemented
# from .system import (
#     ConfigurationService,
#     SchedulerService,
#     MaintenanceService
# )

__all__ = [
    # Data Services
    "UnifiedDataService"
    # Additional services will be uncommented as they are implemented
]
