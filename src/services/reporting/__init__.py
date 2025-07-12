"""
Unified Reporting Services Package

Consolidates all reporting functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/daily_betting_report_service.py → UnifiedReportingService
- src/mlb_sharp_betting/services/betting_analysis_formatter.py → ReportFormattingService
- src/mlb_sharp_betting/services/betting_recommendation_formatter.py → RecommendationReportingService
- Various CLI reporting commands → Integrated into UnifiedReportingService

New Unified Services:
- UnifiedReportingService: Main reporting engine with daily, weekly, and custom reports
- DashboardService: Real-time dashboard data and live metrics
- ExportService: Data export in multiple formats (JSON, CSV, PDF, HTML)
- ReportSchedulingService: Automated report generation and distribution
- PerformanceReportingService: Strategy performance and analytics reporting
"""

from .unified_reporting_service import UnifiedReportingService
from .dashboard_service import DashboardService
from .export_service import ExportService
from .report_scheduling_service import ReportSchedulingService
from .performance_reporting_service import PerformanceReportingService

__all__ = [
    'UnifiedReportingService',
    'DashboardService',
    'ExportService',
    'ReportSchedulingService',
    'PerformanceReportingService'
] 