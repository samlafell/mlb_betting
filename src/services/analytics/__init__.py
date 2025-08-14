"""
Analytics Services Module

Advanced analytics services for MLB betting system including:
- Statistical analysis (regression, correlation, hypothesis testing)
- Performance attribution modeling
- Distribution analysis and outlier detection
- Time series analysis and forecasting
"""

from .statistical_analysis_service import StatisticalAnalysisService, get_statistical_analysis_service

__all__ = ['StatisticalAnalysisService', 'get_statistical_analysis_service']