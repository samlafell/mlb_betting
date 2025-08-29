"""
Automated Retraining Services

This package provides automated retraining workflows that continuously improve
betting strategies as new game outcomes and market data become available.

The system ensures betting strategies stay profitable as baseball and betting
markets evolve through automated triggers, workflow execution, and deployment
management.

Key Components:
- RetrainingTriggerService: Detects when retraining is needed
- AutomatedRetrainingEngine: Manages retraining workflows
- ModelValidationService: Validates new models before deployment  
- RetrainingScheduler: Manages scheduled and triggered retraining jobs
- PerformanceMonitoringService: Tracks strategy performance over time
"""

from .trigger_service import RetrainingTriggerService, TriggerType, TriggerCondition, TriggerSeverity
from .automated_engine import AutomatedRetrainingEngine, RetrainingJob, RetrainingStatus, RetrainingStrategy, RetrainingConfiguration
from .model_validation_service import ModelValidationService, ValidationResult, ValidationLevel
from .performance_monitoring_service import PerformanceMonitoringService, PerformanceTrend
from .scheduler import RetrainingScheduler, ScheduleType, SchedulePriority

__all__ = [
    "RetrainingTriggerService",
    "TriggerType", 
    "TriggerCondition",
    "TriggerSeverity",
    "AutomatedRetrainingEngine",
    "RetrainingJob",
    "RetrainingStatus",
    "RetrainingStrategy",
    "RetrainingConfiguration",
    "ModelValidationService",
    "ValidationResult",
    "ValidationLevel",
    "PerformanceMonitoringService", 
    "PerformanceTrend",
    "RetrainingScheduler",
    "ScheduleType",
    "SchedulePriority",
]