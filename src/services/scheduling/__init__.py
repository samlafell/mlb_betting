"""
Scheduling Services Package

Consolidates scheduling and automation functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/scheduler_engine.py → SchedulerEngineService
- src/mlb_sharp_betting/services/pre_game_scheduler.py → PreGameSchedulingService
- Various automation scripts → AutomationService

New Unified Services:
- SchedulerEngineService: Core scheduling engine with cron-based job management
- PreGameSchedulingService: Pre-game workflow scheduling and automation
- TaskSchedulingService: Generic task scheduling and execution
- AutomationService: Automated pipeline execution and monitoring
"""

from .automation_service import AutomationService
from .pre_game_scheduling_service import PreGameSchedulingService
from .scheduler_engine_service import SchedulerEngineService
from .task_scheduling_service import TaskSchedulingService

__all__ = [
    "SchedulerEngineService",
    "PreGameSchedulingService",
    "TaskSchedulingService",
    "AutomationService",
]
