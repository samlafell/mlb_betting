"""
Workflow Services Package

Consolidates workflow automation functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/pre_game_workflow.py → PreGameWorkflowService
- src/mlb_sharp_betting/services/pipeline_orchestrator.py → WorkflowOrchestrationService
- Various workflow automation scripts → WorkflowAutomationService

New Unified Services:
- PreGameWorkflowService: Pre-game automated workflow execution
- WorkflowOrchestrationService: Multi-stage workflow coordination
- WorkflowAutomationService: Generic workflow automation engine
- NotificationWorkflowService: Notification and alert workflows
"""

from .notification_workflow_service import NotificationWorkflowService
from .pre_game_workflow_service import PreGameWorkflowService
from .workflow_automation_service import WorkflowAutomationService
from .workflow_orchestration_service import WorkflowOrchestrationService

__all__ = [
    "PreGameWorkflowService",
    "WorkflowOrchestrationService",
    "WorkflowAutomationService",
    "NotificationWorkflowService",
]
