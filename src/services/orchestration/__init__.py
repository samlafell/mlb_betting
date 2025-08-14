"""
Orchestration Services Package

Consolidates pipeline and system orchestration functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/pipeline_orchestrator.py → PipelineOrchestrationService
- Various orchestration utilities → SystemOrchestrationService

New Unified Services:
- PipelineOrchestrationService: Data pipeline coordination and execution
- SystemOrchestrationService: System-wide orchestration and coordination
- TaskOrchestrationService: Task dependency management and execution
- ResourceOrchestrationService: Resource allocation and management
"""

from .pipeline_orchestration_service import PipelineOrchestrationService

# Future services will be implemented as needed:
# - ResourceOrchestrationService: Resource allocation and management
# - SystemOrchestrationService: System-wide orchestration and coordination  
# - TaskOrchestrationService: Task dependency management and execution

__all__ = [
    "PipelineOrchestrationService",
]
