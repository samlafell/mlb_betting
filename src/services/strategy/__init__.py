"""
Strategy Services Package

Consolidates strategy management functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/strategy_manager.py → StrategyManagerService
- src/mlb_sharp_betting/services/strategy_orchestrator.py → StrategyOrchestrationService
- src/mlb_sharp_betting/services/strategy_config_manager.py → StrategyConfigurationService

New Unified Services:
- StrategyManagerService: Strategy lifecycle management and configuration
- StrategyOrchestrationService: Strategy execution and coordination
- StrategyConfigurationService: Strategy configuration and validation
- StrategyPerformanceService: Strategy performance tracking and optimization
"""

from .strategy_configuration_service import StrategyConfigurationService
from .strategy_manager_service import StrategyManagerService
from .strategy_orchestration_service import StrategyOrchestrationService
from .strategy_performance_service import StrategyPerformanceService

__all__ = [
    "StrategyManagerService",
    "StrategyOrchestrationService",
    "StrategyConfigurationService",
    "StrategyPerformanceService",
]
