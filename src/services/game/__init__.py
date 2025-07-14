"""
Game Management Services Package

Consolidates game-related functionality from legacy modules:

Legacy Service Mappings:
- src/mlb_sharp_betting/services/game_manager.py → GameManagerService
- src/mlb_sharp_betting/services/game_updater.py → GameUpdateService
- src/mlb_sharp_betting/services/game_outcome_service.py → Enhanced GameOutcomeService

New Unified Services:
- GameManagerService: Game record management and database operations
- GameUpdateService: Game information updates and MLB API integration
- GameLifecycleService: Game state management and workflow coordination
- GameValidationService: Game data validation and quality assurance
"""

from .game_manager_service import GameManagerService
from .game_update_service import GameUpdateService
from .game_lifecycle_service import GameLifecycleService
from .game_validation_service import GameValidationService

__all__ = [
    'GameManagerService',
    'GameUpdateService', 
    'GameLifecycleService',
    'GameValidationService'
] 