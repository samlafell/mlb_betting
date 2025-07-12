"""
Unified Strategy Processors

Migrated and enhanced strategy processors from the legacy mlb_sharp_betting system.
All processors now inherit from BaseStrategyProcessor and use the unified data models.

This package contains:
- Migrated strategy processors with enhanced capabilities
- Modern async-first architecture
- Unified data models and validation
- Performance monitoring and error handling
- Integration with the strategy orchestration system

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

from .sharp_action_processor import UnifiedSharpActionProcessor
from .book_conflict_processor import UnifiedBookConflictProcessor
from .timing_based_processor import UnifiedTimingBasedProcessor
from .consensus_processor import UnifiedConsensusProcessor
from .public_fade_processor import UnifiedPublicFadeProcessor
from .late_flip_processor import UnifiedLateFlipProcessor
from .underdog_value_processor import UnifiedUnderdogValueProcessor
from .line_movement_processor import UnifiedLineMovementProcessor
from .hybrid_sharp_processor import UnifiedHybridSharpProcessor

# Phase 5C: All processors migrated!
__all__ = [
    "UnifiedSharpActionProcessor",
    "UnifiedBookConflictProcessor", 
    "UnifiedTimingBasedProcessor",
    "UnifiedConsensusProcessor",
    "UnifiedPublicFadeProcessor",
    "UnifiedLateFlipProcessor",
    "UnifiedUnderdogValueProcessor",
    "UnifiedLineMovementProcessor",
    "UnifiedHybridSharpProcessor"
] 