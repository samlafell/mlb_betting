"""
MLB Sharp Betting Analysis Module

This module provides comprehensive sharp action analysis with layered architecture:
- Analytical processing: ML-based historical analysis and feature extraction
- Real-time processing: Live signal processing with strategy validation
- Strategy management: Business rules and thresholds
"""

from .processors.analytical_processor import AnalyticalProcessor
from .processors.sharpaction_processor import SharpActionProcessor
from .processors.opposingmarkets_processor import OpposingMarketsProcessor
from .processors.consensus_processor import ConsensusProcessor
from .processors.underdogvalue_processor import UnderdogValueProcessor
from .processors.bookconflict_processor import BookConflictProcessor
from .processors.publicfade_processor import PublicFadeProcessor
from .processors.lateflip_processor import LateFlipProcessor
from .processors.hybridsharp_processor import HybridSharpProcessor
from .processors.timingbased_processor import TimingBasedProcessor
from .processors.strategy_processor_factory import StrategyProcessorFactory

__all__ = [
    "AnalyticalProcessor", 
    "SharpActionProcessor",
    "OpposingMarketsProcessor",
    "ConsensusProcessor",
    "UnderdogValueProcessor",
    "BookConflictProcessor",
    "PublicFadeProcessor",
    "LateFlipProcessor",
    "HybridSharpProcessor",
    "TimingBasedProcessor",
    "StrategyProcessorFactory",
] 