"""
Unified Data Models

Consolidated models from mlb_sharp_betting, sportsbookreview, and action modules.
These models provide a single source of truth for all MLB betting data structures.
"""

from .betting_analysis import (
    BettingAnalysis,
    BettingRecommendation,
    BettingSignalType,
    BettingSplit,
    SharpAction,
    SignalStrength,
)
from .game import GameStatus, GameType, Team, UnifiedGame
from .odds import BookType, LineMovement, MarketType, OddsData, OddsSnapshot
from .sharp_data import (
    ConfidenceLevel,
    SharpConsensus,
    SharpDirection,
    SharpIndicatorType,
    SharpMoney,
    SharpSignal,
)

__all__ = [
    # Game models
    "UnifiedGame",
    "Team",
    "GameStatus",
    "GameType",
    # Odds models
    "OddsData",
    "OddsSnapshot",
    "LineMovement",
    "MarketType",
    "BookType",
    # Betting analysis models
    "BettingAnalysis",
    "SharpAction",
    "BettingSplit",
    "BettingSignalType",
    "SignalStrength",
    "BettingRecommendation",
    # Sharp data models
    "SharpSignal",
    "SharpDirection",
    "ConfidenceLevel",
    "SharpIndicatorType",
    "SharpMoney",
    "SharpConsensus",
]
