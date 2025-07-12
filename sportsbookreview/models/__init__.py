"""
SportsbookReview data models.

This module provides Pydantic models for type-safe data handling across
the SportsbookReview integration system.
"""

# Base models and enums
from .base import (
    SportsbookReviewBaseModel,
    SportsBookReviewTimestampedModel,
    SportsbookReviewValidatedModel,
    BetType,
    SportsbookName,
    OddsFormat,
    DataQuality
)

# Game models with MLB Stats API integration
from .game import (
    EnhancedGame,
    VenueInfo,
    WeatherData,
    WeatherCondition,
    PitcherInfo,
    PitcherMatchup,
    GameContext,
    GameType
)

# Odds data models
from .odds_data import (
    OddsData,
    OddsSnapshot,
    LineMovementData,
    MarketSide,
    OddsMovement,
    LineStatus
)

# Sportsbook mapping models
from .sportsbook_mapping import (
    SportsbookMapping,
    SportsbookCapabilities,
    MarketMapping,
    MarketAvailability,
    OddsDisplayPreference
)

__all__ = [
    # Base models and enums
    "SportsbookReviewBaseModel",
    "SportsBookReviewTimestampedModel",
    "SportsbookReviewValidatedModel",
    "BetType",
    "SportsbookName",
    "OddsFormat",
    "DataQuality",
    
    # Game models
    "EnhancedGame",
    "VenueInfo",
    "WeatherData",
    "WeatherCondition",
    "PitcherInfo",
    "PitcherMatchup",
    "GameContext",
    "GameType",
    
    # Odds data models
    "OddsData",
    "OddsSnapshot",
    "LineMovementData",
    "MarketSide",
    "OddsMovement",
    "LineStatus",
    
    # Sportsbook mapping models
    "SportsbookMapping",
    "SportsbookCapabilities",
    "MarketMapping",
    "MarketAvailability",
    "OddsDisplayPreference",
] 