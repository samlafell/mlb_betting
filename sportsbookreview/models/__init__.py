"""
SportsbookReview data models.

This module provides Pydantic models for type-safe data handling across
the SportsbookReview integration system.
"""

# Base models and enums
from .base import (
    BetType,
    DataQuality,
    OddsFormat,
    SportsbookName,
    SportsbookReviewBaseModel,
    SportsBookReviewTimestampedModel,
    SportsbookReviewValidatedModel,
)

# Game models with MLB Stats API integration
from .game import (
    EnhancedGame,
    GameContext,
    GameType,
    PitcherInfo,
    PitcherMatchup,
    VenueInfo,
    WeatherCondition,
    WeatherData,
)

# Odds data models
from .odds_data import (
    LineMovementData,
    LineStatus,
    MarketSide,
    OddsData,
    OddsMovement,
    OddsSnapshot,
)

# Sportsbook mapping models
from .sportsbook_mapping import (
    MarketAvailability,
    MarketMapping,
    OddsDisplayPreference,
    SportsbookCapabilities,
    SportsbookMapping,
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
