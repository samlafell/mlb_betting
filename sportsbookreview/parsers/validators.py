"""
Data validators for the SportsbookReview parser.
"""

import logging
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)


class PublicBettingPercentage(BaseModel):
    home_ml: float | None = Field(None, ge=0, le=100)
    away_ml: float | None = Field(None, ge=0, le=100)
    home_spread: float | None = Field(None, ge=0, le=100)
    away_spread: float | None = Field(None, ge=0, le=100)
    over: float | None = Field(None, ge=0, le=100)
    under: float | None = Field(None, ge=0, le=100)


class OddsDataValidator(BaseModel):
    sportsbook: str
    bet_type: str | None = (
        None  # Add bet_type field to preserve it through validation
    )

    # Original field names (from parser)
    moneyline_home: int | None = None
    moneyline_away: int | None = None
    spread_home: float | None = None
    spread_away: float | None = None
    total_line: float | None = None
    total_over: float | None = None
    total_under: float | None = None

    # Transformed field names (from collection orchestrator)
    home_ml: int | None = None
    away_ml: int | None = None
    home_spread: float | None = None
    away_spread: float | None = None
    home_spread_price: int | None = None
    away_spread_price: int | None = None
    over_price: float | None = None
    under_price: float | None = None

    # Additional fields for completeness
    timestamp: str | None = None

    @field_validator("moneyline_home", "moneyline_away", "home_ml", "away_ml")
    @classmethod
    def check_moneyline_odds(cls, v):
        if v is not None and (v > 10000 or v < -10000):
            raise ValueError("Moneyline odds seem unrealistic")
        return v


class GameDataValidator(BaseModel):
    sbr_game_id: str
    game_datetime: datetime
    home_team: str
    away_team: str
    bet_type: str
    source_url: str
    public_betting_percentage: PublicBettingPercentage | None = None
    odds_data: list[OddsDataValidator] = []

    @field_validator("sbr_game_id")
    @classmethod
    def sbr_game_id_non_empty(cls, v):
        """Accept any non-empty ID. Upstream now uses "sbr-YYYY-mm-dd-*"."""
        if not v or not v.strip():
            raise ValueError("sbr_game_id cannot be empty")
        return v

    @field_validator("game_datetime")
    @classmethod
    def game_datetime_must_be_in_past(cls, v):
        if v.year < 2021:
            raise ValueError("Game date is before 2021, which is unexpected")
        return v

    @field_validator("home_team", "away_team")
    @classmethod
    def team_names_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Team name cannot be empty")
        return v

    @staticmethod
    def validate_data(data: dict) -> Optional["GameDataValidator"]:
        try:
            instance = GameDataValidator(**data)
            return instance
        except ValidationError as e:
            logger.warning(f"Validation failed for game data: {e}. Data: {data}")
            return None
