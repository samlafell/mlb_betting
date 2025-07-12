"""
Data validators for the SportsbookReview parser.
"""
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, Dict, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PublicBettingPercentage(BaseModel):
    home_ml: Optional[float] = Field(None, ge=0, le=100)
    away_ml: Optional[float] = Field(None, ge=0, le=100)
    home_spread: Optional[float] = Field(None, ge=0, le=100)
    away_spread: Optional[float] = Field(None, ge=0, le=100)
    over: Optional[float] = Field(None, ge=0, le=100)
    under: Optional[float] = Field(None, ge=0, le=100)

class OddsDataValidator(BaseModel):
    sportsbook: str
    bet_type: Optional[str] = None  # Add bet_type field to preserve it through validation
    
    # Original field names (from parser)
    moneyline_home: Optional[int] = None
    moneyline_away: Optional[int] = None
    spread_home: Optional[float] = None
    spread_away: Optional[float] = None
    total_line: Optional[float] = None
    total_over: Optional[float] = None
    total_under: Optional[float] = None
    
    # Transformed field names (from collection orchestrator)
    home_ml: Optional[int] = None
    away_ml: Optional[int] = None
    home_spread: Optional[float] = None
    away_spread: Optional[float] = None
    home_spread_price: Optional[int] = None
    away_spread_price: Optional[int] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    
    # Additional fields for completeness
    timestamp: Optional[str] = None
    
    @field_validator('moneyline_home', 'moneyline_away', 'home_ml', 'away_ml')
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
    public_betting_percentage: Optional[PublicBettingPercentage] = None
    odds_data: List[OddsDataValidator] = []

    @field_validator('sbr_game_id')
    @classmethod
    def sbr_game_id_non_empty(cls, v):
        """Accept any non-empty ID. Upstream now uses "sbr-YYYY-mm-dd-*"."""
        if not v or not v.strip():
            raise ValueError('sbr_game_id cannot be empty')
        return v

    @field_validator('game_datetime')
    @classmethod
    def game_datetime_must_be_in_past(cls, v):
        if v.year < 2021:
            raise ValueError('Game date is before 2021, which is unexpected')
        return v

    @field_validator('home_team', 'away_team')
    @classmethod
    def team_names_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Team name cannot be empty')
        return v
        
    @staticmethod
    def validate_data(data: dict) -> Optional['GameDataValidator']:
        try:
            instance = GameDataValidator(**data)
            return instance
        except ValidationError as e:
            logger.warning(f"Validation failed for game data: {e}. Data: {data}")
            return None 