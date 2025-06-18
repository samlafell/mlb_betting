"""
Game outcome model for storing final game results and betting outcomes.

This model tracks the actual game results to compare against betting data
and calculate betting performance metrics.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, validator

from .game import Team


class GameOutcome(BaseModel):
    """
    Model representing a completed game's outcome and betting results.
    
    Tracks the final score and key betting outcomes:
    - Who won (home/away)
    - Whether total went over/under
    - Whether home team covered the spread
    """
    
    game_id: str = Field(..., description="MLB Stats API game ID (gamePk)")
    home_team: Team = Field(..., description="Home team")
    away_team: Team = Field(..., description="Away team")
    
    # Game scores
    home_score: int = Field(..., description="Home team final score")
    away_score: int = Field(..., description="Away team final score")
    
    # Betting outcomes
    over: bool = Field(..., description="Whether the total went over")
    home_win: bool = Field(..., description="Whether home team won")
    home_cover_spread: Optional[bool] = Field(None, description="Whether home team covered spread (if available)")
    
    # Additional context
    total_line: Optional[float] = Field(None, description="Total line used for over/under calculation")
    home_spread_line: Optional[float] = Field(None, description="Spread line for home team (positive = home gets points, negative = home gives points)")
    game_date: Optional[datetime] = Field(None, description="Game date/time")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @validator('home_win', always=True)
    def validate_home_win(cls, v, values):
        """Ensure home_win matches the scores."""
        if 'home_score' in values and 'away_score' in values:
            expected = values['home_score'] > values['away_score']
            if v != expected:
                raise ValueError(f"home_win ({v}) doesn't match scores: home {values['home_score']}, away {values['away_score']}")
        return v
    
    @validator('over')
    def validate_over_calculation(cls, v, values):
        """Validate over calculation if total_line is provided."""
        if 'total_line' in values and values['total_line'] is not None:
            if 'home_score' in values and 'away_score' in values:
                total_score = values['home_score'] + values['away_score']
                expected = total_score > values['total_line']
                if v != expected:
                    raise ValueError(f"over ({v}) doesn't match calculation: {total_score} vs {values['total_line']}")
        return v
    
    @validator('home_cover_spread')
    def validate_spread_cover(cls, v, values):
        """Validate spread cover calculation if home_spread_line is provided."""
        if v is not None and 'home_spread_line' in values and values['home_spread_line'] is not None:
            if 'home_score' in values and 'away_score' in values:
                home_spread_result = values['home_score'] + values['home_spread_line']
                expected = home_spread_result > values['away_score']
                if v != expected:
                    raise ValueError(f"home_cover_spread ({v}) doesn't match calculation")
        return v
    
    class Config:
        """Pydantic model configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Team: lambda v: v.value
        }
        
        # Allow updates to updated_at
        allow_population_by_field_name = True 