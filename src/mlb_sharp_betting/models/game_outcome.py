"""
Game outcome model for storing final game results and betting outcomes.

This model tracks the actual game results to compare against betting data
and calculate betting performance metrics.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict, ValidationInfo

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
    
    @field_validator('home_win')
    @classmethod
    def validate_home_win(cls, v: bool, info: ValidationInfo) -> bool:
        """Ensure home_win matches the scores."""
        if info.data and 'home_score' in info.data and 'away_score' in info.data:
            expected = info.data['home_score'] > info.data['away_score']
            if v != expected:
                raise ValueError(f"home_win ({v}) doesn't match scores: home {info.data['home_score']}, away {info.data['away_score']}")
        return v
    
    @field_validator('over')
    @classmethod
    def validate_over_calculation(cls, v: bool, info: ValidationInfo) -> bool:
        """Validate over calculation if total_line is provided."""
        if info.data and 'total_line' in info.data and info.data['total_line'] is not None:
            if 'home_score' in info.data and 'away_score' in info.data:
                total_score = info.data['home_score'] + info.data['away_score']
                expected = total_score > info.data['total_line']
                if v != expected:
                    raise ValueError(f"over ({v}) doesn't match calculation: {total_score} vs {info.data['total_line']}")
        return v
    
    @field_validator('home_cover_spread')
    @classmethod
    def validate_spread_cover(cls, v: Optional[bool], info: ValidationInfo) -> Optional[bool]:
        """Validate spread cover calculation if home_spread_line is provided."""
        if v is not None and info.data and 'home_spread_line' in info.data and info.data['home_spread_line'] is not None:
            if 'home_score' in info.data and 'away_score' in info.data:
                home_spread_result = info.data['home_score'] + info.data['home_spread_line']
                expected = home_spread_result > info.data['away_score']
                if v != expected:
                    raise ValueError(f"home_cover_spread ({v}) doesn't match calculation")
        return v
    
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat(),
            Team: lambda v: v.value
        },
        # Allow field validation by name
        populate_by_name=True
    ) 