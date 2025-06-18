"""
Betting splits models for the MLB Sharp Betting system.

This module provides models for representing betting split data
from various sportsbooks and sources.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union
import json

from pydantic import Field, validator

from mlb_sharp_betting.models.base import IdentifiedModel, ValidatedModel
from mlb_sharp_betting.models.game import Team


class SplitType(str, Enum):
    """Enumeration of betting split types."""
    
    SPREAD = "spread"
    TOTAL = "total"
    MONEYLINE = "moneyline"


class BookType(str, Enum):
    """Enumeration of supported sportsbooks."""
    
    BETMGM = "betmgm"
    BET365 = "bet365"
    FANATICS = "fanatics"
    DRAFTKINGS = "draftkings"
    CAESARS = "caesars"
    FANDUEL = "fanduel"
    CIRCA = "circa"
    WESTGATE = "westgate"
    STATION = "station"
    SOUTH_POINT = "south_point"
    
    @classmethod
    def get_display_name(cls, book: str) -> str:
        """
        Get display name for a sportsbook.
        
        Args:
            book: Book enum value
            
        Returns:
            Display name for the sportsbook
        """
        display_names = {
            "betmgm": "BetMGM",
            "bet365": "Bet365",
            "fanatics": "Fanatics",
            "draftkings": "DraftKings",
            "caesars": "Caesars",
            "fanduel": "FanDuel",
            "circa": "Circa",
            "westgate": "Westgate",
            "station": "Station",
            "south_point": "South Point",
        }
        
        return display_names.get(book, book.title())


class DataSource(str, Enum):
    """Enumeration of data sources."""
    
    SBD = "SBD"      # SportsBettingDime
    VSIN = "VSIN"    # VSIN


class BettingSplit(IdentifiedModel, ValidatedModel):
    """
    Model representing betting split data for a specific game and split type.
    
    Contains percentage and count information for both sides of a bet
    along with metadata about the source and timing of the data.
    """
    
    # Game identification
    game_id: str = Field(
        ...,
        description="Unique identifier for the game",
        min_length=1,
        max_length=100
    )
    
    home_team: Team = Field(
        ...,
        description="Home team abbreviation"
    )
    
    away_team: Team = Field(
        ...,
        description="Away team abbreviation"
    )
    
    game_datetime: datetime = Field(
        ...,
        description="Scheduled game start time"
    )
    
    # Split information
    split_type: SplitType = Field(
        ...,
        description="Type of betting split"
    )
    
    split_value: Optional[Union[float, str]] = Field(
        default=None,
        description="The line value (spread, total, etc.) or JSON string for complex values like moneyline"
    )
    
    # Data source and timing
    source: DataSource = Field(
        ...,
        description="Source of the split data"
    )
    
    book: Optional[BookType] = Field(
        default=None,
        description="Sportsbook for this split data (None for aggregated data)"
    )
    
    last_updated: datetime = Field(
        ...,
        description="When this split data was last updated"
    )
    
    # Home/Over betting information
    home_or_over_bets: Optional[int] = Field(
        default=None,
        description="Number of bets on home team or over",
        ge=0
    )
    
    home_or_over_bets_percentage: Optional[float] = Field(
        default=None,
        description="Percentage of bets on home team or over",
        ge=0.0,
        le=100.0
    )
    
    home_or_over_stake_percentage: Optional[float] = Field(
        default=None,
        description="Percentage of stake on home team or over",
        ge=0.0,
        le=100.0
    )
    
    # Away/Under betting information
    away_or_under_bets: Optional[int] = Field(
        default=None,
        description="Number of bets on away team or under",
        ge=0
    )
    
    away_or_under_bets_percentage: Optional[float] = Field(
        default=None,
        description="Percentage of bets on away team or under",
        ge=0.0,
        le=100.0
    )
    
    away_or_under_stake_percentage: Optional[float] = Field(
        default=None,
        description="Percentage of stake on away team or under",
        ge=0.0,
        le=100.0
    )
    
    # Analysis results
    sharp_action: Optional[str] = Field(
        default=None,
        description="Detected sharp action direction (if any)",
        max_length=50
    )
    
    # Game outcome (populated after completion)
    outcome: Optional[str] = Field(
        default=None,
        description="Outcome of the bet (win/loss/push)",
        pattern="^(win|loss|push)$"
    )
    
    @validator("home_or_over_bets_percentage", "away_or_under_bets_percentage")
    def validate_bet_percentages(cls, v: Optional[float], values: Dict[str, Any]) -> Optional[float]:
        """
        Validate that bet percentages add up to approximately 100%.
        
        Args:
            v: Current percentage value
            values: All field values
            
        Returns:
            Validated percentage value
        """
        if v is None:
            return None
        
        # Get the other percentage
        field_names = ["home_or_over_bets_percentage", "away_or_under_bets_percentage"]
        current_field = None
        other_field = None
        
        # Determine which field we're validating
        for field in field_names:
            if field in values and values[field] is not None:
                if current_field is None:
                    current_field = field
                    other_field = field_names[1] if field == field_names[0] else field_names[0]
                    break
        
        # If we have both percentages, check they add up to ~100%
        if other_field and other_field in values and values[other_field] is not None:
            total = v + values[other_field]
            if not (99.0 <= total <= 101.0):  # Allow small rounding errors
                raise ValueError(f"Bet percentages must add up to 100%, got {total}%")
        
        return v
    
    @validator("home_or_over_stake_percentage", "away_or_under_stake_percentage")
    def validate_stake_percentages(cls, v: Optional[float], values: Dict[str, Any]) -> Optional[float]:
        """
        Validate that stake percentages add up to approximately 100%.
        
        Args:
            v: Current percentage value
            values: All field values
            
        Returns:
            Validated percentage value
        """
        if v is None:
            return None
        
        # Get the other percentage
        field_names = ["home_or_over_stake_percentage", "away_or_under_stake_percentage"]
        current_field = None
        other_field = None
        
        # Determine which field we're validating
        for field in field_names:
            if field in values and values[field] is not None:
                if current_field is None:
                    current_field = field
                    other_field = field_names[1] if field == field_names[0] else field_names[0]
                    break
        
        # If we have both percentages, check they add up to ~100%
        if other_field and other_field in values and values[other_field] is not None:
            total = v + values[other_field]
            if not (99.0 <= total <= 101.0):  # Allow small rounding errors
                raise ValueError(f"Stake percentages must add up to 100%, got {total}%")
        
        return v
    
    @validator("split_value")
    def validate_split_value(cls, v: Optional[Union[float, str]], values: Dict[str, Any]) -> Optional[Union[float, str]]:
        """
        Validate split value based on split type.
        
        Args:
            v: Split value (float for spread/total, JSON string for moneyline)
            values: All field values
            
        Returns:
            Validated split value
        """
        if v is None:
            return None
        
        split_type = values.get("split_type")
        
        # Handle JSON strings for moneyline
        if isinstance(v, str):
            if split_type == SplitType.MONEYLINE:
                try:
                    # Validate that it's valid JSON
                    parsed = json.loads(v)
                    if isinstance(parsed, dict) and 'home' in parsed and 'away' in parsed:
                        return v
                    else:
                        raise ValueError(f"Moneyline JSON must contain 'home' and 'away' keys")
                except json.JSONDecodeError:
                    raise ValueError(f"Invalid JSON for moneyline split_value: {v}")
            else:
                # For non-moneyline, try to convert string to float
                try:
                    v = float(v)
                except ValueError:
                    raise ValueError(f"Non-moneyline split_value must be numeric: {v}")
        
        # Validate numeric values
        if isinstance(v, (int, float)):
            if split_type == SplitType.SPREAD:
                # Spreads are typically between -30 and +30
                if not (-30.0 <= v <= 30.0):
                    raise ValueError(f"Spread value {v} is outside expected range (-30 to +30)")
            
            elif split_type == SplitType.TOTAL:
                # Totals are typically between 6 and 20 for MLB
                if not (6.0 <= v <= 20.0):
                    raise ValueError(f"Total value {v} is outside expected range (6 to 20)")
        
        return v
    
    @property
    def bet_percentage_difference(self) -> Optional[float]:
        """
        Calculate the difference between home/over and away/under bet percentages.
        
        Returns:
            Percentage difference (positive means more bets on home/over)
        """
        if (self.home_or_over_bets_percentage is not None and 
            self.away_or_under_bets_percentage is not None):
            return self.home_or_over_bets_percentage - self.away_or_under_bets_percentage
        return None
    
    @property
    def stake_percentage_difference(self) -> Optional[float]:
        """
        Calculate the difference between home/over and away/under stake percentages.
        
        Returns:
            Percentage difference (positive means more stake on home/over)
        """
        if (self.home_or_over_stake_percentage is not None and 
            self.away_or_under_stake_percentage is not None):
            return self.home_or_over_stake_percentage - self.away_or_under_stake_percentage
        return None
    
    @property
    def is_reverse_line_movement(self) -> bool:
        """
        Detect potential reverse line movement (more bets on one side, more money on other).
        
        Returns:
            True if there's potential reverse line movement
        """
        bet_diff = self.bet_percentage_difference
        stake_diff = self.stake_percentage_difference
        
        if bet_diff is None or stake_diff is None:
            return False
        
        # Check if bet and stake percentages are moving in opposite directions
        # with a meaningful difference (at least 5%)
        return (
            (bet_diff > 5 and stake_diff < -5) or 
            (bet_diff < -5 and stake_diff > 5)
        )
    
    @property
    def display_split_description(self) -> str:
        """
        Get a human-readable description of the split.
        
        Returns:
            Description string for the split
        """
        split_desc = self.split_type.value.title()
        
        if self.split_value is not None:
            if self.split_type == SplitType.SPREAD:
                split_desc += f" ({self.split_value:+.1f})"
            elif self.split_type == SplitType.TOTAL:
                split_desc += f" (O/U {self.split_value:.1f})"
        
        return split_desc
    
    def get_side_description(self, home_or_over: bool) -> str:
        """
        Get description for a specific side of the bet.
        
        Args:
            home_or_over: True for home/over side, False for away/under
            
        Returns:
            Description of the bet side
        """
        if self.split_type == SplitType.SPREAD:
            if home_or_over:
                return f"{self.home_team.value} {self.split_value:+.1f}" if self.split_value else f"{self.home_team.value}"
            else:
                return f"{self.away_team.value} {-self.split_value:+.1f}" if self.split_value else f"{self.away_team.value}"
        
        elif self.split_type == SplitType.TOTAL:
            return "Over" if home_or_over else "Under"
        
        elif self.split_type == SplitType.MONEYLINE:
            return self.home_team.value if home_or_over else self.away_team.value
        
        return "Home/Over" if home_or_over else "Away/Under"
    
    class Config:
        # Allow population by field name to handle created_at from parent
        populate_by_name = True
        
        json_schema_extra = {
            "example": {
                "game_id": "2024-04-15-LAD-SF-1",
                "home_team": "SF",
                "away_team": "LAD",
                "game_datetime": "2024-04-15T22:45:00Z",
                "split_type": "spread",
                "split_value": -1.5,
                "source": "SBD",
                "book": "draftkings",
                "last_updated": "2024-04-15T20:30:00Z",
                "home_or_over_bets": 1250,
                "home_or_over_bets_percentage": 35.2,
                "home_or_over_stake_percentage": 45.8,
                "away_or_under_bets": 2300,
                "away_or_under_bets_percentage": 64.8,
                "away_or_under_stake_percentage": 54.2,
                "sharp_action": "home",
                "outcome": None
            }
        } 