"""
OddsData model for SportsbookReview system.

This module provides models for representing betting odds data from
SportsbookReview.com with comprehensive validation and temporal tracking.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from decimal import Decimal

from pydantic import Field, field_validator, computed_field

from .base import (
    SportsbookReviewBaseModel,
    SportsBookReviewTimestampedModel,
    SportsbookReviewValidatedModel,
    BetType,
    SportsbookName,
    OddsFormat,
    DataQuality
)


class MarketSide(str, Enum):
    """Sides/options for betting markets."""
    
    # Moneyline
    HOME = "home"
    AWAY = "away"
    
    # Spread
    HOME_SPREAD = "home_spread"
    AWAY_SPREAD = "away_spread"
    
    # Total
    OVER = "over"
    UNDER = "under"
    
    # Team Total
    HOME_OVER = "home_over"
    HOME_UNDER = "home_under"
    AWAY_OVER = "away_over"
    AWAY_UNDER = "away_under"


class OddsMovement(str, Enum):
    """Direction of odds movement."""
    
    UP = "up"           # Odds moved up (longer odds, less likely)
    DOWN = "down"       # Odds moved down (shorter odds, more likely)
    STABLE = "stable"   # No significant movement
    VOLATILE = "volatile"  # High volatility, multiple direction changes


class LineStatus(str, Enum):
    """Status of betting line."""
    
    ACTIVE = "active"           # Line is available for betting
    SUSPENDED = "suspended"     # Line temporarily unavailable
    CLOSED = "closed"          # Line permanently closed
    NOT_OFFERED = "not_offered" # Sportsbook doesn't offer this line


class OddsSnapshot(SportsbookReviewBaseModel):
    """
    Model representing a single odds value at a specific time.
    """
    
    # Odds value in specified format
    odds_value: Optional[Union[str, int, float]] = Field(
        default=None,
        description="Odds value in sportsbook format"
    )
    
    # American odds equivalent (for standardization)
    american_odds: Optional[int] = Field(
        default=None,
        description="Odds in American format (+/-XXX)"
    )
    
    # Decimal odds equivalent
    decimal_odds: Optional[float] = Field(
        default=None,
        description="Odds in decimal format",
        ge=1.0
    )
    
    # Implied probability
    implied_probability: Optional[float] = Field(
        default=None,
        description="Implied probability (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    
    # Line status
    status: LineStatus = Field(
        default=LineStatus.ACTIVE,
        description="Status of the betting line"
    )
    
    # Timestamp when odds were observed
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When these odds were observed"
    )
    
    @field_validator("american_odds")
    @classmethod
    def validate_american_odds(cls, v: Optional[int]) -> Optional[int]:
        """Validate American odds format."""
        if v is None:
            return v
        
        # American odds should not be between -99 and +99 (except -100/+100)
        if -99 < v < 99 and v != 0:
            raise ValueError("American odds between -99 and +99 are invalid")
        
        return v
    
    @field_validator("decimal_odds")
    @classmethod
    def validate_decimal_odds(cls, v: Optional[float]) -> Optional[float]:
        """Validate decimal odds format."""
        if v is None:
            return v
        
        if v < 1.0:
            raise ValueError("Decimal odds must be 1.0 or greater")
        
        return v
    
    def calculate_implied_probability(self) -> float:
        """
        Calculate implied probability from odds.
        
        Returns:
            Implied probability as decimal (0.0-1.0)
        """
        if self.american_odds is not None:
            if self.american_odds > 0:
                return 100 / (self.american_odds + 100)
            else:
                return abs(self.american_odds) / (abs(self.american_odds) + 100)
        
        elif self.decimal_odds is not None:
            return 1.0 / self.decimal_odds
        
        return 0.0
    
    def convert_to_american(self) -> Optional[int]:
        """
        Convert odds to American format.
        
        Returns:
            American odds or None if conversion not possible
        """
        if self.american_odds is not None:
            return self.american_odds
        
        if self.decimal_odds is not None:
            if self.decimal_odds >= 2.0:
                return int((self.decimal_odds - 1) * 100)
            else:
                return int(-100 / (self.decimal_odds - 1))
        
        return None


class LineMovementData(SportsbookReviewBaseModel):
    """
    Model representing line movement data for a specific market.
    """
    
    # Line value (spread, total, etc.)
    line_value: Optional[float] = Field(
        default=None,
        description="Line value (e.g., -1.5 for spread, 9.5 for total)"
    )
    
    # Odds snapshots over time
    odds_history: List[OddsSnapshot] = Field(
        default_factory=list,
        description="Historical odds values"
    )
    
    # Movement analysis
    opening_odds: Optional[OddsSnapshot] = Field(
        default=None,
        description="Opening odds"
    )
    
    current_odds: Optional[OddsSnapshot] = Field(
        default=None,
        description="Most recent odds"
    )
    
    movement_direction: OddsMovement = Field(
        default=OddsMovement.STABLE,
        description="Overall movement direction"
    )
    
    total_movements: int = Field(
        default=0,
        description="Total number of line movements",
        ge=0
    )
    
    volatility_score: Optional[float] = Field(
        default=None,
        description="Volatility score (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    
    @computed_field
    @property
    def odds_range(self) -> Optional[Dict[str, Union[int, float]]]:
        """Get the range of odds movement."""
        if not self.odds_history:
            return None
        
        american_odds = [snap.american_odds for snap in self.odds_history if snap.american_odds is not None]
        
        if not american_odds:
            return None
        
        return {
            "min": min(american_odds),
            "max": max(american_odds),
            "range": max(american_odds) - min(american_odds)
        }
    
    def add_odds_snapshot(self, snapshot: OddsSnapshot) -> None:
        """
        Add a new odds snapshot and update movement data.
        
        Args:
            snapshot: New odds snapshot to add
        """
        self.odds_history.append(snapshot)
        self.current_odds = snapshot
        
        if self.opening_odds is None:
            self.opening_odds = snapshot
        
        # Update movement analysis
        self._analyze_movement()
    
    def _analyze_movement(self) -> None:
        """Analyze odds movement and update movement metrics."""
        if len(self.odds_history) < 2:
            return
        
        # Count movements
        movements = 0
        previous_odds = None
        
        for snapshot in self.odds_history:
            if previous_odds is not None and snapshot.american_odds is not None:
                if abs(snapshot.american_odds - previous_odds) >= 5:  # Minimum movement threshold
                    movements += 1
            previous_odds = snapshot.american_odds
        
        self.total_movements = movements
        
        # Determine overall direction
        if self.opening_odds and self.current_odds:
            if (self.opening_odds.american_odds is not None and 
                self.current_odds.american_odds is not None):
                
                diff = self.current_odds.american_odds - self.opening_odds.american_odds
                
                if abs(diff) < 5:
                    self.movement_direction = OddsMovement.STABLE
                elif diff > 0:
                    self.movement_direction = OddsMovement.UP
                else:
                    self.movement_direction = OddsMovement.DOWN
                
                # Check for volatility
                if movements > len(self.odds_history) * 0.3:  # 30% of snapshots had movements
                    self.movement_direction = OddsMovement.VOLATILE
        
        # Calculate volatility score
        if len(self.odds_history) > 1:
            odds_values = [snap.american_odds for snap in self.odds_history if snap.american_odds is not None]
            if len(odds_values) > 1:
                # Simple volatility: standard deviation / mean
                mean_odds = sum(odds_values) / len(odds_values)
                variance = sum((x - mean_odds) ** 2 for x in odds_values) / len(odds_values)
                std_dev = variance ** 0.5
                
                if mean_odds != 0:
                    self.volatility_score = min(std_dev / abs(mean_odds), 1.0)


class OddsData(SportsBookReviewTimestampedModel, SportsbookReviewValidatedModel):
    """
    Comprehensive model for odds data from SportsbookReview.
    
    Tracks all betting markets for a game across multiple sportsbooks
    with temporal tracking and movement analysis.
    """
    
    # Core identification
    game_id: str = Field(
        ...,
        description="Associated game identifier",
        min_length=1,
        max_length=100
    )
    
    # Game information (denormalized for easier querying)
    home_team: str = Field(
        ...,
        description="Home team abbreviation",
        min_length=2,
        max_length=5
    )
    
    away_team: str = Field(
        ...,
        description="Away team abbreviation",
        min_length=2,
        max_length=5
    )
    
    game_datetime: datetime = Field(
        ...,
        description="Game datetime for easier identification"
    )
    
    sportsbook: SportsbookName = Field(
        ...,
        description="Sportsbook name"
    )
    
    bet_type: BetType = Field(
        ...,
        description="Type of bet"
    )
    
    market_side: MarketSide = Field(
        ...,
        description="Side/option being bet"
    )
    
    # Market data
    line_movement: LineMovementData = Field(
        ...,
        description="Line movement data and odds history"
    )
    
    # Metadata
    odds_format: OddsFormat = Field(
        default=OddsFormat.AMERICAN,
        description="Primary odds format used"
    )
    
    market_open_time: Optional[datetime] = Field(
        default=None,
        description="When market opened for betting"
    )
    
    market_close_time: Optional[datetime] = Field(
        default=None,
        description="When market closed"
    )
    
    last_update_time: datetime = Field(
        default_factory=datetime.now,
        description="Last time odds were updated"
    )
    
    # Data quality and validation
    data_quality: DataQuality = Field(
        default=DataQuality.MEDIUM,
        description="Data quality assessment"
    )
    
    validation_errors: List[str] = Field(
        default_factory=list,
        description="List of validation errors encountered"
    )
    
    source_confidence: Optional[float] = Field(
        default=None,
        description="Confidence in data source accuracy (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    
    # Juice/vig analysis
    juice_percentage: Optional[float] = Field(
        default=None,
        description="Estimated juice/vig percentage",
        ge=0.0,
        le=100.0
    )
    
    @field_validator("validation_errors")
    @classmethod
    def validate_errors_list(cls, v: List[str]) -> List[str]:
        """Ensure validation errors are strings."""
        return [str(error) for error in v]
    
    @field_validator("home_team", "away_team")
    @classmethod
    def validate_team_abbreviations(cls, v: str) -> str:
        """Validate team abbreviations."""
        if not v or len(v.strip()) < 2:
            raise ValueError("Team abbreviation must be at least 2 characters")
        return v.strip().upper()
    
    @computed_field
    @property
    def current_odds(self) -> Optional[OddsSnapshot]:
        """Get current odds snapshot."""
        return self.line_movement.current_odds
    
    @computed_field
    @property
    def has_movement(self) -> bool:
        """Check if odds have moved."""
        return self.line_movement.total_movements > 0
    
    @computed_field
    @property
    def is_active(self) -> bool:
        """Check if market is currently active."""
        current = self.current_odds
        return current is not None and current.status == LineStatus.ACTIVE
    
    @computed_field
    @property
    def matchup_description(self) -> str:
        """Get a human-readable matchup description."""
        return f"{self.away_team} @ {self.home_team}"
    
    def add_odds_update(self, 
                       odds_value: Union[str, int, float],
                       timestamp: Optional[datetime] = None,
                       status: LineStatus = LineStatus.ACTIVE) -> None:
        """
        Add a new odds update.
        
        Args:
            odds_value: New odds value
            timestamp: When odds were observed (defaults to now)
            status: Line status
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Create odds snapshot
        snapshot = OddsSnapshot(
            odds_value=odds_value,
            timestamp=timestamp,
            status=status
        )
        
        # Convert to standardized formats
        if isinstance(odds_value, str):
            # Parse string odds
            try:
                if odds_value.startswith(('+', '-')):
                    snapshot.american_odds = int(odds_value)
                else:
                    snapshot.decimal_odds = float(odds_value)
            except ValueError:
                self.validation_errors.append(f"Unable to parse odds value: {odds_value}")
                return
        
        elif isinstance(odds_value, int):
            snapshot.american_odds = odds_value
        
        elif isinstance(odds_value, float):
            if odds_value >= 1.0:
                snapshot.decimal_odds = odds_value
            else:
                self.validation_errors.append(f"Invalid decimal odds: {odds_value}")
                return
        
        # Convert between formats
        if snapshot.american_odds is not None:
            if snapshot.american_odds > 0:
                snapshot.decimal_odds = (snapshot.american_odds / 100) + 1
            else:
                snapshot.decimal_odds = (100 / abs(snapshot.american_odds)) + 1
        
        elif snapshot.decimal_odds is not None:
            snapshot.american_odds = snapshot.convert_to_american()
        
        # Calculate implied probability
        snapshot.implied_probability = snapshot.calculate_implied_probability()
        
        # Add to line movement data
        self.line_movement.add_odds_snapshot(snapshot)
        self.last_update_time = timestamp
        
        # Update data quality based on successful updates
        if len(self.line_movement.odds_history) >= 5:
            self.data_quality = DataQuality.HIGH
        elif len(self.line_movement.odds_history) >= 2:
            self.data_quality = DataQuality.MEDIUM
        else:
            self.data_quality = DataQuality.LOW
    
    def validate_odds_consistency(self) -> List[str]:
        """
        Validate odds data for consistency and flag potential issues.
        
        Returns:
            List of validation issues found
        """
        issues = []
        
        # Check for reasonable odds ranges
        if self.current_odds and self.current_odds.american_odds is not None:
            odds = self.current_odds.american_odds
            
            # Flag extremely unlikely odds
            if abs(odds) > 10000:
                issues.append(f"Extremely high odds detected: {odds}")
            
            # Flag odds that don't make sense for the bet type
            if self.bet_type == BetType.SPREAD and abs(odds) < 105:
                issues.append(f"Unusually low spread odds: {odds}")
        
        # Check for impossible movements
        if len(self.line_movement.odds_history) > 1:
            for i in range(1, len(self.line_movement.odds_history)):
                prev_odds = self.line_movement.odds_history[i-1].american_odds
                curr_odds = self.line_movement.odds_history[i].american_odds
                
                if prev_odds is not None and curr_odds is not None:
                    change = abs(curr_odds - prev_odds)
                    if change > 1000:  # Massive odds swing
                        issues.append(f"Suspicious odds movement: {prev_odds} to {curr_odds}")
        
        return issues
    
    class Config:
        json_schema_extra = {
            "example": {
                "game_id": "sbr-2025-04-15-LAD-SF-1",
                "home_team": "LAD",
                "away_team": "SF",
                "game_datetime": "2025-04-15T19:10:00Z",
                "sportsbook": "draftkings",
                "bet_type": "spread",
                "market_side": "home_spread",
                "line_movement": {
                    "line_value": -1.5,
                    "opening_odds": {
                        "american_odds": -110,
                        "decimal_odds": 1.91,
                        "implied_probability": 0.524,
                        "timestamp": "2025-04-15T10:00:00Z"
                    },
                    "current_odds": {
                        "american_odds": -115,
                        "decimal_odds": 1.87,
                        "implied_probability": 0.535,
                        "timestamp": "2025-04-15T18:00:00Z"
                    },
                    "movement_direction": "down",
                    "total_movements": 3
                },
                "data_quality": "high",
                "source_confidence": 0.95
            }
        } 