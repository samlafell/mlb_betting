"""
Sharp action models for the MLB Sharp Betting system.

This module provides models for representing detected sharp betting
actions and related analysis results.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from mlb_sharp_betting.models.base import IdentifiedModel, ValidatedModel
from mlb_sharp_betting.models.game import Team
from mlb_sharp_betting.models.splits import SplitType, BookType, DataSource


class SharpSignalType(str, Enum):
    """Types of sharp betting signals."""
    
    REVERSE_LINE_MOVEMENT = "reverse_line_movement"
    STEAM_MOVE = "steam_move"
    SHARP_MONEY = "sharp_money"
    CONSENSUS_FADE = "consensus_fade"
    LINE_SHOPPING = "line_shopping"
    LATE_MONEY = "late_money"


class ConfidenceLevel(str, Enum):
    """Confidence levels for sharp action detection."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


class SharpDirection(str, Enum):
    """Direction of sharp action."""
    
    HOME = "home"
    AWAY = "away"
    OVER = "over"
    UNDER = "under"


class SharpSignal(IdentifiedModel, ValidatedModel):
    """
    Model representing a sharp betting signal.
    
    Contains information about a detected pattern that suggests
    sharp money involvement in a specific bet.
    """
    
    # Signal identification
    signal_type: SharpSignalType = Field(
        ...,
        description="Type of sharp signal detected"
    )
    
    confidence: ConfidenceLevel = Field(
        ...,
        description="Confidence level of the signal"
    )
    
    direction: SharpDirection = Field(
        ...,
        description="Direction of the sharp action"
    )
    
    # Game and bet information
    game_id: str = Field(
        ...,
        description="Unique identifier for the game",
        min_length=1,
        max_length=100
    )
    
    split_type: SplitType = Field(
        ...,
        description="Type of betting split"
    )
    
    split_value: Optional[float] = Field(
        default=None,
        description="The line value when signal was detected"
    )
    
    # Signal metrics
    bet_percentage_difference: Optional[float] = Field(
        default=None,
        description="Difference between bet percentages",
        ge=-100.0,
        le=100.0
    )
    
    stake_percentage_difference: Optional[float] = Field(
        default=None,
        description="Difference between stake percentages",
        ge=-100.0,
        le=100.0
    )
    
    # Source information
    detected_at: datetime = Field(
        ...,
        description="When the signal was detected"
    )
    
    source: DataSource = Field(
        ...,
        description="Source of the data that triggered the signal"
    )
    
    books_involved: List[BookType] = Field(
        default_factory=list,
        description="Sportsbooks involved in the signal"
    )
    
    # Analysis details
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the signal",
        max_length=500
    )
    
    supporting_data: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional data supporting the signal"
    )
    
    @property
    def strength_score(self) -> float:
        """
        Calculate a numeric strength score for the signal.
        
        Returns:
            Score from 0.0 to 1.0 indicating signal strength
        """
        base_scores = {
            ConfidenceLevel.LOW: 0.25,
            ConfidenceLevel.MEDIUM: 0.50,
            ConfidenceLevel.HIGH: 0.75,
            ConfidenceLevel.VERY_HIGH: 0.90,
        }
        
        base_score = base_scores[self.confidence]
        
        # Adjust based on signal type importance
        signal_multipliers = {
            SharpSignalType.REVERSE_LINE_MOVEMENT: 1.0,
            SharpSignalType.STEAM_MOVE: 0.9,
            SharpSignalType.SHARP_MONEY: 0.95,
            SharpSignalType.CONSENSUS_FADE: 0.8,
            SharpSignalType.LINE_SHOPPING: 0.7,
            SharpSignalType.LATE_MONEY: 0.85,
        }
        
        multiplier = signal_multipliers.get(self.signal_type, 1.0)
        
        # Adjust based on percentage differences
        if self.stake_percentage_difference is not None:
            diff = abs(self.stake_percentage_difference)
            if diff > 20:
                multiplier += 0.1
            elif diff > 10:
                multiplier += 0.05
        
        return min(base_score * multiplier, 1.0)
    
    @property
    def is_high_confidence(self) -> bool:
        """Check if this is a high confidence signal."""
        return self.confidence in [ConfidenceLevel.HIGH, ConfidenceLevel.VERY_HIGH]
    
    def generate_description(self) -> str:
        """
        Generate a human-readable description of the signal.
        
        Returns:
            Formatted description string
        """
        direction_map = {
            SharpDirection.HOME: "home team",
            SharpDirection.AWAY: "away team",
            SharpDirection.OVER: "over",
            SharpDirection.UNDER: "under",
        }
        
        direction_desc = direction_map.get(self.direction, self.direction.value)
        split_desc = self.split_type.value
        
        if self.signal_type == SharpSignalType.REVERSE_LINE_MOVEMENT:
            return f"Reverse line movement detected on {split_desc} favoring {direction_desc}"
        
        elif self.signal_type == SharpSignalType.STEAM_MOVE:
            return f"Steam move on {split_desc} toward {direction_desc}"
        
        elif self.signal_type == SharpSignalType.SHARP_MONEY:
            return f"Sharp money detected on {split_desc} backing {direction_desc}"
        
        elif self.signal_type == SharpSignalType.CONSENSUS_FADE:
            return f"Sharp action fading public consensus on {split_desc}, backing {direction_desc}"
        
        elif self.signal_type == SharpSignalType.LINE_SHOPPING:
            return f"Line shopping activity detected on {split_desc} for {direction_desc}"
        
        elif self.signal_type == SharpSignalType.LATE_MONEY:
            return f"Late money coming in on {split_desc} for {direction_desc}"
        
        return f"{self.signal_type.value.replace('_', ' ').title()} on {split_desc} for {direction_desc}"
    
    class Config:
        json_schema_extra = {
            "example": {
                "signal_type": "reverse_line_movement",
                "confidence": "high",
                "direction": "home",
                "game_id": "2024-04-15-LAD-SF-1",
                "split_type": "spread",
                "split_value": -1.5,
                "bet_percentage_difference": -15.8,
                "stake_percentage_difference": 8.4,
                "detected_at": "2024-04-15T20:30:00Z",
                "source": "SBD",
                "books_involved": ["draftkings", "fanduel"],
                "description": "Reverse line movement detected on spread favoring home team",
                "supporting_data": {
                    "public_bet_percentage": 35.2,
                    "sharp_stake_percentage": 54.2,
                    "line_movement": "+0.5"
                }
            }
        }


class SharpAction(IdentifiedModel, ValidatedModel):
    """
    Model representing aggregated sharp action for a game.
    
    Combines multiple signals and provides overall assessment
    of sharp betting activity for a specific game.
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
    
    # Sharp action summary
    total_signals: int = Field(
        default=0,
        description="Total number of sharp signals detected",
        ge=0
    )
    
    high_confidence_signals: int = Field(
        default=0,
        description="Number of high confidence signals",
        ge=0
    )
    
    signals: List[SharpSignal] = Field(
        default_factory=list,
        description="List of detected sharp signals"
    )
    
    # Overall assessment
    overall_confidence: ConfidenceLevel = Field(
        default=ConfidenceLevel.LOW,
        description="Overall confidence in sharp action"
    )
    
    primary_direction: Optional[SharpDirection] = Field(
        default=None,
        description="Primary direction of sharp action"
    )
    
    recommended_bet: Optional[str] = Field(
        default=None,
        description="Recommended bet based on sharp action",
        max_length=100
    )
    
    # Tracking
    first_detected: datetime = Field(
        ...,
        description="When sharp action was first detected"
    )
    
    last_updated: datetime = Field(
        ...,
        description="When analysis was last updated"
    )
    
    @validator("high_confidence_signals")
    def validate_high_confidence_count(cls, v: int, values: Dict[str, Any]) -> int:
        """Validate high confidence signal count."""
        total_signals = values.get("total_signals", 0)
        if v > total_signals:
            raise ValueError("High confidence signals cannot exceed total signals")
        return v
    
    def add_signal(self, signal: SharpSignal) -> None:
        """
        Add a new sharp signal and update aggregated data.
        
        Args:
            signal: Sharp signal to add
        """
        self.signals.append(signal)
        self.total_signals = len(self.signals)
        self.high_confidence_signals = sum(
            1 for s in self.signals if s.is_high_confidence
        )
        
        # Update overall confidence
        self._calculate_overall_confidence()
        
        # Update primary direction
        self._calculate_primary_direction()
        
        # Update timestamps
        self.last_updated = datetime.utcnow()
        if self.total_signals == 1:
            self.first_detected = signal.detected_at
        
        self.touch()
    
    def _calculate_overall_confidence(self) -> None:
        """Calculate overall confidence based on signals."""
        if not self.signals:
            self.overall_confidence = ConfidenceLevel.LOW
            return
        
        # Calculate weighted average of signal strengths
        total_strength = sum(signal.strength_score for signal in self.signals)
        avg_strength = total_strength / len(self.signals)
        
        # Bonus for multiple signals
        signal_bonus = min(0.1 * (len(self.signals) - 1), 0.3)
        final_strength = min(avg_strength + signal_bonus, 1.0)
        
        # Map to confidence level
        if final_strength >= 0.8:
            self.overall_confidence = ConfidenceLevel.VERY_HIGH
        elif final_strength >= 0.6:
            self.overall_confidence = ConfidenceLevel.HIGH
        elif final_strength >= 0.4:
            self.overall_confidence = ConfidenceLevel.MEDIUM
        else:
            self.overall_confidence = ConfidenceLevel.LOW
    
    def _calculate_primary_direction(self) -> None:
        """Calculate the primary direction of sharp action."""
        if not self.signals:
            self.primary_direction = None
            return
        
        # Count signals by direction, weighted by strength
        direction_scores: Dict[SharpDirection, float] = {}
        
        for signal in self.signals:
            score = signal.strength_score
            if signal.direction in direction_scores:
                direction_scores[signal.direction] += score
            else:
                direction_scores[signal.direction] = score
        
        # Find direction with highest score
        if direction_scores:
            self.primary_direction = max(
                direction_scores.keys(),
                key=lambda d: direction_scores[d]
            )
    
    @property
    def strength_score(self) -> float:
        """
        Get overall strength score for the sharp action.
        
        Returns:
            Score from 0.0 to 1.0
        """
        if not self.signals:
            return 0.0
        
        # Average signal strength with bonus for multiple signals
        avg_strength = sum(s.strength_score for s in self.signals) / len(self.signals)
        signal_bonus = min(0.05 * len(self.signals), 0.25)
        
        return min(avg_strength + signal_bonus, 1.0)
    
    @property
    def is_actionable(self) -> bool:
        """
        Check if the sharp action is strong enough to be actionable.
        
        Returns:
            True if action is recommended
        """
        return (
            self.overall_confidence in [ConfidenceLevel.HIGH, ConfidenceLevel.VERY_HIGH] and
            self.total_signals >= 2
        )
    
    def get_signals_by_type(self, signal_type: SharpSignalType) -> List[SharpSignal]:
        """
        Get all signals of a specific type.
        
        Args:
            signal_type: Type of signal to filter by
            
        Returns:
            List of matching signals
        """
        return [s for s in self.signals if s.signal_type == signal_type]
    
    def get_signals_by_split_type(self, split_type: SplitType) -> List[SharpSignal]:
        """
        Get all signals for a specific split type.
        
        Args:
            split_type: Type of split to filter by
            
        Returns:
            List of matching signals
        """
        return [s for s in self.signals if s.split_type == split_type]
    
    class Config:
        json_schema_extra = {
            "example": {
                "game_id": "2024-04-15-LAD-SF-1",
                "home_team": "SF",
                "away_team": "LAD",
                "game_datetime": "2024-04-15T22:45:00Z",
                "total_signals": 3,
                "high_confidence_signals": 2,
                "overall_confidence": "high",
                "primary_direction": "home",
                "recommended_bet": "SF +1.5",
                "first_detected": "2024-04-15T18:00:00Z",
                "last_updated": "2024-04-15T20:30:00Z"
            }
        } 