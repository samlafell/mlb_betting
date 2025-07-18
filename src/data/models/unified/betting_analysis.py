"""
Unified Betting Analysis Models

Consolidates betting analysis models from:
- src/mlb_sharp_betting/analysis/betting_analysis.py
- src/mlb_sharp_betting/models/betting_analysis.py
- sportsbookreview/analysis/ (analysis patterns)
- action/ (analysis patterns)

All times are in EST as per project requirements.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field, computed_field, field_validator, ValidationInfo

from .base import AnalysisEntity, ValidatedModel
from .odds import BookType, MarketType, OddsData


class BettingSignalType(str, Enum):
    """Types of betting signals that can be detected."""

    SHARP_ACTION = "sharp_action"
    STEAM_MOVE = "steam_move"
    REVERSE_LINE_MOVEMENT = "reverse_line_movement"
    BOOK_CONFLICT = "book_conflict"
    LATE_MONEY = "late_money"
    TIMING_EDGE = "timing_edge"
    VOLUME_SPIKE = "volume_spike"
    CONSENSUS_FADE = "consensus_fade"
    ARBITRAGE = "arbitrage"
    MARKET_INEFFICIENCY = "market_inefficiency"


class SignalStrength(str, Enum):
    """Signal strength levels."""

    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    EXTREME = "extreme"


class BettingRecommendation(str, Enum):
    """Betting recommendation types."""

    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    AVOID = "avoid"
    FADE = "fade"


class BettingAnalysis(AnalysisEntity):
    """
    Unified betting analysis model.

    Consolidates analysis results from all detection systems and provides
    a comprehensive view of betting opportunities.
    """

    # Analysis identification
    analysis_id: str = Field(
        ...,
        description="Unique identifier for this analysis",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ...,
        description="Reference to the game being analyzed",
        min_length=1,
        max_length=100,
    )

    # Analysis metadata
    analysis_type: str = Field(
        ..., description="Type of analysis performed", min_length=1, max_length=50
    )

    analysis_timestamp: datetime = Field(
        ..., description="When analysis was performed (EST)"
    )

    analyzer_version: str = Field(
        default="1.0", description="Version of the analyzer used", max_length=20
    )

    # Market context
    market_type: MarketType = Field(..., description="Market type being analyzed")

    market_side: str | None = Field(
        default=None,
        description="Specific side of the market (home/away/over/under)",
        max_length=20,
    )

    # Analysis results
    signals_detected: list[BettingSignalType] = Field(
        default_factory=list, description="List of betting signals detected"
    )

    primary_signal: BettingSignalType | None = Field(
        default=None, description="Primary/strongest signal detected"
    )

    signal_strength: SignalStrength = Field(
        default=SignalStrength.MODERATE, description="Overall signal strength"
    )

    confidence_score: float = Field(
        ..., description="Confidence in the analysis (0.0-1.0)", ge=0.0, le=1.0
    )

    # Recommendations
    recommendation: BettingRecommendation = Field(
        ..., description="Betting recommendation based on analysis"
    )

    recommended_bet: str | None = Field(
        default=None, description="Specific bet recommendation", max_length=100
    )

    recommended_stake: float | None = Field(
        default=None,
        description="Recommended stake size (0.0-1.0 as fraction of bankroll)",
        ge=0.0,
        le=1.0,
    )

    # Supporting data
    odds_data: list[OddsData] = Field(
        default_factory=list, description="Odds data used in analysis"
    )

    feature_importance: dict[str, float] = Field(
        default_factory=dict, description="Feature importance scores for the analysis"
    )

    model_features: dict[str, Any] = Field(
        default_factory=dict, description="Model features and values used"
    )

    # Risk assessment
    risk_level: str = Field(
        default="MEDIUM",
        description="Risk level assessment (LOW, MEDIUM, HIGH)",
        pattern="^(LOW|MEDIUM|HIGH)$",
    )

    max_loss_potential: float | None = Field(
        default=None, description="Maximum potential loss (0.0-1.0)", ge=0.0, le=1.0
    )

    # Timing information
    time_to_game: int | None = Field(
        default=None, description="Minutes until game start", ge=0
    )

    optimal_bet_time: datetime | None = Field(
        default=None, description="Optimal time to place bet (EST)"
    )

    expires_at: datetime | None = Field(
        default=None, description="When this analysis expires (EST)"
    )

    # Validation
    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: float) -> float:
        """Ensure confidence score is reasonable."""
        if v < 0.0 or v > 1.0:
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return v

    @field_validator("signals_detected")
    @classmethod
    def validate_signals_detected(
        cls, v: list[BettingSignalType]
    ) -> list[BettingSignalType]:
        """Ensure signals list is not empty if primary signal is set."""
        # This will be validated in model_validate
        return v

    @field_validator("primary_signal")
    @classmethod  
    def validate_primary_signal(
        cls, v: BettingSignalType | None, info: ValidationInfo
    ) -> BettingSignalType | None:
        """Ensure primary signal is in signals detected list."""
        if v is not None and info.data:
            signals = info.data.get("signals_detected", [])
            if signals and v not in signals:
                raise ValueError("Primary signal must be in signals_detected list")
        return v

    # Computed properties
    @property
    def is_actionable(self) -> bool:
        """Check if analysis is actionable (has recommendation other than hold)."""
        return self.recommendation != BettingRecommendation.HOLD

    @property
    def is_high_confidence(self) -> bool:
        """Check if analysis is high confidence."""
        return self.confidence_score >= 0.75

    @property
    def is_strong_signal(self) -> bool:
        """Check if analysis has strong signal strength."""
        return self.signal_strength in [SignalStrength.STRONG, SignalStrength.EXTREME]

    @property
    def signal_count(self) -> int:
        """Get number of signals detected."""
        return len(self.signals_detected)

    @property
    def is_expired(self) -> bool:
        """Check if analysis has expired."""
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at

    @property
    def minutes_until_expiry(self) -> int | None:
        """Get minutes until analysis expires."""
        if self.expires_at is None:
            return None

        now = datetime.now()
        if now >= self.expires_at:
            return 0

        delta = self.expires_at - now
        return int(delta.total_seconds() / 60)

    # Utility methods
    def add_signal(self, signal: BettingSignalType) -> None:
        """Add a detected signal to the analysis."""
        if signal not in self.signals_detected:
            self.signals_detected.append(signal)

            # Update primary signal if this is the first or stronger
            if self.primary_signal is None:
                self.primary_signal = signal

    def get_signal_description(self) -> str:
        """Get human-readable description of detected signals."""
        if not self.signals_detected:
            return "No signals detected"

        if len(self.signals_detected) == 1:
            return f"{self.signals_detected[0].value.replace('_', ' ').title()}"

        return f"{len(self.signals_detected)} signals: {', '.join(s.value.replace('_', ' ').title() for s in self.signals_detected)}"

    def get_recommendation_display(self) -> str:
        """Get formatted recommendation display."""
        base = self.recommendation.value.replace("_", " ").title()

        if self.recommended_bet:
            return f"{base}: {self.recommended_bet}"

        return base

    def calculate_kelly_stake(self, bankroll: float, odds: float) -> float:
        """
        Calculate Kelly Criterion stake size.

        Args:
            bankroll: Total bankroll
            odds: Decimal odds for the bet

        Returns:
            Recommended stake amount
        """
        if self.confidence_score <= 0.5:
            return 0.0

        # Kelly formula: f = (bp - q) / b
        # Where: b = odds - 1, p = confidence, q = 1 - confidence
        b = odds - 1
        p = self.confidence_score
        q = 1 - p

        kelly_fraction = (b * p - q) / b

        # Cap at 5% of bankroll for safety
        kelly_fraction = min(kelly_fraction, 0.05)
        kelly_fraction = max(kelly_fraction, 0.0)

        return kelly_fraction * bankroll


class SharpAction(ValidatedModel):
    """
    Sharp action detection model.

    Tracks sharp money movements and professional betting patterns.
    """

    # Identification
    sharp_action_id: str = Field(
        ...,
        description="Unique identifier for this sharp action",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Detection metadata
    detected_at: datetime = Field(
        ..., description="When sharp action was detected (EST)"
    )

    detector_version: str = Field(
        default="1.0", description="Version of the detector used", max_length=20
    )

    # Sharp action details
    market_type: MarketType = Field(
        ..., description="Market where sharp action was detected"
    )

    sharp_side: str = Field(
        ..., description="Side receiving sharp action", max_length=20
    )

    sharp_books: list[BookType] = Field(
        default_factory=list, description="Books showing sharp action"
    )

    # Movement details
    line_movement: float | None = Field(
        default=None, description="Line movement amount"
    )

    odds_movement: int | None = Field(
        default=None, description="Odds movement in American format"
    )

    movement_speed: str | None = Field(
        default=None,
        description="Speed of movement (slow, moderate, fast, instant)",
        pattern="^(slow|moderate|fast|instant)$",
    )

    # Volume indicators
    volume_spike: bool = Field(
        default=False, description="Whether volume spike was detected"
    )

    volume_ratio: float | None = Field(
        default=None, description="Volume ratio compared to average", ge=0.0
    )

    # Sharp indicators
    reverse_line_movement: bool = Field(
        default=False, description="Whether reverse line movement occurred"
    )

    steam_move: bool = Field(
        default=False, description="Whether steam move was detected"
    )

    coordinated_move: bool = Field(
        default=False, description="Whether coordinated move across books"
    )

    # Confidence metrics
    sharp_confidence: float = Field(
        ..., description="Confidence this is sharp action (0.0-1.0)", ge=0.0, le=1.0
    )

    signal_strength: SignalStrength = Field(
        default=SignalStrength.MODERATE, description="Strength of the sharp signal"
    )

    # Context
    time_to_game: int | None = Field(
        default=None, description="Minutes until game start when detected", ge=0
    )

    market_context: dict[str, Any] = Field(
        default_factory=dict, description="Additional market context"
    )

    # Computed properties
    @property
    def is_high_confidence(self) -> bool:
        """Check if sharp action is high confidence."""
        return self.sharp_confidence >= 0.8

    @property
    def has_multiple_indicators(self) -> bool:
        """Check if multiple sharp indicators are present."""
        indicators = [
            self.reverse_line_movement,
            self.steam_move,
            self.coordinated_move,
            self.volume_spike,
        ]
        return sum(indicators) >= 2

    @property
    def is_late_sharp_action(self) -> bool:
        """Check if this is late sharp action (within 2 hours of game)."""
        return self.time_to_game is not None and self.time_to_game <= 120

    def get_sharp_description(self) -> str:
        """Get human-readable description of sharp action."""
        indicators = []

        if self.reverse_line_movement:
            indicators.append("Reverse Line Movement")
        if self.steam_move:
            indicators.append("Steam Move")
        if self.coordinated_move:
            indicators.append("Coordinated Move")
        if self.volume_spike:
            indicators.append("Volume Spike")

        if not indicators:
            return "Sharp Action Detected"

        return f"Sharp Action: {', '.join(indicators)}"


class BettingSplit(ValidatedModel):
    """
    Betting split model tracking public vs sharp money distribution.

    Consolidates betting split data from various sources.
    """

    # Identification
    split_id: str = Field(
        ...,
        description="Unique identifier for this split",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Split metadata
    timestamp: datetime = Field(..., description="When split data was captured (EST)")

    data_source: str = Field(..., description="Source of split data", max_length=50)

    # Market information
    market_type: MarketType = Field(..., description="Market type for this split")

    # Public betting data
    public_bet_percentage: float | None = Field(
        default=None, description="Percentage of public bets (0.0-1.0)", ge=0.0, le=1.0
    )

    public_money_percentage: float | None = Field(
        default=None, description="Percentage of public money (0.0-1.0)", ge=0.0, le=1.0
    )

    # Sharp betting data
    sharp_bet_percentage: float | None = Field(
        default=None, description="Percentage of sharp bets (0.0-1.0)", ge=0.0, le=1.0
    )

    sharp_money_percentage: float | None = Field(
        default=None, description="Percentage of sharp money (0.0-1.0)", ge=0.0, le=1.0
    )

    # Side-specific splits
    home_bet_percentage: float | None = Field(
        default=None,
        description="Percentage of bets on home team (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    away_bet_percentage: float | None = Field(
        default=None,
        description="Percentage of bets on away team (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    over_bet_percentage: float | None = Field(
        default=None, description="Percentage of bets on over (0.0-1.0)", ge=0.0, le=1.0
    )

    under_bet_percentage: float | None = Field(
        default=None,
        description="Percentage of bets on under (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    # Volume data
    total_bet_count: int | None = Field(
        default=None, description="Total number of bets", ge=0
    )

    total_money_amount: float | None = Field(
        default=None, description="Total money wagered", ge=0.0
    )

    # Computed properties
    @property
    def public_sharp_divergence(self) -> float | None:
        """Calculate divergence between public and sharp money."""
        if self.public_money_percentage is None or self.sharp_money_percentage is None:
            return None

        return abs(self.public_money_percentage - self.sharp_money_percentage)

    @property
    def is_contrarian_opportunity(self) -> bool:
        """Check if this represents a contrarian betting opportunity."""
        if self.public_sharp_divergence is None:
            return False

        # Significant divergence suggests contrarian opportunity
        return self.public_sharp_divergence >= 0.3

    @property
    def public_heavy_side(self) -> str | None:
        """Get the side heavily favored by public."""
        if self.public_bet_percentage is None:
            return None

        if self.public_bet_percentage >= 0.65:
            return "public_heavy"
        elif self.public_bet_percentage <= 0.35:
            return "public_light"
        else:
            return "balanced"

    @property
    def sharp_heavy_side(self) -> str | None:
        """Get the side heavily favored by sharp money."""
        if self.sharp_money_percentage is None:
            return None

        if self.sharp_money_percentage >= 0.65:
            return "sharp_heavy"
        elif self.sharp_money_percentage <= 0.35:
            return "sharp_light"
        else:
            return "balanced"

    def get_split_summary(self) -> str:
        """Get human-readable summary of betting splits."""
        summary_parts = []

        if self.public_bet_percentage is not None:
            summary_parts.append(f"Public: {self.public_bet_percentage:.1%} bets")

        if self.public_money_percentage is not None:
            summary_parts.append(f"{self.public_money_percentage:.1%} money")

        if self.sharp_money_percentage is not None:
            summary_parts.append(f"Sharp: {self.sharp_money_percentage:.1%} money")

        if not summary_parts:
            return "No split data available"

        return " | ".join(summary_parts)

    def calculate_fade_value(self) -> float:
        """Calculate fade value based on public/sharp divergence."""
        if self.public_sharp_divergence is None:
            return 0.0

        # Higher divergence = higher fade value
        fade_value = self.public_sharp_divergence

        # Bonus for extreme public percentages
        if self.public_bet_percentage is not None:
            if self.public_bet_percentage >= 0.8 or self.public_bet_percentage <= 0.2:
                fade_value += 0.1

        return min(fade_value, 1.0)
