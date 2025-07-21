"""
Unified Sharp Data Models

Consolidates sharp betting detection models from:
- src/mlb_sharp_betting/models/sharp_detection.py
- src/mlb_sharp_betting/analysis/sharp_money_detector.py
- sportsbookreview/models/sharp_signals.py (if exists)
- action/models/sharp_data.py (if exists)

All times are in EST as per project requirements.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field
from ....core.pydantic_compat import computed_field, field_validator, ValidationInfo

from .base import AnalysisEntity, ValidatedModel
from .odds import BookType, MarketType


class SharpDirection(str, Enum):
    """Direction of sharp action."""

    LONG = "long"  # Betting on the team/over
    SHORT = "short"  # Betting against the team/under
    NEUTRAL = "neutral"  # No clear direction


class ConfidenceLevel(str, Enum):
    """Confidence levels for sharp detection."""

    LOW = "low"  # 50-65% confidence
    MEDIUM = "medium"  # 65-80% confidence
    HIGH = "high"  # 80-95% confidence
    VERY_HIGH = "very_high"  # 95%+ confidence


class SharpIndicatorType(str, Enum):
    """Types of sharp indicators."""

    REVERSE_LINE_MOVEMENT = "reverse_line_movement"
    STEAM_MOVE = "steam_move"
    COORDINATED_MOVE = "coordinated_move"
    VOLUME_SPIKE = "volume_spike"
    SHARP_BOOK_MOVEMENT = "sharp_book_movement"
    LATE_MONEY = "late_money"
    CONTRARIAN_MOVE = "contrarian_move"
    ARBITRAGE_CLOSE = "arbitrage_close"
    LIMIT_REDUCTION = "limit_reduction"
    PROFESSIONAL_PATTERN = "professional_pattern"


class SharpSignal(AnalysisEntity):
    """
    Unified sharp signal model.

    Represents a detected sharp betting signal with comprehensive
    metadata and confidence scoring.
    """

    # Signal identification
    signal_id: str = Field(
        ...,
        description="Unique identifier for this signal",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Signal metadata
    signal_type: SharpIndicatorType = Field(
        ..., description="Type of sharp indicator detected"
    )

    detected_at: datetime = Field(..., description="When signal was detected (EST)")

    detector_name: str = Field(
        ..., description="Name of the detector that found this signal", max_length=100
    )

    detector_version: str = Field(
        default="1.0", description="Version of the detector", max_length=20
    )

    # Market context
    market_type: MarketType = Field(..., description="Market where signal was detected")

    market_side: str | None = Field(
        default=None, description="Specific side of the market", max_length=20
    )

    # Sharp action details
    sharp_direction: SharpDirection = Field(
        ..., description="Direction of sharp action"
    )

    sharp_books: list[BookType] = Field(
        default_factory=list, description="Books showing sharp action"
    )

    non_sharp_books: list[BookType] = Field(
        default_factory=list, description="Books not showing sharp action"
    )

    # Confidence and strength
    confidence_level: ConfidenceLevel = Field(
        ..., description="Confidence level of the signal"
    )

    confidence_score: float = Field(
        ..., description="Numerical confidence score (0.0-1.0)", ge=0.0, le=1.0
    )

    signal_strength: float = Field(
        ..., description="Strength of the signal (0.0-1.0)", ge=0.0, le=1.0
    )

    # Movement data
    line_movement: float | None = Field(
        default=None, description="Line movement amount"
    )

    odds_movement: int | None = Field(
        default=None, description="Odds movement in American format"
    )

    movement_speed: str | None = Field(
        default=None,
        description="Speed of movement",
        pattern="^(slow|moderate|fast|instant)$",
    )

    # Volume and money indicators
    volume_spike: bool = Field(
        default=False, description="Whether volume spike was detected"
    )

    volume_ratio: float | None = Field(
        default=None, description="Volume ratio compared to average", ge=0.0
    )

    money_percentage: float | None = Field(
        default=None,
        description="Percentage of money on sharp side (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    bet_percentage: float | None = Field(
        default=None,
        description="Percentage of bets on sharp side (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    # Timing context
    time_to_game: int | None = Field(
        default=None, description="Minutes until game start when detected", ge=0
    )

    is_late_signal: bool = Field(
        default=False, description="Whether this is a late signal (within 2 hours)"
    )

    # Supporting evidence
    supporting_indicators: list[SharpIndicatorType] = Field(
        default_factory=list, description="Additional supporting indicators"
    )

    contradicting_indicators: list[SharpIndicatorType] = Field(
        default_factory=list, description="Contradicting indicators that weaken signal"
    )

    # Raw data
    raw_data: dict[str, Any] = Field(
        default_factory=dict, description="Raw data used in signal detection"
    )

    feature_values: dict[str, float] = Field(
        default_factory=dict, description="Feature values used in detection algorithm"
    )

    # Validation
    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: float, info: ValidationInfo) -> float:
        """Ensure confidence score matches confidence level."""
        if info.data and "confidence_level" in info.data:
            level = info.data["confidence_level"]

            if level == ConfidenceLevel.LOW and not (0.5 <= v < 0.65):
                raise ValueError("LOW confidence should be 0.5-0.65")
            elif level == ConfidenceLevel.MEDIUM and not (0.65 <= v < 0.8):
                raise ValueError("MEDIUM confidence should be 0.65-0.8")
            elif level == ConfidenceLevel.HIGH and not (0.8 <= v < 0.95):
                raise ValueError("HIGH confidence should be 0.8-0.95")
            elif level == ConfidenceLevel.VERY_HIGH and not (0.95 <= v <= 1.0):
                raise ValueError("VERY_HIGH confidence should be 0.95-1.0")

        return v

    @field_validator("is_late_signal")
    @classmethod
    def validate_late_signal(cls, v: bool, info: ValidationInfo) -> bool:
        """Ensure late signal flag matches time to game."""
        if info.data and "time_to_game" in info.data:
            time_to_game = info.data["time_to_game"]
            if time_to_game is not None:
                expected_late = time_to_game <= 120
                if v != expected_late:
                    return expected_late

        return v

    # Computed properties
    @property
    def is_high_confidence(self) -> bool:
        """Check if signal is high confidence."""
        return self.confidence_level in [
            ConfidenceLevel.HIGH,
            ConfidenceLevel.VERY_HIGH,
        ]

    @property
    def is_strong_signal(self) -> bool:
        """Check if signal is strong."""
        return self.signal_strength >= 0.7

    @property
    def has_supporting_evidence(self) -> bool:
        """Check if signal has supporting evidence."""
        return len(self.supporting_indicators) > 0

    @property
    def has_contradicting_evidence(self) -> bool:
        """Check if signal has contradicting evidence."""
        return len(self.contradicting_indicators) > 0

    @property
    def net_indicator_score(self) -> int:
        """Calculate net indicator score (supporting - contradicting)."""
        return len(self.supporting_indicators) - len(self.contradicting_indicators)

    @property
    def book_consensus(self) -> float:
        """Calculate book consensus (sharp books / total books)."""
        total_books = len(self.sharp_books) + len(self.non_sharp_books)
        if total_books == 0:
            return 0.0

        return len(self.sharp_books) / total_books

    @property
    def is_coordinated_move(self) -> bool:
        """Check if this represents a coordinated move across books."""
        return len(self.sharp_books) >= 3 and self.book_consensus >= 0.7

    @property
    def public_sharp_divergence(self) -> float | None:
        """Calculate divergence between public and sharp percentages."""
        if self.money_percentage is None or self.bet_percentage is None:
            return None

        return abs(self.money_percentage - self.bet_percentage)

    # Utility methods
    def get_signal_description(self) -> str:
        """Get human-readable description of the signal."""
        base = self.signal_type.value.replace("_", " ").title()

        if self.sharp_direction != SharpDirection.NEUTRAL:
            direction = self.sharp_direction.value.title()
            base = f"{base} ({direction})"

        if self.confidence_level:
            base = f"{base} - {self.confidence_level.value.title()} Confidence"

        return base

    def calculate_composite_score(self) -> float:
        """Calculate composite score combining all signal metrics."""
        score = 0.0

        # Base confidence score (40% weight)
        score += self.confidence_score * 0.4

        # Signal strength (30% weight)
        score += self.signal_strength * 0.3

        # Book consensus (20% weight)
        score += self.book_consensus * 0.2

        # Supporting evidence bonus (10% weight)
        if self.supporting_indicators:
            evidence_score = min(len(self.supporting_indicators) / 5, 1.0)
            score += evidence_score * 0.1

        # Contradicting evidence penalty
        if self.contradicting_indicators:
            penalty = min(len(self.contradicting_indicators) / 5, 0.2)
            score -= penalty

        # Late signal bonus
        if self.is_late_signal:
            score += 0.05

        return max(0.0, min(1.0, score))

    def is_actionable(
        self, min_confidence: float = 0.65, min_strength: float = 0.6
    ) -> bool:
        """
        Check if signal is actionable based on thresholds.

        Args:
            min_confidence: Minimum confidence score
            min_strength: Minimum signal strength

        Returns:
            True if signal meets actionable criteria
        """
        return (
            self.confidence_score >= min_confidence
            and self.signal_strength >= min_strength
            and not self.has_contradicting_evidence
        )

    def get_risk_assessment(self) -> dict[str, Any]:
        """Get risk assessment for the signal."""
        risk_factors = []
        risk_score = 0.0

        # Low confidence risk
        if self.confidence_score < 0.7:
            risk_factors.append("Low confidence")
            risk_score += 0.2

        # Contradicting evidence risk
        if self.has_contradicting_evidence:
            risk_factors.append("Contradicting evidence")
            risk_score += 0.3

        # Low book consensus risk
        if self.book_consensus < 0.5:
            risk_factors.append("Low book consensus")
            risk_score += 0.1

        # Public-sharp divergence risk
        if self.public_sharp_divergence and self.public_sharp_divergence < 0.1:
            risk_factors.append("Low public-sharp divergence")
            risk_score += 0.1

        # Late signal risk (can be good or bad)
        if self.is_late_signal:
            if self.signal_strength < 0.8:
                risk_factors.append("Late signal with moderate strength")
                risk_score += 0.1

        risk_level = "LOW"
        if risk_score >= 0.3:
            risk_level = "HIGH"
        elif risk_score >= 0.15:
            risk_level = "MEDIUM"

        return {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "risk_factors": risk_factors,
            "is_high_risk": risk_score >= 0.3,
        }


class SharpMoney(ValidatedModel):
    """
    Sharp money tracking model.

    Tracks the flow of sharp money across different markets and books.
    """

    # Identification
    sharp_money_id: str = Field(
        ...,
        description="Unique identifier for this sharp money entry",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Timing
    timestamp: datetime = Field(..., description="When sharp money was detected (EST)")

    # Market context
    market_type: MarketType = Field(..., description="Market receiving sharp money")

    sportsbook: BookType = Field(..., description="Sportsbook receiving sharp money")

    # Money flow data
    sharp_side: str = Field(
        ..., description="Side receiving sharp money", max_length=20
    )

    estimated_amount: float | None = Field(
        default=None, description="Estimated amount of sharp money", ge=0.0
    )

    percentage_of_handle: float | None = Field(
        default=None, description="Percentage of total handle (0.0-1.0)", ge=0.0, le=1.0
    )

    # Sharp characteristics
    bet_size_category: str | None = Field(
        default=None,
        description="Category of bet size (small, medium, large, massive)",
        pattern="^(small|medium|large|massive)$",
    )

    betting_pattern: str | None = Field(
        default=None, description="Betting pattern observed", max_length=100
    )

    is_coordinated: bool = Field(
        default=False, description="Whether this appears to be coordinated sharp action"
    )

    # Impact assessment
    line_impact: float | None = Field(
        default=None, description="Impact on line movement"
    )

    odds_impact: int | None = Field(
        default=None, description="Impact on odds (American format)"
    )

    market_impact_score: float | None = Field(
        default=None,
        description="Overall market impact score (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    # Validation and confidence
    sharp_probability: float = Field(
        ...,
        description="Probability this is genuine sharp money (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    validation_indicators: list[str] = Field(
        default_factory=list,
        description="Indicators supporting sharp money classification",
    )

    # Context
    time_to_game: int | None = Field(
        default=None, description="Minutes until game start", ge=0
    )

    related_signals: list[str] = Field(
        default_factory=list, description="Related sharp signal IDs"
    )

    # Computed properties
    @property
    def is_high_probability(self) -> bool:
        """Check if sharp money is high probability."""
        return self.sharp_probability >= 0.8

    @property
    def is_significant_amount(self) -> bool:
        """Check if amount is significant."""
        return self.bet_size_category in ["large", "massive"] or (
            self.percentage_of_handle is not None and self.percentage_of_handle >= 0.1
        )

    @property
    def is_late_money(self) -> bool:
        """Check if this is late sharp money."""
        return self.time_to_game is not None and self.time_to_game <= 120

    def get_impact_assessment(self) -> dict[str, Any]:
        """Get comprehensive impact assessment."""
        impact_level = "LOW"

        if self.market_impact_score is not None:
            if self.market_impact_score >= 0.7:
                impact_level = "HIGH"
            elif self.market_impact_score >= 0.4:
                impact_level = "MEDIUM"

        return {
            "impact_level": impact_level,
            "line_moved": self.line_impact is not None and abs(self.line_impact) >= 0.5,
            "odds_moved": self.odds_impact is not None and abs(self.odds_impact) >= 5,
            "is_coordinated": self.is_coordinated,
            "is_late": self.is_late_money,
            "is_significant": self.is_significant_amount,
        }


class SharpConsensus(AnalysisEntity):
    """
    Sharp consensus model aggregating multiple sharp signals.

    Provides a consensus view of sharp action across multiple indicators.
    """

    # Identification
    consensus_id: str = Field(
        ...,
        description="Unique identifier for this consensus",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Consensus metadata
    calculated_at: datetime = Field(
        ..., description="When consensus was calculated (EST)"
    )

    # Market context
    market_type: MarketType = Field(..., description="Market for this consensus")

    # Consensus data
    sharp_signals: list[str] = Field(
        default_factory=list, description="Sharp signal IDs included in consensus"
    )

    signal_count: int = Field(
        default=0, description="Number of signals in consensus", ge=0
    )

    # Consensus results
    consensus_direction: SharpDirection = Field(
        ..., description="Consensus direction of sharp action"
    )

    consensus_confidence: float = Field(
        ..., description="Confidence in consensus (0.0-1.0)", ge=0.0, le=1.0
    )

    consensus_strength: float = Field(
        ..., description="Strength of consensus (0.0-1.0)", ge=0.0, le=1.0
    )

    # Agreement metrics
    signal_agreement: float = Field(
        ..., description="Agreement between signals (0.0-1.0)", ge=0.0, le=1.0
    )

    book_agreement: float = Field(
        ..., description="Agreement between books (0.0-1.0)", ge=0.0, le=1.0
    )

    # Timing analysis
    signal_timing_spread: int | None = Field(
        default=None, description="Time spread of signals in minutes", ge=0
    )

    is_coordinated_consensus: bool = Field(
        default=False, description="Whether consensus represents coordinated action"
    )

    # Quality metrics
    consensus_quality: str = Field(
        ...,
        description="Quality of consensus (LOW, MEDIUM, HIGH, EXCELLENT)",
        pattern="^(LOW|MEDIUM|HIGH|EXCELLENT)$",
    )

    outlier_signals: list[str] = Field(
        default_factory=list, description="Signal IDs that are outliers"
    )

    # Computed properties
    @property
    def is_high_quality(self) -> bool:
        """Check if consensus is high quality."""
        return self.consensus_quality in ["HIGH", "EXCELLENT"]

    @property
    def is_strong_consensus(self) -> bool:
        """Check if consensus is strong."""
        return (
            self.consensus_confidence >= 0.8
            and self.signal_agreement >= 0.7
            and self.signal_count >= 3
        )

    @property
    def has_outliers(self) -> bool:
        """Check if consensus has outlier signals."""
        return len(self.outlier_signals) > 0

    def get_consensus_summary(self) -> dict[str, Any]:
        """Get comprehensive consensus summary."""
        return {
            "direction": self.consensus_direction.value,
            "confidence": self.consensus_confidence,
            "strength": self.consensus_strength,
            "signal_count": self.signal_count,
            "agreement": self.signal_agreement,
            "quality": self.consensus_quality,
            "is_coordinated": self.is_coordinated_consensus,
            "has_outliers": self.has_outliers,
            "is_actionable": self.is_strong_consensus,
        }
