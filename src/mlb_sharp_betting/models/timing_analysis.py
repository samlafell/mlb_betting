"""
Timing analysis models for betting recommendation accuracy evaluation.

This module provides Pydantic models for analyzing betting recommendation
performance based on timing relative to game start.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import Field, ValidationInfo, field_validator, model_validator

from mlb_sharp_betting.models.base import BaseModel, IdentifiedModel, TimestampedModel
from mlb_sharp_betting.models.splits import BookType, DataSource, SplitType


class TimingBucket(str, Enum):
    """Time buckets for recommendation timing analysis."""

    ZERO_TO_TWO_HOURS = "0-2h"
    TWO_TO_SIX_HOURS = "2-6h"
    SIX_TO_TWENTY_FOUR_HOURS = "6-24h"
    TWENTY_FOUR_PLUS_HOURS = "24h+"

    @classmethod
    def from_hours_before_game(cls, hours: float) -> "TimingBucket":
        """
        Determine timing bucket from hours before game start.

        Args:
            hours: Hours before game start

        Returns:
            Appropriate timing bucket
        """
        if hours < 0:
            # Game has already started or passed
            return cls.ZERO_TO_TWO_HOURS
        elif hours <= 2:
            return cls.ZERO_TO_TWO_HOURS
        elif hours <= 6:
            return cls.TWO_TO_SIX_HOURS
        elif hours <= 24:
            return cls.SIX_TO_TWENTY_FOUR_HOURS
        else:
            return cls.TWENTY_FOUR_PLUS_HOURS

    @property
    def display_name(self) -> str:
        """Get human-readable display name for the bucket."""
        display_names = {
            "0-2h": "0-2 Hours Before",
            "2-6h": "2-6 Hours Before",
            "6-24h": "6-24 Hours Before",
            "24h+": "24+ Hours Before",
        }
        return display_names.get(self.value, self.value)

    @property
    def sort_order(self) -> int:
        """Get sort order for consistent bucket ordering."""
        order = {"0-2h": 1, "2-6h": 2, "6-24h": 3, "24h+": 4}
        return order.get(self.value, 999)


class ConfidenceLevel(str, Enum):
    """Statistical confidence levels for timing analysis."""

    LOW = "LOW"  # < 20 bets
    MODERATE = "MODERATE"  # 20-49 bets
    HIGH = "HIGH"  # 50-99 bets
    VERY_HIGH = "VERY_HIGH"  # 100+ bets

    @classmethod
    def from_sample_size(cls, sample_size: int) -> "ConfidenceLevel":
        """
        Determine confidence level from sample size.

        Args:
            sample_size: Number of bets in the sample

        Returns:
            Appropriate confidence level
        """
        if sample_size < 20:
            return cls.LOW
        elif sample_size < 50:
            return cls.MODERATE
        elif sample_size < 100:
            return cls.HIGH
        else:
            return cls.VERY_HIGH


class TimingPerformanceMetrics(BaseModel):
    """Performance metrics for a specific timing bucket."""

    # Basic performance
    total_bets: int = Field(
        ..., description="Total number of bets in this bucket", ge=0
    )

    wins: int = Field(..., description="Number of winning bets", ge=0)

    losses: int = Field(..., description="Number of losing bets", ge=0)

    pushes: int = Field(default=0, description="Number of pushed bets", ge=0)

    # Financial metrics
    total_units_wagered: Decimal = Field(..., description="Total units wagered", ge=0)

    total_profit_loss: Decimal = Field(..., description="Total profit/loss in units")

    # Average odds
    avg_odds_at_recommendation: Decimal | None = Field(
        default=None, description="Average odds when recommendation was made"
    )

    avg_closing_odds: Decimal | None = Field(
        default=None, description="Average closing odds at game time"
    )

    @field_validator("wins", "losses", "pushes")
    @classmethod
    def validate_bet_counts(cls, v: int, info: ValidationInfo) -> int:
        """Validate that bet counts don't exceed total."""
        if info.data and "total_bets" in info.data:
            total_decided = v
            for field in ["wins", "losses", "pushes"]:
                if field in info.data and field != info.field_name:
                    total_decided += info.data[field]

            if total_decided > info.data["total_bets"]:
                raise ValueError(
                    "Sum of wins, losses, and pushes cannot exceed total bets"
                )

        return v

    @property
    def win_rate(self) -> float:
        """Calculate win rate percentage."""
        if self.total_bets == 0:
            return 0.0
        return (self.wins / self.total_bets) * 100

    @property
    def roi_percentage(self) -> float:
        """Calculate ROI percentage."""
        if self.total_units_wagered == 0:
            return 0.0
        return float((self.total_profit_loss / self.total_units_wagered) * 100)

    @property
    def units_per_bet(self) -> float:
        """Average units wagered per bet."""
        if self.total_bets == 0:
            return 0.0
        return float(self.total_units_wagered / self.total_bets)

    @property
    def avg_profit_per_bet(self) -> float:
        """Average profit per bet."""
        if self.total_bets == 0:
            return 0.0
        return float(self.total_profit_loss / self.total_bets)

    @property
    def confidence_level(self) -> ConfidenceLevel:
        """Statistical confidence level based on sample size."""
        return ConfidenceLevel.from_sample_size(self.total_bets)

    @property
    def odds_movement(self) -> float | None:
        """Calculate average odds movement from recommendation to close."""
        if self.avg_odds_at_recommendation is None or self.avg_closing_odds is None:
            return None
        return float(self.avg_closing_odds - self.avg_odds_at_recommendation)


class TimingBucketAnalysis(IdentifiedModel):
    """Complete analysis for a specific timing bucket."""

    timing_bucket: TimingBucket = Field(
        ..., description="The timing bucket being analyzed"
    )

    source: DataSource | None = Field(
        default=None, description="Data source filter (None for all sources)"
    )

    book: BookType | None = Field(
        default=None, description="Sportsbook filter (None for all books)"
    )

    split_type: SplitType | None = Field(
        default=None, description="Bet type filter (None for all types)"
    )

    strategy_name: str | None = Field(
        default=None,
        description="Strategy filter (None for all strategies)",
        max_length=100,
    )

    # Performance metrics
    metrics: TimingPerformanceMetrics = Field(
        ..., description="Performance metrics for this bucket"
    )

    # Analysis period
    analysis_start_date: datetime = Field(
        ..., description="Start date of analysis period"
    )

    analysis_end_date: datetime = Field(..., description="End date of analysis period")

    @property
    def is_statistically_significant(self) -> bool:
        """Check if sample size is statistically significant."""
        return self.metrics.confidence_level in [
            ConfidenceLevel.HIGH,
            ConfidenceLevel.VERY_HIGH,
        ]

    @property
    def performance_grade(self) -> str:
        """Get performance grade based on win rate and ROI."""
        win_rate = self.metrics.win_rate
        roi = self.metrics.roi_percentage

        if win_rate >= 60 and roi >= 10:
            return "EXCELLENT"
        elif win_rate >= 55 and roi >= 5:
            return "GOOD"
        elif win_rate >= 52 and roi >= 0:
            return "PROFITABLE"
        elif win_rate >= 50:
            return "BREAKEVEN"
        else:
            return "UNPROFITABLE"

    @property
    def recommendation_confidence(self) -> str:
        """Get recommendation confidence for future bets."""
        if not self.is_statistically_significant:
            return "INSUFFICIENT_DATA"

        performance = self.performance_grade
        if performance in ["EXCELLENT", "GOOD"]:
            return "HIGH_CONFIDENCE"
        elif performance == "PROFITABLE":
            return "MODERATE_CONFIDENCE"
        else:
            return "LOW_CONFIDENCE"


class ComprehensiveTimingAnalysis(TimestampedModel):
    """Complete timing analysis across all buckets and segments."""

    # Analysis configuration
    analysis_name: str = Field(
        ..., description="Name/description of this analysis", max_length=200
    )

    total_games_analyzed: int = Field(
        ..., description="Total number of games included in analysis", ge=0
    )

    total_recommendations: int = Field(
        ..., description="Total number of recommendations analyzed", ge=0
    )

    # Analysis results by bucket
    bucket_analyses: list[TimingBucketAnalysis] = Field(
        ..., description="Analysis results for each timing bucket"
    )

    # Overall metrics
    overall_metrics: TimingPerformanceMetrics = Field(
        ..., description="Overall performance across all buckets"
    )

    # Best performing configurations
    best_bucket: TimingBucket | None = Field(
        default=None, description="Best performing timing bucket"
    )

    best_source: DataSource | None = Field(
        default=None, description="Best performing data source"
    )

    best_strategy: str | None = Field(
        default=None, description="Best performing strategy", max_length=100
    )

    # Trend analysis
    trends: dict[str, str | float | int] = Field(
        default_factory=dict, description="Trend analysis results and insights"
    )

    @model_validator(mode="after")
    def validate_analysis_consistency(self) -> "ComprehensiveTimingAnalysis":
        """Validate that analysis components are consistent."""
        if self.bucket_analyses:
            bucket_total = sum(
                analysis.metrics.total_bets for analysis in self.bucket_analyses
            )
            # Allow some variance for filtered vs unfiltered data
            if abs(bucket_total - self.total_recommendations) > (
                self.total_recommendations * 0.1
            ):
                # Only warn, don't fail - different filters might cause this
                pass

        return self

    @property
    def optimal_timing_recommendation(self) -> str:
        """Get recommendation for optimal betting timing."""
        if not self.bucket_analyses:
            return "INSUFFICIENT_DATA"

        # Find bucket with best combination of performance and confidence
        scored_buckets = []
        for analysis in self.bucket_analyses:
            if analysis.metrics.total_bets < 10:  # Skip buckets with tiny samples
                continue

            # Score based on ROI, win rate, and confidence
            roi_score = min(
                analysis.metrics.roi_percentage / 20, 1.0
            )  # Cap at 20% ROI = max score
            win_rate_score = max(
                0, (analysis.metrics.win_rate - 50) / 20
            )  # 50% = 0, 70% = 1.0
            confidence_score = (
                analysis.metrics.confidence_level.value.count("HIGH") * 0.25
            )

            total_score = roi_score + win_rate_score + confidence_score
            scored_buckets.append((analysis.timing_bucket, total_score, analysis))

        if not scored_buckets:
            return "INSUFFICIENT_DATA"

        # Sort by score descending
        scored_buckets.sort(key=lambda x: x[1], reverse=True)
        best_bucket = scored_buckets[0][2]

        if best_bucket.metrics.roi_percentage > 5 and best_bucket.metrics.win_rate > 52:
            return f"RECOMMENDED: {best_bucket.timing_bucket.display_name} ({best_bucket.metrics.win_rate:.1f}% WR, {best_bucket.metrics.roi_percentage:.1f}% ROI)"
        else:
            return "NO_CLEAR_ADVANTAGE"


class RealtimeTimingLookup(BaseModel):
    """Model for real-time timing recommendation lookup."""

    hours_until_game: float = Field(..., description="Hours until game start", ge=0)

    source: DataSource | None = Field(
        default=None, description="Data source for the recommendation"
    )

    book: BookType | None = Field(
        default=None, description="Sportsbook for the recommendation"
    )

    split_type: SplitType = Field(..., description="Type of bet being considered")

    strategy_name: str | None = Field(
        default=None,
        description="Strategy generating the recommendation",
        max_length=100,
    )

    @property
    def timing_bucket(self) -> TimingBucket:
        """Get timing bucket for this lookup."""
        return TimingBucket.from_hours_before_game(self.hours_until_game)


class TimingRecommendation(BaseModel):
    """Recommendation based on timing analysis."""

    # Input context
    lookup: RealtimeTimingLookup = Field(..., description="The lookup parameters used")

    # Historical performance
    historical_metrics: TimingPerformanceMetrics | None = Field(
        default=None, description="Historical performance for this timing/context"
    )

    # Recommendation
    recommendation: str = Field(
        ..., description="Betting recommendation based on timing", max_length=500
    )

    confidence: str = Field(..., description="Confidence level for this recommendation")

    expected_win_rate: float | None = Field(
        default=None,
        description="Expected win rate percentage based on historical data",
        ge=0,
        le=100,
    )

    expected_roi: float | None = Field(
        default=None, description="Expected ROI percentage based on historical data"
    )

    # Risk assessment
    risk_factors: list[str] = Field(
        default_factory=list, description="Risk factors to consider for this timing"
    )

    sample_size_warning: bool = Field(
        default=False, description="Whether to warn about insufficient sample size"
    )

    @property
    def is_recommended(self) -> bool:
        """Whether this timing is recommended for betting."""
        return (
            self.confidence in ["HIGH_CONFIDENCE", "MODERATE_CONFIDENCE"]
            and not self.sample_size_warning
        )

    @property
    def action_needed(self) -> str:
        """Get recommended action."""
        if self.sample_size_warning:
            return "WAIT_FOR_MORE_DATA"
        elif self.confidence == "HIGH_CONFIDENCE":
            return "PLACE_BET"
        elif self.confidence == "MODERATE_CONFIDENCE":
            return "CONSIDER_BET"
        else:
            return "AVOID_BET"
