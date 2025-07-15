"""
Unified Analysis Models

Modern, type-safe models for the unified strategy processing system.
Consolidates and enhances models from the legacy mlb_sharp_betting system.

These models provide:
- Type-safe data structures with Pydantic validation
- Async-compatible serialization
- Enhanced performance metrics
- Cross-strategy comparison capabilities
- Integration with the unified database layer

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import Field, validator

from src.data.models.unified.base import UnifiedBaseModel


class SignalType(str, Enum):
    """Enhanced signal types for the unified system"""

    SHARP_ACTION = "sharp_action"
    BOOK_CONFLICT = "book_conflict"
    LINE_MOVEMENT = "line_movement"
    PUBLIC_FADE = "public_fade"
    CONSENSUS = "consensus"
    TIMING_BASED = "timing_based"
    HYBRID_SHARP = "hybrid_sharp"
    OPPOSING_MARKETS = "opposing_markets"
    UNDERDOG_VALUE = "underdog_value"
    LATE_FLIP = "late_flip"


class StrategyCategory(str, Enum):
    """Strategy categorization for organization and routing"""

    SHARP_ACTION = "sharp_action"
    MARKET_INEFFICIENCY = "market_inefficiency"
    CONSENSUS_ANALYSIS = "consensus_analysis"
    TIMING_ANALYSIS = "timing_analysis"
    HYBRID_ANALYSIS = "hybrid_analysis"
    VALUE_ANALYSIS = "value_analysis"


class ConfidenceLevel(str, Enum):
    """Confidence levels for strategy signals"""

    VERY_HIGH = "very_high"  # 90%+
    HIGH = "high"  # 75-89%
    MEDIUM = "medium"  # 50-74%
    LOW = "low"  # 25-49%
    VERY_LOW = "very_low"  # <25%


class UnifiedBettingSignal(UnifiedBaseModel):
    """
    Unified betting signal model consolidating all legacy signal types.

    Provides type-safe, validated structure for all betting signals
    with enhanced metadata and performance tracking.
    """

    # Core Signal Information
    signal_id: str = Field(..., description="Unique signal identifier")
    signal_type: SignalType = Field(..., description="Type of betting signal")
    strategy_category: StrategyCategory = Field(..., description="Strategy category")

    # Game Information
    game_id: str = Field(..., description="Unique game identifier")
    home_team: str = Field(..., description="Home team name")
    away_team: str = Field(..., description="Away team name")
    game_date: datetime = Field(..., description="Game date and time (EST)")

    # Signal Details
    recommended_side: str = Field(..., description="Recommended betting side")
    bet_type: str = Field(..., description="Type of bet (ML, spread, total)")
    confidence_score: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence score (0-1)"
    )
    confidence_level: ConfidenceLevel = Field(
        ..., description="Confidence level category"
    )

    # Financial Information
    recommended_stake: Decimal | None = Field(
        None, description="Recommended stake amount"
    )
    expected_value: float | None = Field(None, description="Expected value calculation")
    odds: float | None = Field(None, description="Odds at signal generation")

    # Strategy-Specific Data
    strategy_data: dict[str, Any] = Field(
        default_factory=dict, description="Strategy-specific data"
    )
    signal_strength: float = Field(
        ..., ge=0.0, le=1.0, description="Raw signal strength"
    )

    # Timing Information
    minutes_to_game: int = Field(..., description="Minutes until game start")
    timing_category: str = Field(
        ..., description="Timing category (ULTRA_LATE, LATE, etc.)"
    )

    # Source Information
    data_source: str = Field(..., description="Primary data source")
    book_sources: list[str] = Field(
        default_factory=list, description="Contributing sportsbooks"
    )

    # Validation and Quality
    validation_passed: bool = Field(
        True, description="Whether signal passed validation"
    )
    quality_score: float = Field(..., ge=0.0, le=1.0, description="Data quality score")

    # Performance Tracking
    strategy_performance_id: str | None = Field(
        None, description="Associated performance record"
    )

    @validator("game_date")
    def validate_game_date(cls, v):
        """Ensure game date is in the future"""
        if v <= datetime.now():
            raise ValueError("Game date must be in the future")
        return v

    @validator("confidence_score")
    def validate_confidence_score(cls, v):
        """Ensure confidence score is reasonable"""
        if not 0.0 <= v <= 1.0:
            raise ValueError("Confidence score must be between 0 and 1")
        return v

    def to_legacy_format(self) -> dict[str, Any]:
        """Convert to legacy BettingSignal format for compatibility"""
        return {
            "signal_id": self.signal_id,
            "signal_type": self.signal_type.value,
            "game_id": self.game_id,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "game_date": self.game_date,
            "recommended_side": self.recommended_side,
            "confidence_score": self.confidence_score,
            "strategy_data": self.strategy_data,
            "created_at": self.created_at,
        }


class UnifiedStrategyData(UnifiedBaseModel):
    """
    Unified strategy data model for cross-strategy analysis.

    Provides standardized structure for strategy-specific data
    with enhanced validation and type safety.
    """

    # Strategy Identification
    strategy_id: str = Field(..., description="Unique strategy identifier")
    strategy_name: str = Field(..., description="Human-readable strategy name")
    strategy_category: StrategyCategory = Field(..., description="Strategy category")

    # Configuration
    configuration: dict[str, Any] = Field(
        default_factory=dict, description="Strategy configuration"
    )
    thresholds: dict[str, float] = Field(
        default_factory=dict, description="Strategy thresholds"
    )

    # Performance Data
    total_signals: int = Field(0, description="Total signals generated")
    successful_signals: int = Field(0, description="Successful signals")
    win_rate: float = Field(0.0, ge=0.0, le=1.0, description="Win rate")
    roi: float = Field(0.0, description="Return on investment")

    # Recent Performance
    recent_performance: list[dict[str, Any]] = Field(
        default_factory=list, description="Recent performance data"
    )

    # Validation Status
    is_active: bool = Field(True, description="Whether strategy is active")
    last_validation: datetime | None = Field(
        None, description="Last validation timestamp"
    )
    validation_errors: list[str] = Field(
        default_factory=list, description="Validation errors"
    )

    def calculate_success_rate(self) -> float:
        """Calculate success rate from performance data"""
        if self.total_signals == 0:
            return 0.0
        return self.successful_signals / self.total_signals


class UnifiedPerformanceMetrics(UnifiedBaseModel):
    """
    Unified performance metrics for comprehensive strategy analysis.

    Provides standardized metrics across all strategies with
    enhanced statistical analysis and comparison capabilities.
    """

    # Basic Performance
    total_bets: int = Field(0, description="Total number of bets")
    winning_bets: int = Field(0, description="Number of winning bets")
    losing_bets: int = Field(0, description="Number of losing bets")
    push_bets: int = Field(0, description="Number of push bets")

    # Financial Metrics
    total_wagered: Decimal = Field(Decimal("0"), description="Total amount wagered")
    total_profit: Decimal = Field(Decimal("0"), description="Total profit/loss")
    roi: float = Field(0.0, description="Return on investment")

    # Statistical Metrics
    win_rate: float = Field(0.0, ge=0.0, le=1.0, description="Win rate")
    average_odds: float = Field(0.0, description="Average odds")
    average_stake: Decimal = Field(Decimal("0"), description="Average stake size")

    # Risk Metrics
    max_drawdown: float = Field(0.0, description="Maximum drawdown")
    sharpe_ratio: float | None = Field(None, description="Sharpe ratio")
    volatility: float = Field(0.0, description="Performance volatility")

    # Time-based Metrics
    performance_period_days: int = Field(0, description="Performance period in days")
    daily_roi: float = Field(0.0, description="Daily ROI")

    # Confidence Metrics
    average_confidence: float = Field(
        0.0, ge=0.0, le=1.0, description="Average confidence score"
    )
    confidence_accuracy: float = Field(
        0.0, description="Confidence calibration accuracy"
    )

    def calculate_kelly_criterion(
        self, win_probability: float, average_odds: float
    ) -> float:
        """Calculate optimal bet size using Kelly Criterion"""
        if average_odds <= 1.0 or win_probability <= 0.0:
            return 0.0

        b = average_odds - 1.0  # Net odds received
        p = win_probability  # Probability of winning
        q = 1.0 - p  # Probability of losing

        kelly_fraction = (b * p - q) / b
        return max(0.0, kelly_fraction)  # Never bet negative

    def calculate_profit_factor(self) -> float:
        """Calculate profit factor (gross profit / gross loss)"""
        if self.losing_bets == 0:
            return float("inf") if self.winning_bets > 0 else 0.0

        # Estimate gross profit and loss from available data
        if self.total_profit > 0:
            gross_profit = abs(self.total_profit) + abs(
                self.total_profit * 0.3
            )  # Estimate
            gross_loss = abs(self.total_profit * 0.3)  # Estimate
            return gross_profit / gross_loss if gross_loss > 0 else float("inf")

        return 0.0


class CrossStrategyComparison(UnifiedBaseModel):
    """
    Cross-strategy comparison model for A/B testing and performance analysis.

    Enables comprehensive comparison of multiple strategies with
    statistical significance testing and performance attribution.
    """

    # Comparison Metadata
    comparison_id: str = Field(..., description="Unique comparison identifier")
    comparison_name: str = Field(..., description="Human-readable comparison name")
    comparison_date: datetime = Field(
        default_factory=datetime.now, description="Comparison date"
    )

    # Strategies Being Compared
    strategies: list[str] = Field(..., description="Strategy IDs being compared")
    strategy_names: list[str] = Field(..., description="Strategy names")

    # Performance Comparison
    performance_metrics: dict[str, UnifiedPerformanceMetrics] = Field(
        default_factory=dict, description="Performance metrics by strategy"
    )

    # Statistical Analysis
    statistical_significance: dict[str, float] = Field(
        default_factory=dict, description="Statistical significance p-values"
    )
    confidence_intervals: dict[str, dict[str, float]] = Field(
        default_factory=dict, description="Confidence intervals for metrics"
    )

    # Rankings
    roi_ranking: list[str] = Field(
        default_factory=list, description="Strategies ranked by ROI"
    )
    win_rate_ranking: list[str] = Field(
        default_factory=list, description="Strategies ranked by win rate"
    )
    sharpe_ranking: list[str] = Field(
        default_factory=list, description="Strategies ranked by Sharpe ratio"
    )

    # Recommendations
    recommended_strategy: str | None = Field(None, description="Recommended strategy")
    recommendation_reason: str | None = Field(
        None, description="Reason for recommendation"
    )

    # Portfolio Analysis
    portfolio_metrics: UnifiedPerformanceMetrics | None = Field(
        None, description="Combined portfolio metrics"
    )
    correlation_matrix: dict[str, dict[str, float]] = Field(
        default_factory=dict, description="Strategy correlation matrix"
    )

    def calculate_portfolio_optimization(self) -> dict[str, float]:
        """Calculate optimal portfolio weights using mean-variance optimization"""
        # Simplified portfolio optimization
        # In a full implementation, this would use modern portfolio theory

        if not self.performance_metrics:
            return {}

        # Equal weight as baseline
        num_strategies = len(self.strategies)
        equal_weight = 1.0 / num_strategies

        # Adjust weights based on Sharpe ratio
        sharpe_ratios = {}
        for strategy_id in self.strategies:
            metrics = self.performance_metrics.get(strategy_id)
            if metrics and metrics.sharpe_ratio:
                sharpe_ratios[strategy_id] = max(0.0, metrics.sharpe_ratio)
            else:
                sharpe_ratios[strategy_id] = 0.0

        total_sharpe = sum(sharpe_ratios.values())

        if total_sharpe == 0:
            return dict.fromkeys(self.strategies, equal_weight)

        # Weight by Sharpe ratio
        optimal_weights = {}
        for strategy_id in self.strategies:
            optimal_weights[strategy_id] = sharpe_ratios[strategy_id] / total_sharpe

        return optimal_weights

    def generate_comparison_report(self) -> dict[str, Any]:
        """Generate comprehensive comparison report"""
        return {
            "comparison_id": self.comparison_id,
            "comparison_name": self.comparison_name,
            "strategies_compared": len(self.strategies),
            "best_roi_strategy": self.roi_ranking[0] if self.roi_ranking else None,
            "best_win_rate_strategy": self.win_rate_ranking[0]
            if self.win_rate_ranking
            else None,
            "recommended_strategy": self.recommended_strategy,
            "recommendation_reason": self.recommendation_reason,
            "portfolio_optimization": self.calculate_portfolio_optimization(),
            "created_at": self.created_at,
        }


# Type aliases for backward compatibility
StrategySignal = UnifiedBettingSignal
StrategyData = UnifiedStrategyData
PerformanceMetrics = UnifiedPerformanceMetrics
