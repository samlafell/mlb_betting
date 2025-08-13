"""
Feature Engineering Data Models with Pydantic V2
Defines the structure and validation for ML features
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from decimal import Decimal
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field, ConfigDict, validator, field_validator

# Graceful fallback for polars dependency
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

# Add src to path for imports
# Removed sys.path.append() for proper import structure


class BaseFeatureModel(BaseModel):
    """Base model for all feature classes with optimized config"""

    model_config = ConfigDict(
        # Performance optimizations
        validate_assignment=True,
        use_enum_values=True,
        arbitrary_types_allowed=True,
        # JSON serialization config
        json_encoders={
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v),
            timedelta: lambda v: v.total_seconds(),
        },
    )


class TemporalFeatures(BaseFeatureModel):
    """
    Temporal features with 60-minute cutoff enforcement
    Line movement patterns and sharp action indicators
    """

    # Feature cutoff enforcement
    feature_cutoff_time: datetime = Field(
        description="Exactly 60min before first pitch"
    )
    game_start_time: datetime = Field(description="Game start time")
    minutes_before_game: int = Field(ge=60, description="Must be >= 60 minutes")

    # Line movement features
    line_movement_velocity_60min: Optional[Decimal] = Field(
        None, description="Rate of odds changes in final hour"
    )
    opening_to_current_ml_home: Optional[Decimal] = Field(
        None, description="Home ML movement from open"
    )
    opening_to_current_ml_away: Optional[Decimal] = Field(
        None, description="Away ML movement from open"
    )
    opening_to_current_spread_home: Optional[Decimal] = Field(
        None, description="Spread movement"
    )
    opening_to_current_total: Optional[Decimal] = Field(
        None, description="Total line movement"
    )

    # Movement patterns
    ml_movement_direction: Optional[str] = Field(
        None, pattern="^(toward_home|toward_away|stable)$"
    )
    spread_movement_direction: Optional[str] = Field(
        None, pattern="^(toward_home|toward_away|stable)$"
    )
    total_movement_direction: Optional[str] = Field(
        None, pattern="^(toward_over|toward_under|stable)$"
    )
    movement_consistency_score: Optional[Decimal] = Field(None, ge=0, le=1)

    # Sharp action synthesis
    sharp_action_intensity_60min: Optional[Decimal] = Field(
        None, ge=0, description="Aggregated sharp action strength"
    )
    reverse_line_movement_signals: int = Field(
        0, ge=0, description="Count of RLM instances"
    )
    steam_move_count: int = Field(0, ge=0, description="Cross-book simultaneous moves")

    # Public vs sharp divergence
    money_vs_bet_divergence_home: Optional[Decimal] = Field(
        None, description="Money% - Bet% for home team"
    )
    money_vs_bet_divergence_away: Optional[Decimal] = Field(
        None, description="Money% - Bet% for away team"
    )
    money_vs_bet_divergence_over: Optional[Decimal] = Field(
        None, description="Money% - Bet% for over"
    )
    money_vs_bet_divergence_under: Optional[Decimal] = Field(
        None, description="Money% - Bet% for under"
    )

    # Cross-sportsbook consensus
    cross_sbook_consensus_60min: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Agreement across sportsbooks"
    )
    sportsbook_variance_ml: Optional[Decimal] = Field(
        None, ge=0, description="Variance in ML odds"
    )
    sportsbook_variance_spread: Optional[Decimal] = Field(
        None, ge=0, description="Variance in spread"
    )
    sportsbook_variance_total: Optional[Decimal] = Field(
        None, ge=0, description="Variance in total"
    )
    participating_sportsbooks: int = Field(
        0, ge=0, description="Number of books with data"
    )

    # Public sentiment shift
    public_sentiment_shift_60min: Optional[Decimal] = Field(
        None, description="Change in public betting direction"
    )

    # Source-specific features
    dk_money_vs_bet_gap: Optional[Decimal] = Field(
        None, description="VSIN DraftKings specific"
    )
    circa_money_vs_bet_gap: Optional[Decimal] = Field(
        None, description="VSIN Circa specific"
    )

    # Feature versioning
    feature_version: str = Field("v2.1", description="Feature version")
    feature_hash: Optional[str] = Field(None, description="SHA-256 hash for caching")

    @field_validator("minutes_before_game")
    @classmethod
    def validate_ml_cutoff(cls, v: int) -> int:
        if v < 60:
            raise ValueError(
                "ML data leakage prevention: must be >= 60 minutes before game"
            )
        return v


class MarketFeatures(BaseFeatureModel):
    """
    Market structure and efficiency features
    Steam moves, arbitrage opportunities, consensus metrics
    """

    # Market efficiency metrics
    closing_line_efficiency: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Historical accuracy of final pre-game lines"
    )
    market_liquidity_score: Optional[Decimal] = Field(
        None, ge=0, description="Volume and depth indicators"
    )
    line_stability_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="How stable lines were"
    )

    # Steam move detection
    steam_move_indicators: int = Field(
        0, ge=0, description="Count of detected steam moves"
    )
    steam_move_magnitude: Optional[Decimal] = Field(
        None, ge=0, description="Average magnitude of steam moves"
    )
    largest_steam_move: Optional[Decimal] = Field(
        None, ge=0, description="Biggest single move"
    )
    steam_move_sportsbooks: List[str] = Field(
        default_factory=list, description="Books that participated"
    )

    # Arbitrage opportunities
    max_ml_arbitrage_opportunity: Optional[Decimal] = Field(
        None, ge=0, description="Best ML arbitrage found"
    )
    max_spread_arbitrage_opportunity: Optional[Decimal] = Field(
        None, ge=0, description="Best spread arbitrage"
    )
    max_total_arbitrage_opportunity: Optional[Decimal] = Field(
        None, ge=0, description="Best total arbitrage"
    )
    arbitrage_duration_minutes: Optional[int] = Field(
        None, ge=0, description="How long arbitrage lasted"
    )

    # Sportsbook coverage
    participating_sportsbooks: List[str] = Field(
        default_factory=list, description="All books with data"
    )
    sportsbook_count: int = Field(
        0, ge=0, description="Number of participating sportsbooks"
    )
    sportsbook_consensus_strength: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Agreement level"
    )

    # Market depth indicators
    best_ml_spread: Optional[int] = Field(
        None, description="Difference between best home/away ML odds"
    )
    best_total_spread: Optional[int] = Field(
        None, description="Difference between best over/under odds"
    )
    odds_efficiency_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="How efficient odds pricing appears"
    )

    # Line movement patterns
    total_line_movements: int = Field(
        0, ge=0, description="Count of significant line changes"
    )
    average_movement_magnitude: Optional[Decimal] = Field(
        None, ge=0, description="Average size of movements"
    )
    movement_frequency: Optional[Decimal] = Field(
        None, ge=0, description="Movements per hour"
    )
    late_movement_indicator: bool = Field(
        False, description="Significant moves in final hour"
    )

    # Sharp vs public indicators
    sharp_public_divergence_ml: Optional[Decimal] = Field(
        None, description="Difference in ML preferences"
    )
    sharp_public_divergence_spread: Optional[Decimal] = Field(
        None, description="Difference in spread preferences"
    )
    sharp_public_divergence_total: Optional[Decimal] = Field(
        None, description="Difference in total preferences"
    )

    # Market microstructure
    bid_ask_spread_estimate: Optional[Decimal] = Field(
        None, ge=0, description="Estimated transaction costs"
    )
    market_maker_vs_flow: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Market maker advantage indicator"
    )

    # Feature metadata
    feature_version: str = Field("v2.1", description="Feature version")
    calculation_timestamp: datetime = Field(default_factory=datetime.utcnow)


class TeamFeatures(BaseFeatureModel):
    """
    Team performance features with MLB Stats API enrichment
    Recent form, head-to-head, pitcher matchups, venue factors
    """

    # Recent form metrics
    home_recent_form_weighted: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Last 10 games with decay weighting"
    )
    away_recent_form_weighted: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Last 10 games with decay weighting"
    )
    home_last_5_record: Optional[str] = Field(
        None, pattern=r"^\d-\d$", description="e.g., '3-2'"
    )
    away_last_5_record: Optional[str] = Field(
        None, pattern=r"^\d-\d$", description="e.g., '3-2'"
    )
    home_last_10_record: Optional[str] = Field(
        None, pattern=r"^\d{1,2}-\d{1,2}$", description="e.g., '7-3'"
    )
    away_last_10_record: Optional[str] = Field(
        None, pattern=r"^\d{1,2}-\d{1,2}$", description="e.g., '7-3'"
    )

    # Head-to-head historical performance
    h2h_home_wins_last_10: int = Field(
        0, ge=0, le=10, description="H2H wins for home team"
    )
    h2h_away_wins_last_10: int = Field(
        0, ge=0, le=10, description="H2H wins for away team"
    )
    h2h_total_games: int = Field(0, ge=0, description="Total H2H games in sample")
    h2h_avg_total_runs: Optional[Decimal] = Field(
        None, ge=0, description="Average total runs in H2H matchups"
    )
    h2h_home_advantage: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Home field advantage in this matchup"
    )

    # Season performance metrics
    home_season_record: Optional[str] = Field(
        None, pattern=r"^\d{1,3}-\d{1,3}$", description="e.g., '45-32'"
    )
    away_season_record: Optional[str] = Field(
        None, pattern=r"^\d{1,3}-\d{1,3}$", description="e.g., '45-32'"
    )
    home_win_pct: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Home team win percentage"
    )
    away_win_pct: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Away team win percentage"
    )
    home_runs_per_game: Optional[Decimal] = Field(
        None, ge=0, description="Home runs per game"
    )
    away_runs_per_game: Optional[Decimal] = Field(
        None, ge=0, description="Away runs per game"
    )
    home_runs_allowed_per_game: Optional[Decimal] = Field(
        None, ge=0, description="Home runs allowed per game"
    )
    away_runs_allowed_per_game: Optional[Decimal] = Field(
        None, ge=0, description="Away runs allowed per game"
    )

    # Pitcher-specific features
    home_pitcher_season_era: Optional[Decimal] = Field(
        None, ge=0, description="Home pitcher season ERA"
    )
    away_pitcher_season_era: Optional[Decimal] = Field(
        None, ge=0, description="Away pitcher season ERA"
    )
    home_pitcher_whip: Optional[Decimal] = Field(
        None, ge=0, description="Walks + Hits per Inning Pitched"
    )
    away_pitcher_whip: Optional[Decimal] = Field(
        None, ge=0, description="Walks + Hits per Inning Pitched"
    )
    home_pitcher_k9: Optional[Decimal] = Field(
        None, ge=0, description="Strikeouts per 9 innings"
    )
    away_pitcher_k9: Optional[Decimal] = Field(
        None, ge=0, description="Strikeouts per 9 innings"
    )
    home_pitcher_hr9: Optional[Decimal] = Field(
        None, ge=0, description="Home runs per 9 innings"
    )
    away_pitcher_hr9: Optional[Decimal] = Field(
        None, ge=0, description="Home runs per 9 innings"
    )

    # Pitcher vs opposing team history
    home_pitcher_vs_opponent_era: Optional[Decimal] = Field(
        None, ge=0, description="ERA against this specific opponent"
    )
    away_pitcher_vs_opponent_era: Optional[Decimal] = Field(
        None, ge=0, description="ERA against this specific opponent"
    )
    home_pitcher_opponent_games: int = Field(
        0, ge=0, description="Games against this opponent"
    )
    away_pitcher_opponent_games: int = Field(
        0, ge=0, description="Games against this opponent"
    )

    # Bullpen factors
    home_bullpen_era: Optional[Decimal] = Field(
        None, ge=0, description="Home bullpen ERA"
    )
    away_bullpen_era: Optional[Decimal] = Field(
        None, ge=0, description="Away bullpen ERA"
    )
    home_bullpen_recent_usage: Optional[Decimal] = Field(
        None, ge=0, description="Innings pitched last 3 days"
    )
    away_bullpen_recent_usage: Optional[Decimal] = Field(
        None, ge=0, description="Innings pitched last 3 days"
    )
    home_bullpen_fatigue_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="0-1 fatigue indicator"
    )
    away_bullpen_fatigue_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="0-1 fatigue indicator"
    )

    # Venue-specific performance
    home_field_advantage_factor: Optional[Decimal] = Field(
        None, description="Historical home field advantage"
    )
    venue_total_factor: Optional[Decimal] = Field(
        None, description="Venue impact on over/under"
    )
    venue_home_team_factor: Optional[Decimal] = Field(
        None, description="How well home team plays at venue"
    )
    venue_away_team_factor: Optional[Decimal] = Field(
        None, description="How well away team plays at this venue"
    )

    # Weather impact factors
    temperature_impact_total: Optional[Decimal] = Field(
        None, description="Expected impact on total runs"
    )
    wind_impact_total: Optional[Decimal] = Field(
        None, description="Wind impact on over/under"
    )
    weather_advantage_home: Optional[Decimal] = Field(
        None, description="Weather favor for home team"
    )
    weather_advantage_away: Optional[Decimal] = Field(
        None, description="Weather favor for away team"
    )

    # Rest and travel factors
    home_days_rest: int = Field(0, ge=0, description="Days since last game")
    away_days_rest: int = Field(0, ge=0, description="Days since last game")
    away_travel_distance: Optional[int] = Field(
        None, ge=0, description="Miles traveled for away team"
    )
    away_timezone_change: Optional[int] = Field(
        None, description="Hours of timezone change"
    )

    # Lineup and injury factors
    home_key_players_out: int = Field(
        0, ge=0, description="Count of key injured players"
    )
    away_key_players_out: int = Field(
        0, ge=0, description="Count of key injured players"
    )
    home_lineup_strength: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Estimated lineup strength"
    )
    away_lineup_strength: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Estimated lineup strength"
    )

    # Situational factors
    home_motivation_factor: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Playoff race, rivalry, etc."
    )
    away_motivation_factor: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Playoff race, rivalry, etc."
    )
    game_importance_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Overall game importance"
    )

    # Feature metadata
    feature_version: str = Field("v2.1", description="Feature version")
    mlb_api_last_updated: Optional[datetime] = Field(
        None, description="When MLB data was last refreshed"
    )


class BettingSplitsFeatures(BaseFeatureModel):
    """
    Betting splits features from unified multi-source data
    Sharp action indicators, public sentiment, cross-book analysis
    """

    # Data source attribution
    data_sources: List[str] = Field(
        default_factory=list, description="Contributing data sources"
    )
    sportsbook_coverage: List[str] = Field(
        default_factory=list, description="Covered sportsbooks"
    )

    # Moneyline and spread betting splits aggregates
    avg_bet_percentage_home: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average bet % for home team"
    )
    avg_bet_percentage_away: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average bet % for away team"
    )
    avg_money_percentage_home: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average money % for home team"
    )
    avg_money_percentage_away: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average money % for away team"
    )

    # Totals betting splits aggregates
    avg_bet_percentage_over: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average bet % for over"
    )
    avg_bet_percentage_under: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average bet % for under"
    )
    avg_money_percentage_over: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average money % for over"
    )
    avg_money_percentage_under: Optional[Decimal] = Field(
        None, ge=0, le=100, description="Average money % for under"
    )

    # Sharp action aggregated indicators
    sharp_action_signals: int = Field(
        0, ge=0, description="Count of sharp action signals detected"
    )
    sharp_action_strength: Optional[str] = Field(
        None,
        pattern="^(weak|moderate|strong)$",
        description="Overall sharp action strength",
    )
    reverse_line_movement_count: int = Field(
        0, ge=0, description="Count of RLM instances across sources"
    )

    # Public vs sharp divergence calculations
    home_money_bet_divergence: Optional[Decimal] = Field(
        None, description="Home money % - bet % divergence"
    )
    away_money_bet_divergence: Optional[Decimal] = Field(
        None, description="Away money % - bet % divergence"
    )
    over_money_bet_divergence: Optional[Decimal] = Field(
        None, description="Over money % - bet % divergence"
    )
    under_money_bet_divergence: Optional[Decimal] = Field(
        None, description="Under money % - bet % divergence"
    )

    # Cross-sportsbook variance and consensus
    sportsbook_consensus_ml: Optional[Decimal] = Field(
        None, ge=0, le=1, description="ML consensus across books"
    )
    sportsbook_consensus_spread: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Spread consensus across books"
    )
    sportsbook_consensus_total: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Total consensus across books"
    )

    # Variance metrics
    bet_percentage_variance_home: Optional[Decimal] = Field(
        None, ge=0, description="Variance in home bet %"
    )
    money_percentage_variance_home: Optional[Decimal] = Field(
        None, ge=0, description="Variance in home money %"
    )
    bet_percentage_variance_over: Optional[Decimal] = Field(
        None, ge=0, description="Variance in over bet %"
    )
    money_percentage_variance_over: Optional[Decimal] = Field(
        None, ge=0, description="Variance in over money %"
    )

    # Weighted averages (by sportsbook importance/volume)
    weighted_sharp_action_score: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Volume-weighted sharp action"
    )
    weighted_public_sentiment: Optional[Decimal] = Field(
        None, ge=0, le=1, description="Volume-weighted public sentiment"
    )

    # Source-specific highlights
    vsin_sharp_signals: int = Field(0, ge=0, description="VSIN-specific sharp signals")
    sbd_consensus_strength: Optional[Decimal] = Field(
        None, ge=0, le=1, description="SBD multi-book consensus"
    )
    action_network_steam_signals: int = Field(
        0, ge=0, description="Action Network steam move signals"
    )

    # Feature metadata
    feature_version: str = Field("v2.1", description="Feature version")
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class FeatureVector(BaseFeatureModel):
    """
    Consolidated feature vector for ML model input
    Combines all feature types with data quality metrics
    """

    # Game identification
    game_id: int = Field(description="Reference to curated.enhanced_games.id")
    feature_cutoff_time: datetime = Field(description="60min before first pitch")
    feature_version: str = Field("v2.1", description="Feature version")
    feature_hash: Optional[str] = Field(None, description="SHA-256 hash for caching")

    # Feature components
    temporal_features: Optional[TemporalFeatures] = Field(
        None, description="Temporal features"
    )
    market_features: Optional[MarketFeatures] = Field(
        None, description="Market features"
    )
    team_features: Optional[TeamFeatures] = Field(None, description="Team features")
    betting_splits_features: Optional[BettingSplitsFeatures] = Field(
        None, description="Betting splits features"
    )

    # Additional computed features
    derived_features: Dict[str, Union[float, int, bool, str]] = Field(
        default_factory=dict, description="Derived features"
    )
    interaction_features: Dict[str, Union[float, int, bool, str]] = Field(
        default_factory=dict, description="Feature interactions"
    )

    # Data quality and completeness metrics
    feature_completeness_score: Decimal = Field(
        Decimal("0.0"), ge=0, le=1, description="0-1 completeness"
    )
    data_source_coverage: int = Field(
        0, ge=0, le=4, description="How many sources contributed"
    )
    missing_feature_count: int = Field(0, ge=0, description="Count of missing features")
    total_feature_count: int = Field(0, ge=0, description="Total feature count")

    # Source attribution
    action_network_data: bool = Field(False, description="Action Network data present")
    vsin_data: bool = Field(False, description="VSIN data present")
    sbd_data: bool = Field(False, description="SBD data present")
    mlb_stats_api_data: bool = Field(False, description="MLB Stats API data present")

    # Pipeline metadata
    normalization_applied: bool = Field(
        False, description="Whether normalization was applied"
    )
    scaling_method: Optional[str] = Field(
        None, pattern="^(standard|minmax|robust)$", description="Scaling method used"
    )
    feature_selection_applied: bool = Field(
        False, description="Whether feature selection was applied"
    )
    dimensionality_reduction: Optional[str] = Field(
        None, pattern="^(pca|lda)$", description="Dimensionality reduction method"
    )

    # Temporal tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)

    def to_model_input(self) -> Dict[str, Any]:
        """Convert to dictionary for ML model input"""
        features = {}

        # Extract features from each component
        if self.temporal_features:
            features.update(
                self._extract_numeric_features(self.temporal_features.model_dump())
            )
        if self.market_features:
            features.update(
                self._extract_numeric_features(self.market_features.model_dump())
            )
        if self.team_features:
            features.update(
                self._extract_numeric_features(self.team_features.model_dump())
            )
        if self.betting_splits_features:
            features.update(
                self._extract_numeric_features(
                    self.betting_splits_features.model_dump()
                )
            )

        # Add derived and interaction features
        features.update(self.derived_features)
        features.update(self.interaction_features)

        return features

    def _extract_numeric_features(
        self, feature_dict: Dict[str, Any]
    ) -> Dict[str, Union[float, int]]:
        """Extract numeric features for ML model input"""
        numeric_features = {}

        for key, value in feature_dict.items():
            if isinstance(value, (int, float, Decimal)):
                numeric_features[key] = (
                    float(value) if isinstance(value, Decimal) else value
                )
            elif isinstance(value, bool):
                numeric_features[key] = int(value)

        return numeric_features


class BaseFeatureExtractor(ABC):
    """
    Abstract base class for feature extractors
    Defines common interface for all feature extraction classes
    """

    def __init__(self, feature_version: str = "v2.1"):
        self.feature_version = feature_version

    @abstractmethod
    async def extract_features(
        self, df, game_id: int, cutoff_time: datetime
    ) -> Any:
        """
        Extract features from data DataFrame

        Args:
            df: Polars DataFrame with source data (if polars available, otherwise dict)
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time (60min before game)

        Returns:
            Feature model instance
        """
        pass

    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        Get list of required columns from source data

        Returns:
            List of required column names
        """
        pass

    def validate_data_quality(
        self, df, required_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Validate data quality for feature extraction

        Args:
            df: Source data DataFrame (polars DataFrame if available, otherwise dict)
            required_columns: Required columns for extraction

        Returns:
            Data quality metrics
        """
        # Check if polars is available and if df is a polars DataFrame
        if not POLARS_AVAILABLE or pl is None:
            return {
                "is_valid": False,
                "missing_columns": required_columns,
                "completeness_score": 0.0,
                "row_count": 0,
                "error": "Polars not available"
            }
        
        if df.is_empty():
            return {
                "is_valid": False,
                "missing_columns": required_columns,
                "completeness_score": 0.0,
                "row_count": 0,
            }

        available_columns = df.columns
        missing_columns = [
            col for col in required_columns if col not in available_columns
        ]

        # Calculate completeness score
        completeness_score = 1.0 - (len(missing_columns) / len(required_columns))

        # Calculate data completeness within available columns
        if available_columns:
            null_counts = df.select(
                [
                    pl.col(col).is_null().sum().alias(f"{col}_nulls")
                    for col in available_columns
                    if col in required_columns
                ]
            )
            total_cells = df.height * len(
                [col for col in required_columns if col in available_columns]
            )
            null_cells = sum(null_counts.row(0)) if null_counts.height > 0 else 0
            data_completeness = (
                1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0
            )
            completeness_score *= data_completeness

        return {
            "is_valid": len(missing_columns) == 0,
            "missing_columns": missing_columns,
            "completeness_score": completeness_score,
            "row_count": df.height,
            "available_columns": available_columns,
        }
