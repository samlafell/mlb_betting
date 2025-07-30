"""
Database models for ML prediction system
Maps to existing curated.* tables created by migrations 011-014
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class MLPrediction(BaseModel):
    """Model for curated.ml_predictions table"""

    id: Optional[int] = None
    game_id: int

    # Model identification
    model_name: str
    model_version: str
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    model_artifact_path: Optional[str] = None

    # Feature vector reference
    feature_vector_id: Optional[int] = None
    feature_version: str

    # Total Over/Under Predictions
    total_over_probability: Optional[Decimal] = None
    total_over_binary: Optional[int] = None
    total_over_confidence: Optional[Decimal] = None

    # Home Team Moneyline Predictions
    home_ml_probability: Optional[Decimal] = None
    home_ml_binary: Optional[int] = None
    home_ml_confidence: Optional[Decimal] = None

    # Home Team Spread Predictions
    home_spread_probability: Optional[Decimal] = None
    home_spread_binary: Optional[int] = None
    home_spread_confidence: Optional[Decimal] = None

    # Model explanation
    feature_importance: Dict[str, Any] = Field(default_factory=dict)
    prediction_explanation: Dict[str, Any] = Field(default_factory=dict)
    model_confidence_factors: Dict[str, Any] = Field(default_factory=dict)

    # Betting recommendations
    total_expected_value: Optional[Decimal] = None
    total_kelly_fraction: Optional[Decimal] = None
    total_recommended_bet_size: Optional[Decimal] = None
    total_min_odds: Optional[int] = None

    ml_expected_value: Optional[Decimal] = None
    ml_kelly_fraction: Optional[Decimal] = None
    ml_recommended_bet_size: Optional[Decimal] = None
    ml_min_odds: Optional[int] = None

    spread_expected_value: Optional[Decimal] = None
    spread_kelly_fraction: Optional[Decimal] = None
    spread_recommended_bet_size: Optional[Decimal] = None
    spread_min_odds: Optional[int] = None

    # Risk management
    max_bet_recommendation: Optional[Decimal] = None
    risk_level: Optional[str] = None
    confidence_threshold_met: bool = False

    # Metadata
    prediction_timestamp: datetime
    market_close_time: Optional[datetime] = None
    time_to_game_minutes: Optional[int] = None

    # Model performance context
    model_recent_accuracy: Optional[Decimal] = None
    model_recent_roi: Optional[Decimal] = None
    similar_game_predictions: Optional[int] = None

    # Timestamps
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MLModelPerformance(BaseModel):
    """Model for curated.ml_model_performance table"""

    id: Optional[int] = None

    # Model identification
    model_name: str
    model_version: str
    prediction_type: str  # 'total_over', 'home_ml', 'home_spread'

    # Evaluation period
    evaluation_period_start: date
    evaluation_period_end: date
    total_predictions: int
    total_games_evaluated: int

    # Classification metrics
    accuracy: Optional[Decimal] = None
    precision_score: Optional[Decimal] = None
    recall_score: Optional[Decimal] = None
    f1_score: Optional[Decimal] = None
    roc_auc: Optional[Decimal] = None
    log_loss: Optional[Decimal] = None

    # Betting performance
    total_bets_made: int = 0
    winning_bets: int = 0
    losing_bets: int = 0
    push_bets: int = 0
    hit_rate: Optional[Decimal] = None

    # Financial performance
    total_amount_wagered: Decimal = Decimal("0.00")
    total_amount_won: Decimal = Decimal("0.00")
    net_profit_loss: Decimal = Decimal("0.00")
    roi_percentage: Optional[Decimal] = None

    # Risk metrics
    sharpe_ratio: Optional[Decimal] = None
    sortino_ratio: Optional[Decimal] = None
    max_drawdown_amount: Optional[Decimal] = None
    max_drawdown_pct: Optional[Decimal] = None

    # Kelly Criterion analysis
    kelly_theoretical_roi: Optional[Decimal] = None
    kelly_actual_roi: Optional[Decimal] = None
    kelly_sizing_effectiveness: Optional[Decimal] = None

    # Bet sizing
    average_bet_size: Optional[Decimal] = None
    median_bet_size: Optional[Decimal] = None
    largest_bet_size: Optional[Decimal] = None
    smallest_bet_size: Optional[Decimal] = None

    # Market analysis
    average_closing_line_value: Optional[Decimal] = None
    positive_clv_rate: Optional[Decimal] = None
    average_hold_percentage: Optional[Decimal] = None

    # Performance patterns
    favorite_vs_underdog_accuracy: Dict[str, Any] = Field(default_factory=dict)
    home_vs_away_accuracy: Dict[str, Any] = Field(default_factory=dict)
    over_vs_under_accuracy: Dict[str, Any] = Field(default_factory=dict)
    performance_by_month: Dict[str, Any] = Field(default_factory=dict)
    performance_by_day_of_week: Dict[str, Any] = Field(default_factory=dict)

    # Feature analysis
    top_features: Dict[str, Any] = Field(default_factory=dict)
    feature_stability_score: Optional[Decimal] = None

    # Benchmarking
    benchmark_accuracy: Optional[Decimal] = None
    benchmark_roi: Optional[Decimal] = None
    statistical_significance: Optional[Decimal] = None

    # MLflow integration
    mlflow_experiment_id: Optional[str] = None
    mlflow_run_ids: List[str] = Field(default_factory=list)

    created_at: datetime

    class Config:
        from_attributes = True


class MLExperiment(BaseModel):
    """Model for curated.ml_experiments table"""

    id: Optional[int] = None

    # MLflow identification
    mlflow_experiment_id: str
    experiment_name: str

    # Experiment metadata
    prediction_target: str  # 'total_over', 'home_ml', 'home_spread'
    experiment_description: Optional[str] = None
    experiment_tags: Dict[str, Any] = Field(default_factory=dict)

    # Model architecture
    model_type: str  # 'logistic_regression', 'xgboost', 'neural_network'
    model_category: Optional[str] = None  # 'interpretable', 'blackbox'
    hyperparameter_space: Dict[str, Any] = Field(default_factory=dict)

    # Dataset information
    training_period_start: Optional[date] = None
    training_period_end: Optional[date] = None
    validation_period_start: Optional[date] = None
    validation_period_end: Optional[date] = None
    feature_version: Optional[str] = None

    # Status
    status: str = "active"  # 'active', 'completed', 'failed', 'archived'
    lifecycle_stage: str = "active"  # 'active', 'deleted'

    # Performance tracking
    best_run_id: Optional[str] = None
    best_accuracy: Optional[Decimal] = None
    best_roi: Optional[Decimal] = None
    total_runs: int = 0

    # Timestamps
    created_at: datetime
    last_updated: datetime

    class Config:
        from_attributes = True


class EnhancedGame(BaseModel):
    """Model for curated.enhanced_games table"""

    id: Optional[int] = None

    # Cross-system identifiers
    mlb_stats_api_game_id: Optional[str] = None
    action_network_game_id: Optional[int] = None
    sbd_game_id: Optional[str] = None
    vsin_game_key: Optional[str] = None

    # Game data
    home_team: str
    away_team: str
    home_team_full_name: Optional[str] = None
    away_team_full_name: Optional[str] = None

    # Scheduling
    game_date: date
    game_time: Optional[datetime] = None
    game_datetime: datetime
    season: int
    season_type: str = "regular"

    # Classification
    game_type: str = "regular"
    game_number: int = 1

    # Venue
    venue_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_timezone: Optional[str] = None

    # Weather
    temperature_fahrenheit: Optional[int] = None
    wind_speed_mph: Optional[int] = None
    wind_direction: Optional[str] = None
    humidity_pct: Optional[int] = None
    weather_condition: Optional[str] = None

    class Config:
        from_attributes = True
