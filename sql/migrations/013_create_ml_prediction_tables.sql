-- Migration 013: ML Prediction and Performance Tracking Tables
-- Purpose: Complete ML infrastructure with prediction storage and model performance tracking
-- Integrates: MLFlow experiment tracking and betting-specific metrics
-- Date: 2025-07-24

-- ================================
-- Phase 7: ML Predictions Storage
-- ================================

-- ML model predictions with confidence scores and betting recommendations
CREATE TABLE IF NOT EXISTS curated.ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Model identification and experiment tracking
    model_name VARCHAR(100) NOT NULL, -- e.g., 'xgboost_v1', 'neural_network_deep'
    model_version VARCHAR(20) NOT NULL, -- Semantic versioning
    experiment_id VARCHAR(100), -- MLFlow experiment ID
    run_id VARCHAR(100), -- MLFlow run ID for this specific prediction
    model_artifact_path TEXT, -- Path to model artifacts
    
    -- Feature vector reference
    feature_vector_id INTEGER REFERENCES curated.ml_feature_vectors(id),
    feature_version VARCHAR(20) NOT NULL,
    
    -- Prediction targets (binary classification for three main markets)
    -- Total Over/Under Predictions
    total_over_probability DECIMAL(5,4) CHECK (total_over_probability BETWEEN 0 AND 1),
    total_over_binary INTEGER CHECK (total_over_binary IN (0, 1)),
    total_over_confidence DECIMAL(3,2) CHECK (total_over_confidence BETWEEN 0 AND 1),
    
    -- Home Team Moneyline Predictions
    home_ml_probability DECIMAL(5,4) CHECK (home_ml_probability BETWEEN 0 AND 1),
    home_ml_binary INTEGER CHECK (home_ml_binary IN (0, 1)),
    home_ml_confidence DECIMAL(3,2) CHECK (home_ml_confidence BETWEEN 0 AND 1),
    
    -- Home Team Spread Predictions
    home_spread_probability DECIMAL(5,4) CHECK (home_spread_probability BETWEEN 0 AND 1),
    home_spread_binary INTEGER CHECK (home_spread_binary IN (0, 1)),
    home_spread_confidence DECIMAL(3,2) CHECK (home_spread_confidence BETWEEN 0 AND 1),
    
    -- Model explanation and interpretability
    feature_importance JSONB DEFAULT '{}', -- Feature importance scores
    prediction_explanation JSONB DEFAULT '{}', -- SHAP values, LIME explanations, etc.
    model_confidence_factors JSONB DEFAULT '{}', -- What drove model confidence
    
    -- Betting recommendations (Kelly Criterion and Expected Value)
    -- Total Betting Recommendations
    total_expected_value DECIMAL(8,4), -- Expected value calculation
    total_kelly_fraction DECIMAL(5,4), -- Optimal bet size using Kelly Criterion
    total_recommended_bet_size DECIMAL(5,2), -- Percentage of bankroll
    total_min_odds INTEGER, -- Minimum odds needed for positive EV
    
    -- Moneyline Betting Recommendations  
    ml_expected_value DECIMAL(8,4),
    ml_kelly_fraction DECIMAL(5,4),
    ml_recommended_bet_size DECIMAL(5,2),
    ml_min_odds INTEGER,
    
    -- Spread Betting Recommendations
    spread_expected_value DECIMAL(8,4),
    spread_kelly_fraction DECIMAL(5,4), 
    spread_recommended_bet_size DECIMAL(5,2),
    spread_min_odds INTEGER,
    
    -- Risk management
    max_bet_recommendation DECIMAL(5,2), -- Maximum recommended bet regardless of Kelly
    risk_level VARCHAR(10) CHECK (risk_level IN ('low', 'medium', 'high')),
    confidence_threshold_met BOOLEAN DEFAULT FALSE, -- Whether prediction meets confidence threshold
    
    -- Prediction metadata
    prediction_timestamp TIMESTAMPTZ NOT NULL, -- When prediction was made
    market_close_time TIMESTAMPTZ, -- When betting markets close
    time_to_game_minutes INTEGER, -- Minutes from prediction to game start
    
    -- Model performance context
    model_recent_accuracy DECIMAL(5,4), -- Model's recent accuracy rate
    model_recent_roi DECIMAL(5,2), -- Model's recent ROI performance
    similar_game_predictions INTEGER, -- Count of similar predictions by model
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_prediction_timing CHECK (time_to_game_minutes >= 60), -- Ensure 60min cutoff
    CONSTRAINT valid_kelly_fractions CHECK (
        (total_kelly_fraction IS NULL OR total_kelly_fraction BETWEEN 0 AND 0.25) AND
        (ml_kelly_fraction IS NULL OR ml_kelly_fraction BETWEEN 0 AND 0.25) AND  
        (spread_kelly_fraction IS NULL OR spread_kelly_fraction BETWEEN 0 AND 0.25)
    ), -- Kelly criterion should not exceed 25% of bankroll
    CONSTRAINT unique_model_game_prediction UNIQUE (game_id, model_name, model_version, prediction_timestamp)
);

-- ================================
-- Phase 8: Model Performance Tracking
-- ================================

-- Comprehensive model performance tracking with betting-specific metrics
CREATE TABLE IF NOT EXISTS curated.ml_model_performance (
    id BIGSERIAL PRIMARY KEY,
    
    -- Model identification
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    prediction_type VARCHAR(20) NOT NULL CHECK (prediction_type IN ('total_over', 'home_ml', 'home_spread')),
    
    -- Evaluation period
    evaluation_period_start DATE NOT NULL,
    evaluation_period_end DATE NOT NULL,
    total_predictions INTEGER NOT NULL CHECK (total_predictions > 0),
    total_games_evaluated INTEGER NOT NULL,
    
    -- Standard classification metrics
    accuracy DECIMAL(5,4) CHECK (accuracy BETWEEN 0 AND 1),
    precision_score DECIMAL(5,4) CHECK (precision_score BETWEEN 0 AND 1),
    recall_score DECIMAL(5,4) CHECK (recall_score BETWEEN 0 AND 1),
    f1_score DECIMAL(5,4) CHECK (f1_score BETWEEN 0 AND 1),
    roc_auc DECIMAL(5,4) CHECK (roc_auc BETWEEN 0 AND 1),
    log_loss DECIMAL(8,6) CHECK (log_loss >= 0),
    
    -- Betting-specific performance metrics
    total_bets_made INTEGER DEFAULT 0,
    winning_bets INTEGER DEFAULT 0,
    losing_bets INTEGER DEFAULT 0,
    push_bets INTEGER DEFAULT 0, -- Ties/pushes
    hit_rate DECIMAL(5,4) CHECK (hit_rate BETWEEN 0 AND 1), -- Winning percentage
    
    -- Financial performance
    total_amount_wagered DECIMAL(12,2) DEFAULT 0,
    total_amount_won DECIMAL(12,2) DEFAULT 0,
    net_profit_loss DECIMAL(12,2) DEFAULT 0,
    roi_percentage DECIMAL(6,2), -- Return on investment
    
    -- Risk-adjusted returns
    sharpe_ratio DECIMAL(6,3), -- Risk-adjusted return measure
    sortino_ratio DECIMAL(6,3), -- Downside deviation adjusted return
    max_drawdown_amount DECIMAL(12,2), -- Largest peak-to-trough loss
    max_drawdown_pct DECIMAL(5,2) CHECK (max_drawdown_pct >= 0), -- Max drawdown as percentage
    
    -- Kelly Criterion performance analysis
    kelly_theoretical_roi DECIMAL(5,2), -- Theoretical ROI using Kelly sizing
    kelly_actual_roi DECIMAL(5,2), -- Actual ROI achieved
    kelly_sizing_effectiveness DECIMAL(3,2), -- How well Kelly sizing worked
    
    -- Bet sizing analysis
    average_bet_size DECIMAL(5,2), -- Average bet as % of bankroll
    median_bet_size DECIMAL(5,2),
    largest_bet_size DECIMAL(5,2),
    smallest_bet_size DECIMAL(5,2),
    
    -- Market efficiency analysis
    average_closing_line_value DECIMAL(6,2), -- Average CLV (closing line value)
    positive_clv_rate DECIMAL(5,4), -- Percentage of bets with positive CLV
    average_hold_percentage DECIMAL(4,2), -- Average sportsbook hold on bets placed
    
    -- Confidence calibration
    confidence_accuracy_correlation DECIMAL(5,4), -- How well confidence predicts accuracy
    overconfidence_bias DECIMAL(5,4), -- Tendency to be overconfident
    
    -- Market-specific performance
    favorite_vs_underdog_accuracy JSONB DEFAULT '{}', -- Performance breakdown
    home_vs_away_accuracy JSONB DEFAULT '{}',
    over_vs_under_accuracy JSONB DEFAULT '{}',
    
    -- Time-based performance patterns
    performance_by_month JSONB DEFAULT '{}',
    performance_by_day_of_week JSONB DEFAULT '{}',
    early_season_vs_late_season JSONB DEFAULT '{}',
    
    -- Feature importance stability
    top_features JSONB DEFAULT '{}', -- Most important features for this model
    feature_stability_score DECIMAL(3,2), -- How consistent feature importance is
    
    -- Model comparison metrics
    benchmark_accuracy DECIMAL(5,4), -- Accuracy vs simple benchmark (e.g., betting favorites)
    benchmark_roi DECIMAL(5,2), -- ROI vs benchmark strategy
    statistical_significance DECIMAL(6,4), -- P-value for performance vs benchmark
    
    -- MLFlow integration
    mlflow_experiment_id VARCHAR(100),
    mlflow_run_ids TEXT[], -- Array of run IDs included in this evaluation
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_evaluation_period CHECK (evaluation_period_end > evaluation_period_start),
    CONSTRAINT valid_hit_rate CHECK (
        total_bets_made = 0 OR 
        hit_rate = ROUND(winning_bets::DECIMAL / total_bets_made, 4)
    ),
    CONSTRAINT unique_model_evaluation UNIQUE (
        model_name, model_version, prediction_type, 
        evaluation_period_start, evaluation_period_end
    )
);

-- ================================
-- Phase 9: MLFlow Experiment Integration
-- ================================

-- MLFlow experiment tracking integration
CREATE TABLE IF NOT EXISTS curated.ml_experiments (
    id BIGSERIAL PRIMARY KEY,
    
    -- MLFlow experiment identification
    mlflow_experiment_id VARCHAR(100) UNIQUE NOT NULL,
    experiment_name VARCHAR(200) NOT NULL,
    
    -- Experiment metadata
    prediction_target VARCHAR(20) NOT NULL CHECK (prediction_target IN ('total_over', 'home_ml', 'home_spread')),
    experiment_description TEXT,
    experiment_tags JSONB DEFAULT '{}',
    
    -- Model architecture information
    model_type VARCHAR(50) NOT NULL, -- 'logistic_regression', 'xgboost', 'neural_network', etc.
    model_category VARCHAR(20) CHECK (model_category IN ('interpretable', 'blackbox')),
    hyperparameter_space JSONB DEFAULT '{}', -- Hyperparameters being tuned
    
    -- Dataset information
    training_period_start DATE,
    training_period_end DATE,
    validation_period_start DATE,
    validation_period_end DATE,
    feature_version VARCHAR(20),
    
    -- Experiment status
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'completed', 'failed', 'archived')),
    lifecycle_stage VARCHAR(20) DEFAULT 'active' CHECK (lifecycle_stage IN ('active', 'deleted')),
    
    -- Performance tracking
    best_run_id VARCHAR(100),
    best_accuracy DECIMAL(5,4),
    best_roi DECIMAL(5,2),
    total_runs INTEGER DEFAULT 0,
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- Comments and Documentation  
-- ================================

COMMENT ON TABLE curated.ml_predictions IS 'ML model predictions with betting recommendations using Kelly Criterion. Includes confidence scores and model explanations for three binary classification targets.';
COMMENT ON COLUMN curated.ml_predictions.feature_importance IS 'JSONB containing feature importance scores for model interpretability';
COMMENT ON COLUMN curated.ml_predictions.total_kelly_fraction IS 'Optimal bet size using Kelly Criterion for total over/under market';

COMMENT ON TABLE curated.ml_model_performance IS 'Comprehensive model performance tracking with betting-specific metrics. Includes ROI, Sharpe ratio, Kelly Criterion analysis, and market efficiency measures.';
COMMENT ON COLUMN curated.ml_model_performance.average_closing_line_value IS 'Average CLV (Closing Line Value) - key metric for long-term profitability';
COMMENT ON COLUMN curated.ml_model_performance.sharpe_ratio IS 'Risk-adjusted return metric accounting for volatility of returns';

COMMENT ON TABLE curated.ml_experiments IS 'MLFlow experiment tracking integration for managing ML model development lifecycle and hyperparameter optimization.';