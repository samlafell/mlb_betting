-- =============================================================================
-- CONSOLIDATED MIGRATION: Machine Learning Schema
-- =============================================================================
-- Purpose: Consolidated ML infrastructure including MLflow integration
-- Replaces: 011, 012 (both), 013 (both), 014, 021, 022, 023
-- Date: 2025-08-12 (Consolidation)
-- Integrates: MLflow, feature store, predictions, monitoring
-- =============================================================================

-- Ensure required schemas exist
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- CURATED ZONE ML TABLES
-- =============================================================================

-- ML Feature Store Table
CREATE TABLE IF NOT EXISTS curated.ml_features (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game Information
    game_id VARCHAR(255) NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_date DATE NOT NULL,
    game_start_time TIMESTAMPTZ,
    
    -- Team Performance Features
    home_team_wins INTEGER DEFAULT 0,
    home_team_losses INTEGER DEFAULT 0,
    away_team_wins INTEGER DEFAULT 0,
    away_team_losses INTEGER DEFAULT 0,
    home_team_run_differential INTEGER DEFAULT 0,
    away_team_run_differential INTEGER DEFAULT 0,
    
    -- Pitching Features
    home_starter_era NUMERIC(4,2),
    away_starter_era NUMERIC(4,2),
    home_starter_whip NUMERIC(4,2),
    away_starter_whip NUMERIC(4,2),
    home_bullpen_era NUMERIC(4,2),
    away_bullpen_era NUMERIC(4,2),
    
    -- Offensive Features
    home_team_ops NUMERIC(4,3),
    away_team_ops NUMERIC(4,3),
    home_team_runs_per_game NUMERIC(4,2),
    away_team_runs_per_game NUMERIC(4,2),
    
    -- Market Features
    opening_moneyline_home INTEGER,
    opening_moneyline_away INTEGER,
    opening_spread NUMERIC(4,2),
    opening_total NUMERIC(4,2),
    
    -- Sharp Action Features
    sharp_money_percentage_home NUMERIC(5,2),
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move_detected BOOLEAN DEFAULT FALSE,
    line_movement_magnitude NUMERIC(4,2),
    
    -- Weather and Venue Features
    venue_name VARCHAR(200),
    weather_conditions VARCHAR(100),
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction VARCHAR(20),
    
    -- Feature Engineering Metadata
    feature_extraction_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_quality_score NUMERIC(4,3) DEFAULT 1.0,
    missing_features_count INTEGER DEFAULT 0,
    
    -- Constraints
    CONSTRAINT valid_teams_ml_features CHECK (home_team != away_team),
    CONSTRAINT valid_quality_score_ml_features CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0),
    CONSTRAINT unique_ml_features UNIQUE (game_id, feature_extraction_date),
    
    -- Indexes for ML performance
    INDEX idx_ml_features_game_id (game_id),
    INDEX idx_ml_features_game_date (game_date),
    INDEX idx_ml_features_teams (home_team, away_team),
    INDEX idx_ml_features_mlb_game_id (mlb_stats_api_game_id),
    INDEX idx_ml_features_extraction_date (feature_extraction_date),
    INDEX idx_ml_features_quality (data_quality_score),
    INDEX idx_ml_features_sharp_action (sharp_money_percentage_home, reverse_line_movement, steam_move_detected)
);

-- ML Market Analysis Table
CREATE TABLE IF NOT EXISTS curated.ml_market_analysis (
    id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL,
    analysis_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Market Structure Analysis
    sportsbook_count INTEGER DEFAULT 0,
    line_consensus_strength NUMERIC(4,3),
    market_efficiency_score NUMERIC(4,3),
    arbitrage_opportunities INTEGER DEFAULT 0,
    
    -- Line Movement Analysis
    total_line_movements INTEGER DEFAULT 0,
    significant_movements INTEGER DEFAULT 0,
    line_movement_velocity NUMERIC(6,2),
    movement_direction_consensus NUMERIC(4,3),
    
    -- Volume and Activity Metrics
    betting_volume_estimate BIGINT,
    public_money_percentage NUMERIC(5,2),
    sharp_money_percentage NUMERIC(5,2),
    handle_distribution JSONB,
    
    -- Steam and RLM Detection
    steam_moves JSONB, -- Array of steam move events
    reverse_line_movements JSONB, -- Array of RLM events
    late_money_indicators JSONB,
    
    -- Market Timing Features
    time_to_game_hours NUMERIC(6,2),
    market_maturity_score NUMERIC(4,3),
    liquidity_score NUMERIC(4,3),
    
    INDEX idx_ml_market_game_id (game_id),
    INDEX idx_ml_market_timestamp (analysis_timestamp),
    INDEX idx_ml_market_efficiency (market_efficiency_score),
    INDEX idx_ml_market_consensus (line_consensus_strength),
    INDEX idx_ml_market_movements (total_line_movements, significant_movements)
);

-- =============================================================================
-- ANALYTICS ZONE ML TABLES
-- =============================================================================

-- ML Experiments Table (MLflow Integration)
CREATE TABLE IF NOT EXISTS analytics.ml_experiments (
    id BIGSERIAL PRIMARY KEY,
    mlflow_experiment_id VARCHAR(50) UNIQUE,
    experiment_name VARCHAR(200) NOT NULL,
    experiment_type VARCHAR(50) NOT NULL, -- classification, regression, ensemble
    
    -- Model Details
    model_framework VARCHAR(50), -- sklearn, xgboost, lightgbm, pytorch
    model_algorithm VARCHAR(100),
    hyperparameters JSONB,
    
    -- Training Details
    training_start_date DATE,
    training_end_date DATE,
    training_games_count INTEGER,
    validation_games_count INTEGER,
    test_games_count INTEGER,
    
    -- Performance Metrics
    accuracy NUMERIC(6,4),
    precision_score NUMERIC(6,4),
    recall_score NUMERIC(6,4),
    f1_score NUMERIC(6,4),
    roc_auc NUMERIC(6,4),
    log_loss NUMERIC(8,6),
    
    -- Betting Performance
    roi_percentage NUMERIC(6,2),
    win_rate NUMERIC(6,4),
    profit_loss NUMERIC(12,2),
    max_drawdown NUMERIC(6,2),
    sharpe_ratio NUMERIC(6,4),
    
    -- Model Lifecycle
    model_status VARCHAR(20) DEFAULT 'training', -- training, validation, production, retired
    deployment_date TIMESTAMPTZ,
    retirement_date TIMESTAMPTZ,
    
    -- Metadata
    created_by VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_model_status CHECK (model_status IN ('training', 'validation', 'production', 'retired')),
    INDEX idx_ml_experiments_mlflow_id (mlflow_experiment_id),
    INDEX idx_ml_experiments_name (experiment_name),
    INDEX idx_ml_experiments_type (experiment_type),
    INDEX idx_ml_experiments_status (model_status),
    INDEX idx_ml_experiments_performance (roi_percentage, win_rate)
);

-- ML Predictions Table
CREATE TABLE IF NOT EXISTS analytics.ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    
    -- Model and Experiment Reference
    experiment_id BIGINT REFERENCES analytics.ml_experiments(id),
    mlflow_run_id VARCHAR(50),
    model_version VARCHAR(20),
    
    -- Game Information
    game_id VARCHAR(255) NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    prediction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    game_start_time TIMESTAMPTZ,
    
    -- Predictions
    predicted_outcome VARCHAR(20), -- home_win, away_win, over, under
    win_probability NUMERIC(6,4), -- 0.0 to 1.0
    confidence_score NUMERIC(6,4), -- Model confidence in prediction
    
    -- Betting Recommendations
    recommended_bet VARCHAR(50),
    recommended_stake NUMERIC(8,2),
    expected_value NUMERIC(8,4),
    kelly_criterion_stake NUMERIC(6,4),
    
    -- Feature Importance
    top_features JSONB, -- Top contributing features for this prediction
    feature_values JSONB, -- Feature values used for this prediction
    
    -- Model Performance Tracking
    actual_outcome VARCHAR(20),
    prediction_correct BOOLEAN,
    bet_result NUMERIC(8,2), -- Actual profit/loss if bet was placed
    
    -- Constraints
    CONSTRAINT valid_predicted_outcome CHECK (predicted_outcome IN ('home_win', 'away_win', 'over', 'under', 'no_bet')),
    CONSTRAINT valid_probabilities CHECK (win_probability >= 0.0 AND win_probability <= 1.0 AND confidence_score >= 0.0 AND confidence_score <= 1.0),
    
    INDEX idx_ml_predictions_experiment (experiment_id),
    INDEX idx_ml_predictions_game_id (game_id),
    INDEX idx_ml_predictions_timestamp (prediction_timestamp),
    INDEX idx_ml_predictions_outcome (predicted_outcome),
    INDEX idx_ml_predictions_confidence (confidence_score),
    INDEX idx_ml_predictions_ev (expected_value),
    INDEX idx_ml_predictions_mlflow_run (mlflow_run_id)
);

-- =============================================================================
-- MONITORING ZONE ML TABLES
-- =============================================================================

-- ML Model Performance Monitoring
CREATE TABLE IF NOT EXISTS monitoring.ml_model_performance (
    id BIGSERIAL PRIMARY KEY,
    
    -- Model Reference
    experiment_id BIGINT REFERENCES analytics.ml_experiments(id),
    model_version VARCHAR(20),
    monitoring_date DATE DEFAULT CURRENT_DATE,
    
    -- Performance Metrics (Daily)
    daily_predictions INTEGER DEFAULT 0,
    daily_correct_predictions INTEGER DEFAULT 0,
    daily_accuracy NUMERIC(6,4),
    daily_roi NUMERIC(8,2),
    daily_profit_loss NUMERIC(12,2),
    
    -- Rolling Performance (7-day, 30-day)
    rolling_7d_accuracy NUMERIC(6,4),
    rolling_7d_roi NUMERIC(8,2),
    rolling_30d_accuracy NUMERIC(6,4),
    rolling_30d_roi NUMERIC(8,2),
    
    -- Model Drift Detection
    feature_drift_score NUMERIC(6,4),
    prediction_drift_score NUMERIC(6,4),
    data_quality_drift NUMERIC(6,4),
    
    -- Alert Flags
    performance_alert BOOLEAN DEFAULT FALSE,
    drift_alert BOOLEAN DEFAULT FALSE,
    data_quality_alert BOOLEAN DEFAULT FALSE,
    
    -- MLflow Integration
    mlflow_run_id VARCHAR(50),
    mlflow_metrics JSONB,
    
    INDEX idx_ml_performance_experiment (experiment_id),
    INDEX idx_ml_performance_date (monitoring_date),
    INDEX idx_ml_performance_accuracy (daily_accuracy, rolling_7d_accuracy),
    INDEX idx_ml_performance_roi (daily_roi, rolling_30d_roi),
    INDEX idx_ml_performance_alerts (performance_alert, drift_alert, data_quality_alert)
);

-- ML Model Alerts
CREATE TABLE IF NOT EXISTS monitoring.ml_model_alerts (
    id BIGSERIAL PRIMARY KEY,
    
    -- Alert Details
    experiment_id BIGINT REFERENCES analytics.ml_experiments(id),
    alert_type VARCHAR(50) NOT NULL, -- performance_degradation, drift_detected, data_quality
    alert_severity VARCHAR(20) DEFAULT 'medium', -- low, medium, high, critical
    alert_message TEXT NOT NULL,
    
    -- Alert Metrics
    trigger_value NUMERIC(10,4),
    threshold_value NUMERIC(10,4),
    
    -- Alert Lifecycle
    alert_status VARCHAR(20) DEFAULT 'active', -- active, acknowledged, resolved, false_positive
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100),
    resolution_notes TEXT,
    
    CONSTRAINT valid_alert_severity CHECK (alert_severity IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT valid_alert_status CHECK (alert_status IN ('active', 'acknowledged', 'resolved', 'false_positive')),
    
    INDEX idx_ml_alerts_experiment (experiment_id),
    INDEX idx_ml_alerts_type (alert_type),
    INDEX idx_ml_alerts_severity (alert_severity),
    INDEX idx_ml_alerts_status (alert_status),
    INDEX idx_ml_alerts_created (created_at)
);

-- =============================================================================
-- ML VIEWS AND FUNCTIONS
-- =============================================================================

-- Unified ML Feature View
CREATE OR REPLACE VIEW analytics.ml_features_complete AS
SELECT 
    f.*,
    ma.market_efficiency_score,
    ma.line_consensus_strength,
    ma.sharp_money_percentage,
    ma.betting_volume_estimate,
    p.win_probability as latest_prediction_probability,
    p.confidence_score as latest_prediction_confidence
FROM curated.ml_features f
LEFT JOIN curated.ml_market_analysis ma ON f.game_id = ma.game_id
LEFT JOIN analytics.ml_predictions p ON f.game_id = p.game_id
WHERE p.prediction_timestamp = (
    SELECT MAX(prediction_timestamp) 
    FROM analytics.ml_predictions p2 
    WHERE p2.game_id = f.game_id
);

-- Model Performance Summary View
CREATE OR REPLACE VIEW monitoring.ml_performance_summary AS
SELECT 
    e.experiment_name,
    e.model_status,
    e.roi_percentage as training_roi,
    mp.daily_accuracy,
    mp.rolling_30d_accuracy,
    mp.rolling_30d_roi,
    mp.performance_alert,
    mp.monitoring_date
FROM analytics.ml_experiments e
JOIN monitoring.ml_model_performance mp ON e.id = mp.experiment_id
WHERE mp.monitoring_date = CURRENT_DATE;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA analytics IS 'Analytics zone for ML experiments, predictions, and model management';
COMMENT ON SCHEMA monitoring IS 'Monitoring zone for ML model performance tracking and alerting';

COMMENT ON TABLE curated.ml_features IS 'Comprehensive ML feature store with team performance, market, and environmental features';
COMMENT ON TABLE curated.ml_market_analysis IS 'Market structure analysis and line movement features for ML models';

COMMENT ON TABLE analytics.ml_experiments IS 'ML experiment tracking with MLflow integration and betting performance metrics';
COMMENT ON TABLE analytics.ml_predictions IS 'ML model predictions with betting recommendations and performance tracking';

COMMENT ON TABLE monitoring.ml_model_performance IS 'Daily ML model performance monitoring with drift detection';
COMMENT ON TABLE monitoring.ml_model_alerts IS 'ML model alerting system for performance degradation and data drift';

COMMENT ON VIEW analytics.ml_features_complete IS 'Complete ML feature view combining features, market analysis, and latest predictions';
COMMENT ON VIEW monitoring.ml_performance_summary IS 'Summary view of current ML model performance across all active experiments';