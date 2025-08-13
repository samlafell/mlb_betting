-- =============================================================================
-- CONSOLIDATED MIGRATION: Machine Learning Schema (FIXED)
-- =============================================================================
-- Purpose: Consolidated ML infrastructure including MLflow integration
-- Fixed: PostgreSQL INDEX syntax and dependency issues
-- Date: 2025-08-13 (Fixed version)
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
    CONSTRAINT unique_ml_features UNIQUE (game_id, feature_extraction_date)
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
    arbitrage_opportunities_count INTEGER DEFAULT 0,
    
    -- Movement Analysis
    total_line_movements INTEGER DEFAULT 0,
    significant_movements INTEGER DEFAULT 0,
    reverse_line_movements INTEGER DEFAULT 0,
    steam_moves INTEGER DEFAULT 0,
    
    -- Market Timing
    early_market_activity NUMERIC(4,3),
    late_market_activity NUMERIC(4,3),
    closing_line_value NUMERIC(4,3),
    
    -- Constraints
    CONSTRAINT unique_ml_market_analysis UNIQUE (game_id, analysis_timestamp)
);

-- =============================================================================
-- ANALYTICS ZONE ML TABLES
-- =============================================================================

-- ML Experiments Table (MLflow Integration)
CREATE TABLE IF NOT EXISTS analytics.ml_experiments (
    id BIGSERIAL PRIMARY KEY,
    
    -- MLflow Integration
    mlflow_experiment_id VARCHAR(255) UNIQUE,
    mlflow_run_id VARCHAR(255),
    
    -- Experiment Metadata
    experiment_name VARCHAR(255) NOT NULL,
    model_type VARCHAR(100) NOT NULL,
    algorithm VARCHAR(100),
    hyperparameter_space JSONB,
    
    -- Training Configuration
    training_start_date DATE,
    training_end_date DATE,
    feature_count INTEGER DEFAULT 0,
    training_samples INTEGER DEFAULT 0,
    validation_samples INTEGER DEFAULT 0,
    test_samples INTEGER DEFAULT 0,
    
    -- Performance Metrics
    accuracy NUMERIC(5,4),
    precision_score NUMERIC(5,4),
    recall_score NUMERIC(5,4),
    f1_score NUMERIC(5,4),
    roc_auc NUMERIC(5,4),
    
    -- Business Metrics
    roi NUMERIC(6,4),
    sharpe_ratio NUMERIC(5,4),
    max_drawdown NUMERIC(5,4),
    hit_rate NUMERIC(5,4),
    
    -- Experiment Status
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- MLflow Artifacts
    model_artifact_path VARCHAR(500),
    metrics_artifact_path VARCHAR(500),
    
    -- Constraints
    CONSTRAINT valid_experiment_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
);

-- ML Predictions Table
CREATE TABLE IF NOT EXISTS analytics.ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    
    -- Experiment and Model Info
    experiment_id INTEGER REFERENCES analytics.ml_experiments(id),
    model_version VARCHAR(50),
    
    -- Game Information
    game_id VARCHAR(255) NOT NULL,
    prediction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Predictions
    predicted_outcome VARCHAR(50),
    predicted_probability NUMERIC(5,4),
    confidence_score NUMERIC(5,4),
    expected_value NUMERIC(7,4),
    
    -- Feature Data
    feature_vector JSONB,
    feature_importance JSONB,
    
    -- Actual Outcome (for validation)
    actual_outcome VARCHAR(50),
    outcome_timestamp TIMESTAMPTZ,
    prediction_correct BOOLEAN,
    actual_profit_loss NUMERIC(10,2),
    
    -- Constraints
    CONSTRAINT unique_ml_prediction UNIQUE (experiment_id, game_id, prediction_timestamp)
);

-- =============================================================================
-- MONITORING ZONE ML TABLES
-- =============================================================================

-- ML Model Performance Monitoring
CREATE TABLE IF NOT EXISTS monitoring.ml_model_performance (
    id BIGSERIAL PRIMARY KEY,
    
    -- Model Reference
    experiment_id INTEGER REFERENCES analytics.ml_experiments(id),
    measurement_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Performance Metrics
    accuracy_7d NUMERIC(5,4),
    roi_7d NUMERIC(6,4),
    hit_rate_7d NUMERIC(5,4),
    sharpe_ratio_7d NUMERIC(5,4),
    
    -- Data Quality Metrics
    data_drift_score NUMERIC(5,4),
    feature_drift_score NUMERIC(5,4),
    prediction_drift_score NUMERIC(5,4),
    
    -- Volume Metrics
    predictions_count_7d INTEGER DEFAULT 0,
    profitable_predictions_7d INTEGER DEFAULT 0,
    total_profit_loss_7d NUMERIC(10,2),
    
    -- Constraints
    CONSTRAINT unique_ml_performance UNIQUE (experiment_id, measurement_timestamp)
);

-- ML Model Alerts
CREATE TABLE IF NOT EXISTS monitoring.ml_model_alerts (
    id BIGSERIAL PRIMARY KEY,
    
    -- Alert Information
    experiment_id INTEGER REFERENCES analytics.ml_experiments(id),
    alert_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium',
    message TEXT NOT NULL,
    
    -- Alert Metadata
    triggered_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'active',
    
    -- Alert Context
    trigger_value NUMERIC(10,4),
    threshold_value NUMERIC(10,4),
    context_data JSONB,
    
    -- Constraints
    CONSTRAINT valid_alert_severity CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT valid_alert_status CHECK (status IN ('active', 'acknowledged', 'resolved'))
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- ML Features Indexes
CREATE INDEX IF NOT EXISTS idx_ml_features_game_id ON curated.ml_features (game_id);
CREATE INDEX IF NOT EXISTS idx_ml_features_game_date ON curated.ml_features (game_date);
CREATE INDEX IF NOT EXISTS idx_ml_features_teams ON curated.ml_features (home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_ml_features_mlb_game_id ON curated.ml_features (mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_ml_features_extraction_date ON curated.ml_features (feature_extraction_date);
CREATE INDEX IF NOT EXISTS idx_ml_features_quality ON curated.ml_features (data_quality_score);
CREATE INDEX IF NOT EXISTS idx_ml_features_sharp_action ON curated.ml_features (sharp_money_percentage_home, reverse_line_movement, steam_move_detected);

-- ML Market Analysis Indexes
CREATE INDEX IF NOT EXISTS idx_ml_market_game_id ON curated.ml_market_analysis (game_id);
CREATE INDEX IF NOT EXISTS idx_ml_market_analysis_timestamp ON curated.ml_market_analysis (analysis_timestamp);

-- ML Experiments Indexes
CREATE INDEX IF NOT EXISTS idx_ml_experiments_mlflow_id ON analytics.ml_experiments (mlflow_experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_name ON analytics.ml_experiments (experiment_name);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_status ON analytics.ml_experiments (status);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_roi ON analytics.ml_experiments (roi);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_created ON analytics.ml_experiments (created_at);

-- ML Predictions Indexes
CREATE INDEX IF NOT EXISTS idx_ml_predictions_experiment ON analytics.ml_predictions (experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_game_id ON analytics.ml_predictions (game_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_timestamp ON analytics.ml_predictions (prediction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_outcome ON analytics.ml_predictions (actual_outcome);

-- ML Performance Indexes
CREATE INDEX IF NOT EXISTS idx_ml_performance_experiment ON monitoring.ml_model_performance (experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_performance_timestamp ON monitoring.ml_model_performance (measurement_timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_performance_roi ON monitoring.ml_model_performance (roi_7d);

-- ML Alerts Indexes
CREATE INDEX IF NOT EXISTS idx_ml_alerts_experiment ON monitoring.ml_model_alerts (experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_alerts_status ON monitoring.ml_model_alerts (status);
CREATE INDEX IF NOT EXISTS idx_ml_alerts_severity ON monitoring.ml_model_alerts (severity);
CREATE INDEX IF NOT EXISTS idx_ml_alerts_triggered ON monitoring.ml_model_alerts (triggered_at);

-- =============================================================================
-- VIEWS FOR ML OPERATIONS
-- =============================================================================

-- ML Features Complete View
CREATE OR REPLACE VIEW analytics.ml_features_complete AS
SELECT 
    f.id,
    f.game_id,
    f.mlb_stats_api_game_id,
    f.home_team,
    f.away_team,
    f.game_date,
    f.data_quality_score,
    ma.market_efficiency_score,
    ma.line_consensus_strength,
    f.sharp_money_percentage_home,
    f.reverse_line_movement,
    f.steam_move_detected,
    f.feature_extraction_date
FROM curated.ml_features f
LEFT JOIN curated.ml_market_analysis ma ON f.game_id = ma.game_id
WHERE f.data_quality_score >= 0.7;

-- ML Performance Summary View
CREATE OR REPLACE VIEW monitoring.ml_performance_summary AS
SELECT 
    e.id as experiment_id,
    e.experiment_name,
    e.model_type,
    e.status,
    e.roi,
    e.accuracy,
    p.accuracy_7d,
    p.roi_7d,
    p.predictions_count_7d,
    p.measurement_timestamp as last_measurement
FROM analytics.ml_experiments e
LEFT JOIN LATERAL (
    SELECT * FROM monitoring.ml_model_performance mp 
    WHERE mp.experiment_id = e.id 
    ORDER BY mp.measurement_timestamp DESC 
    LIMIT 1
) p ON true
WHERE e.status IN ('completed', 'running');

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Function to calculate ML feature completeness
CREATE OR REPLACE FUNCTION analytics.calculate_feature_completeness(
    feature_row curated.ml_features
) RETURNS NUMERIC AS $$
DECLARE
    total_features INTEGER := 20; -- Core features count
    non_null_features INTEGER := 0;
BEGIN
    -- Count non-null important features
    IF feature_row.home_team_wins IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.away_team_wins IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.home_starter_era IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.away_starter_era IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.opening_moneyline_home IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.opening_moneyline_away IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.opening_spread IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.opening_total IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.sharp_money_percentage_home IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    IF feature_row.venue_name IS NOT NULL THEN non_null_features := non_null_features + 1; END IF;
    
    -- Add more feature checks as needed
    
    RETURN ROUND(non_null_features::NUMERIC / total_features::NUMERIC, 3);
END;
$$ LANGUAGE plpgsql;

-- Function to update experiment status
CREATE OR REPLACE FUNCTION analytics.update_experiment_status(
    exp_id INTEGER,
    new_status VARCHAR(50)
) RETURNS BOOLEAN AS $$
BEGIN
    UPDATE analytics.ml_experiments 
    SET status = new_status, updated_at = CURRENT_TIMESTAMP
    WHERE id = exp_id;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TRIGGERS FOR AUTOMATED MONITORING
-- =============================================================================

-- Trigger to update experiment timestamp on changes
CREATE OR REPLACE FUNCTION analytics.update_experiment_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_experiment_timestamp
    BEFORE UPDATE ON analytics.ml_experiments
    FOR EACH ROW
    EXECUTE FUNCTION analytics.update_experiment_timestamp();

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA curated IS 'Curated zone for ML-ready data and features';
COMMENT ON SCHEMA analytics IS 'Analytics zone for ML experiments and predictions';
COMMENT ON SCHEMA monitoring IS 'Monitoring zone for ML model performance tracking';

COMMENT ON TABLE curated.ml_features IS 'ML feature store with comprehensive game and market features';
COMMENT ON TABLE curated.ml_market_analysis IS 'Market structure analysis for ML feature engineering';
COMMENT ON TABLE analytics.ml_experiments IS 'MLflow-integrated experiment tracking';
COMMENT ON TABLE analytics.ml_predictions IS 'Model predictions with actual outcomes for validation';
COMMENT ON TABLE monitoring.ml_model_performance IS 'Real-time model performance monitoring';
COMMENT ON TABLE monitoring.ml_model_alerts IS 'Automated alerts for model performance degradation';

-- =============================================================================
-- FINAL VALIDATION
-- =============================================================================

-- Validate that all tables were created successfully
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables 
    WHERE table_schema IN ('curated', 'analytics', 'monitoring')
    AND table_name LIKE '%ml_%';
    
    IF table_count >= 6 THEN
        RAISE NOTICE 'ML schema deployment successful: % tables created', table_count;
    ELSE
        RAISE WARNING 'ML schema deployment may be incomplete: only % tables found', table_count;
    END IF;
END $$;