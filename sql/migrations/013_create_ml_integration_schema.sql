-- Migration 013: Create ML Integration Schema for MLflow and Prediction System
-- Purpose: Support ML model lifecycle, predictions, and performance tracking
-- Date: 2025-07-31
-- Integration: MLflow, FastAPI, Redis feature store
-- Docker: Connects to existing MLflow container on port 5001

-- ================================
-- Phase 1: ML Models Registry
-- ================================

-- ML model registry table (bridges MLflow with our system)
CREATE TABLE IF NOT EXISTS curated.ml_models (
    id SERIAL PRIMARY KEY,
    
    -- MLflow integration
    mlflow_experiment_id INTEGER NOT NULL,
    mlflow_run_id VARCHAR(100) NOT NULL UNIQUE,
    mlflow_model_uri TEXT NOT NULL,
    
    -- Model metadata
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    model_type VARCHAR(50) NOT NULL CHECK (model_type IN (
        'sharp_action_classifier',
        'game_outcome_predictor', 
        'value_bet_detector',
        'line_movement_predictor',
        'consensus_analyzer'
    )),
    
    -- Model performance metrics (from MLflow)
    performance_metrics JSONB DEFAULT '{}',
    hyperparameters JSONB DEFAULT '{}',
    feature_importance JSONB DEFAULT '{}',
    
    -- Model lifecycle management
    model_status VARCHAR(20) DEFAULT 'training' CHECK (model_status IN (
        'training', 'validation', 'staging', 'production', 'archived', 'failed'
    )),
    deployment_environment VARCHAR(20) DEFAULT 'development' CHECK (deployment_environment IN (
        'development', 'staging', 'production'
    )),
    
    -- Training data information
    training_start_date DATE,
    training_end_date DATE,
    training_samples_count INTEGER,
    validation_accuracy DECIMAL(5,4),
    
    -- Model artifacts and metadata
    model_artifact_path TEXT,
    feature_schema JSONB, -- Schema of expected input features
    prediction_schema JSONB, -- Schema of model predictions
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deployed_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,
    
    -- Constraints
    CONSTRAINT unique_model_version UNIQUE (model_name, model_version),
    CONSTRAINT valid_training_period CHECK (
        training_start_date IS NULL OR training_end_date IS NULL OR 
        training_start_date <= training_end_date
    ),
    CONSTRAINT valid_accuracy CHECK (validation_accuracy BETWEEN 0 AND 1)
);

-- ================================
-- Phase 2: ML Predictions Storage
-- ================================

-- ML predictions table for storing model outputs
CREATE TABLE IF NOT EXISTS curated.ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    
    -- Link to game and model
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    model_id INTEGER NOT NULL REFERENCES curated.ml_models(id) ON DELETE CASCADE,
    
    -- Prediction metadata
    prediction_type VARCHAR(30) NOT NULL CHECK (prediction_type IN (
        'sharp_action_home',
        'sharp_action_away', 
        'sharp_action_over',
        'sharp_action_under',
        'game_outcome_home_win',
        'game_outcome_away_win',
        'total_over',
        'total_under',
        'value_bet_moneyline',
        'value_bet_spread',
        'value_bet_total',
        'line_movement_direction',
        'consensus_strength'
    )),
    
    -- Prediction values
    prediction_value DECIMAL(10,6) NOT NULL, -- Primary prediction (probability, score, etc.)
    confidence_score DECIMAL(5,4) CHECK (confidence_score BETWEEN 0 AND 1),
    probability_score DECIMAL(5,4) CHECK (probability_score BETWEEN 0 AND 1),
    
    -- Additional prediction metadata
    prediction_details JSONB DEFAULT '{}', -- Additional model outputs
    feature_values JSONB DEFAULT '{}', -- Input features used for prediction
    feature_hash VARCHAR(64), -- Hash of features for caching and deduplication
    
    -- Model performance context
    model_confidence_interval DECIMAL(5,4), -- Model's confidence in this prediction
    prediction_explainability JSONB, -- SHAP values or other explainability data
    
    -- Betting context (if applicable)
    recommended_bet_size DECIMAL(10,2), -- Kelly criterion or other sizing
    expected_value DECIMAL(10,4), -- Expected betting value
    odds_at_prediction INTEGER, -- Odds available when prediction was made
    
    -- Prediction timing (critical for ML validation)
    game_start_time TIMESTAMPTZ NOT NULL,
    prediction_made_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    minutes_before_game INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (game_start_time - prediction_made_at)) / 60
    ) STORED,
    
    -- Data quality and validation
    prediction_quality_score DECIMAL(3,2) DEFAULT 1.0,
    is_backtest BOOLEAN DEFAULT FALSE, -- True for historical backtesting
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints for data integrity
    CONSTRAINT valid_ml_prediction_cutoff CHECK (minutes_before_game >= 60), -- 60-minute cutoff
    CONSTRAINT unique_prediction_per_game_model UNIQUE (game_id, model_id, prediction_type),
    CONSTRAINT valid_prediction_timing CHECK (prediction_made_at <= game_start_time)
);

-- ================================
-- Phase 3: ML Experiments Tracking
-- ================================

-- ML experiments table (extends MLflow with business context)
CREATE TABLE IF NOT EXISTS curated.ml_experiments (
    id SERIAL PRIMARY KEY,
    
    -- MLflow integration
    mlflow_experiment_id INTEGER NOT NULL UNIQUE,
    mlflow_experiment_name VARCHAR(200) NOT NULL,
    
    -- Business context
    experiment_description TEXT,
    experiment_objective VARCHAR(500),
    success_criteria JSONB, -- Definition of experiment success
    
    -- Experiment parameters
    data_start_date DATE NOT NULL,
    data_end_date DATE NOT NULL,
    feature_engineering_version VARCHAR(50),
    target_variable VARCHAR(100),
    
    -- Experiment status
    experiment_status VARCHAR(20) DEFAULT 'active' CHECK (experiment_status IN (
        'active', 'completed', 'failed', 'archived'
    )),
    
    -- Results summary (will be updated after models are created)
    best_model_run_id VARCHAR(100), -- MLflow run ID of best model
    best_validation_score DECIMAL(8,6),
    total_runs INTEGER DEFAULT 0,
    successful_runs INTEGER DEFAULT 0,
    
    -- Business impact metrics
    expected_roi DECIMAL(10,4), -- Expected return on investment
    risk_assessment VARCHAR(20) DEFAULT 'medium' CHECK (risk_assessment IN ('low', 'medium', 'high')),
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    
    -- Constraints
    CONSTRAINT valid_experiment_period CHECK (data_start_date <= data_end_date),
    CONSTRAINT valid_validation_score CHECK (best_validation_score BETWEEN 0 AND 1)
);

-- ================================
-- Phase 4: Model Performance Tracking
-- ================================

-- Model performance tracking over time
CREATE TABLE IF NOT EXISTS curated.ml_model_performance (
    id BIGSERIAL PRIMARY KEY,
    
    -- Model and time period
    model_id INTEGER NOT NULL REFERENCES curated.ml_models(id) ON DELETE CASCADE,
    evaluation_period_start TIMESTAMPTZ NOT NULL,
    evaluation_period_end TIMESTAMPTZ NOT NULL,
    
    -- Performance metrics
    accuracy DECIMAL(5,4),
    precision_score DECIMAL(5,4),
    recall_score DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    auc_score DECIMAL(5,4),
    log_loss DECIMAL(10,6),
    
    -- Business metrics
    total_predictions INTEGER NOT NULL DEFAULT 0,
    correct_predictions INTEGER NOT NULL DEFAULT 0,
    profitable_bets INTEGER DEFAULT 0,
    total_bets_placed INTEGER DEFAULT 0,
    net_profit DECIMAL(12,2) DEFAULT 0.0,
    roi_percentage DECIMAL(8,4) DEFAULT 0.0,
    
    -- Model drift detection
    feature_drift_score DECIMAL(5,4), -- Evidently feature drift
    prediction_drift_score DECIMAL(5,4), -- Evidently prediction drift
    data_quality_score DECIMAL(5,4), -- Overall data quality
    
    -- Statistical significance
    confidence_interval_lower DECIMAL(5,4),
    confidence_interval_upper DECIMAL(5,4),
    statistical_significance BOOLEAN DEFAULT FALSE,
    
    -- Comparison with baseline
    baseline_model_run_id VARCHAR(100), -- MLflow run ID of baseline model
    improvement_over_baseline DECIMAL(8,6),
    
    -- Metadata
    evaluation_method VARCHAR(50) DEFAULT 'holdout', -- holdout, cross_validation, time_series
    sample_size INTEGER,
    evaluation_metadata JSONB DEFAULT '{}',
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_evaluation_period CHECK (evaluation_period_start < evaluation_period_end),
    CONSTRAINT valid_accuracy_metrics CHECK (
        accuracy BETWEEN 0 AND 1 AND
        precision_score BETWEEN 0 AND 1 AND
        recall_score BETWEEN 0 AND 1 AND
        f1_score BETWEEN 0 AND 1 AND
        auc_score BETWEEN 0 AND 1
    ),
    CONSTRAINT valid_business_metrics CHECK (
        total_predictions >= correct_predictions AND
        total_bets_placed >= profitable_bets
    )
);

-- ================================
-- Phase 5: Feature Store Integration
-- ================================

-- Feature store metadata (for Redis/Feast integration)
CREATE TABLE IF NOT EXISTS curated.ml_feature_definitions (
    id SERIAL PRIMARY KEY,
    
    -- Feature identification
    feature_name VARCHAR(100) NOT NULL UNIQUE,
    feature_group VARCHAR(50) NOT NULL, -- sharp_action, game_context, market_data, etc.
    feature_type VARCHAR(20) NOT NULL CHECK (feature_type IN (
        'numerical', 'categorical', 'boolean', 'timestamp', 'array', 'json'
    )),
    
    -- Feature metadata
    description TEXT,
    calculation_logic TEXT, -- How the feature is calculated
    data_source VARCHAR(50), -- Source table/view
    update_frequency VARCHAR(20) DEFAULT 'game' CHECK (update_frequency IN (
        'real_time', 'hourly', 'game', 'daily', 'weekly'
    )),
    
    -- Feature quality and validation
    is_active BOOLEAN DEFAULT TRUE,
    quality_score DECIMAL(3,2) DEFAULT 1.0,
    null_percentage DECIMAL(5,2) DEFAULT 0.0,
    
    -- ML pipeline integration
    is_training_feature BOOLEAN DEFAULT TRUE,
    is_inference_feature BOOLEAN DEFAULT TRUE,
    feature_importance_score DECIMAL(8,6), -- Average importance across models
    
    -- Redis/Feast configuration
    redis_ttl_seconds INTEGER DEFAULT 3600, -- 1 hour default
    feast_online_enabled BOOLEAN DEFAULT FALSE,
    feast_offline_enabled BOOLEAN DEFAULT TRUE,
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    
    -- Constraints
    CONSTRAINT valid_quality_metrics CHECK (
        quality_score BETWEEN 0 AND 1 AND
        null_percentage BETWEEN 0 AND 100
    )
);

-- ================================
-- Phase 6: Performance Indexes
-- ================================

-- ML models indexes (using regular CREATE INDEX for migration compatibility)
CREATE INDEX IF NOT EXISTS idx_ml_models_status 
ON curated.ml_models(model_status, deployment_environment);

CREATE INDEX IF NOT EXISTS idx_ml_models_mlflow_run 
ON curated.ml_models(mlflow_run_id);

CREATE INDEX IF NOT EXISTS idx_ml_models_created 
ON curated.ml_models(created_at DESC);

-- ML predictions indexes  
CREATE INDEX IF NOT EXISTS idx_ml_predictions_game_model 
ON curated.ml_predictions(game_id, model_id);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_type_confidence 
ON curated.ml_predictions(prediction_type, confidence_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_timing 
ON curated.ml_predictions(game_start_time, prediction_made_at);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_features_hash 
ON curated.ml_predictions(feature_hash) WHERE feature_hash IS NOT NULL;

-- ML experiments indexes
CREATE INDEX IF NOT EXISTS idx_ml_experiments_status 
ON curated.ml_experiments(experiment_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_experiments_mlflow 
ON curated.ml_experiments(mlflow_experiment_id);

-- Model performance indexes
CREATE INDEX IF NOT EXISTS idx_ml_performance_model_period 
ON curated.ml_model_performance(model_id, evaluation_period_start DESC);

CREATE INDEX IF NOT EXISTS idx_ml_performance_metrics 
ON curated.ml_model_performance(accuracy DESC, roi_percentage DESC);

-- Feature definitions indexes
CREATE INDEX IF NOT EXISTS idx_ml_features_group_active 
ON curated.ml_feature_definitions(feature_group, is_active);

CREATE INDEX IF NOT EXISTS idx_ml_features_importance 
ON curated.ml_feature_definitions(feature_importance_score DESC NULLS LAST);

-- ================================
-- Phase 7: Helper Views
-- ================================

-- Production models view
CREATE OR REPLACE VIEW curated.ml_production_models AS
SELECT 
    m.id,
    m.model_name,
    m.model_version,
    m.model_type,
    m.mlflow_run_id,
    m.validation_accuracy,
    m.deployed_at,
    p.accuracy as current_accuracy,
    p.roi_percentage as current_roi,
    p.total_predictions as total_predictions_30d
FROM curated.ml_models m
LEFT JOIN curated.ml_model_performance p ON (
    m.id = p.model_id 
    AND p.evaluation_period_start > NOW() - INTERVAL '30 days'
)
WHERE m.model_status = 'production'
ORDER BY m.deployed_at DESC;

-- Model performance summary view
CREATE OR REPLACE VIEW curated.ml_model_summary AS
SELECT 
    m.id as model_id,
    m.model_name,
    m.model_type,
    m.model_status,
    COUNT(p.id) as total_predictions,
    AVG(p.confidence_score) as avg_confidence,
    COUNT(CASE WHEN p.prediction_made_at > NOW() - INTERVAL '7 days' THEN 1 END) as predictions_7d,
    COUNT(CASE WHEN p.prediction_made_at > NOW() - INTERVAL '30 days' THEN 1 END) as predictions_30d,
    MAX(p.prediction_made_at) as last_prediction_at,
    m.created_at as model_created_at
FROM curated.ml_models m
LEFT JOIN curated.ml_predictions p ON m.id = p.model_id
GROUP BY m.id, m.model_name, m.model_type, m.model_status, m.created_at
ORDER BY m.created_at DESC;

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE curated.ml_models IS 'ML model registry bridging MLflow with business logic. Tracks model lifecycle, performance, and deployment status.';
COMMENT ON COLUMN curated.ml_models.mlflow_run_id IS 'MLflow run ID for connecting to MLflow tracking server on port 5001';
COMMENT ON COLUMN curated.ml_models.feature_schema IS 'JSON schema defining expected input features for the model';

COMMENT ON TABLE curated.ml_predictions IS 'Model predictions with 60-minute cutoff enforcement. Links games to model outputs with full traceability.';
COMMENT ON COLUMN curated.ml_predictions.minutes_before_game IS 'Calculated minutes before game start - must be >= 60 for ML compliance';
COMMENT ON COLUMN curated.ml_predictions.feature_hash IS 'Hash of input features for Redis caching and deduplication';

COMMENT ON TABLE curated.ml_experiments IS 'ML experiments with business context extending MLflow experiment tracking';
COMMENT ON COLUMN curated.ml_experiments.mlflow_experiment_id IS 'Links to MLflow experiment for detailed run tracking';

COMMENT ON TABLE curated.ml_model_performance IS 'Time-series model performance tracking with business metrics and drift detection';
COMMENT ON COLUMN curated.ml_model_performance.feature_drift_score IS 'Evidently feature drift score for model monitoring';

COMMENT ON TABLE curated.ml_feature_definitions IS 'Feature store metadata for Redis/Feast integration with quality tracking';
COMMENT ON COLUMN curated.ml_feature_definitions.redis_ttl_seconds IS 'Redis cache TTL for feature values';

-- ================================
-- Post-Migration Validation
-- ================================

DO $$
DECLARE
    table_count INTEGER;
    index_count INTEGER;
    view_count INTEGER;
BEGIN
    -- Check tables created
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables 
    WHERE table_schema = 'curated' 
    AND table_name LIKE 'ml_%'
    AND table_type = 'BASE TABLE';
    
    RAISE NOTICE 'Created % ML tables in curated schema', table_count;
    
    -- Check indexes created
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes 
    WHERE schemaname = 'curated'
    AND indexname LIKE 'idx_ml_%';
    
    RAISE NOTICE 'Created % ML indexes in curated schema', index_count;
    
    -- Check views created
    SELECT COUNT(*) INTO view_count
    FROM information_schema.views
    WHERE table_schema = 'curated'
    AND table_name LIKE 'ml_%';
    
    RAISE NOTICE 'Created % ML views in curated schema', view_count;
    
    -- Validate foreign key constraints
    PERFORM 1 FROM curated.ml_models LIMIT 0;
    PERFORM 1 FROM curated.ml_predictions LIMIT 0; 
    PERFORM 1 FROM curated.ml_experiments LIMIT 0;
    PERFORM 1 FROM curated.ml_model_performance LIMIT 0;
    PERFORM 1 FROM curated.ml_feature_definitions LIMIT 0;
    
    RAISE NOTICE 'ML schema validation: All tables accessible';
    
END $$;