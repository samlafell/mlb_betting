-- Migration 021: ML Performance Optimization Indexes
-- Purpose: Add critical performance indexes for ML operations based on senior engineer feedback
-- Critical: Index (game_id, timestamp) combinations for query performance
-- Date: 2025-01-30

-- ================================
-- Critical Performance Indexes (game_id, timestamp combinations)
-- ================================

-- ML Feature Vectors - Critical for feature pipeline queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_feature_vectors_game_id_cutoff 
ON curated.ml_feature_vectors(game_id, feature_cutoff_time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_feature_vectors_game_id_created 
ON curated.ml_feature_vectors(game_id, created_at);

-- ML Predictions - Critical for prediction queries and monitoring
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_game_id_timestamp 
ON curated.ml_predictions(game_id, prediction_timestamp);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_game_id_created 
ON curated.ml_predictions(game_id, created_at);

-- ML Temporal Features - Critical for temporal analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_temporal_features_game_id_cutoff 
ON curated.ml_temporal_features(game_id, feature_cutoff_time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_temporal_features_game_id_created 
ON curated.ml_temporal_features(game_id, created_at);

-- ML Market Features - Critical for market analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_market_features_game_id_calc_time 
ON curated.ml_market_features(game_id, calculation_timestamp);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_market_features_game_id_created 
ON curated.ml_market_features(game_id, created_at);

-- ML Team Features - Critical for team-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_team_features_game_id_created 
ON curated.ml_team_features(game_id, created_at);

-- ================================
-- Enhanced Game-Based Indexes for ML Operations
-- ================================

-- Enhanced Games - Add timestamp-based indexes for ML pipeline
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_id_datetime 
ON curated.enhanced_games(id, game_datetime);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_id_created 
ON curated.enhanced_games(id, created_at);

-- Unified Betting Splits - Critical for ML feature extraction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_unified_betting_splits_game_id_collected 
ON curated.unified_betting_splits(game_id, collected_at);

-- Action Network Historical - Critical for temporal features
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_historical_game_id_updated 
ON staging.action_network_odds_historical(external_game_id, updated_at);

-- ================================
-- Model Performance Monitoring Indexes
-- ================================

-- ML Model Performance - Critical for monitoring dashboards
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_model_performance_created_accuracy 
ON curated.ml_model_performance(created_at DESC, accuracy DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_model_performance_model_created 
ON curated.ml_model_performance(model_name, model_version, created_at DESC);

-- ML Experiments - For tracking experiment performance over time
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_experiments_created_status 
ON curated.ml_experiments(created_at DESC, status);

-- ================================
-- Feature Engineering Pipeline Indexes
-- ================================

-- Feature vectors by completeness and recency for ML pipeline
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_feature_vectors_completeness_created 
ON curated.ml_feature_vectors(feature_completeness_score DESC, created_at DESC) 
WHERE feature_completeness_score >= 0.7;

-- Feature vectors by version and game for feature comparison
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_feature_vectors_version_game_cutoff 
ON curated.ml_feature_vectors(feature_version, game_id, feature_cutoff_time);

-- ================================
-- Real-time Prediction Indexes
-- ================================

-- Active predictions by confidence for real-time filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_confidence_created 
ON curated.ml_predictions(confidence_threshold_met, created_at DESC) 
WHERE confidence_threshold_met = true;

-- Recent predictions by model for monitoring
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_model_recent 
ON curated.ml_predictions(model_name, model_version, prediction_timestamp DESC);

-- ================================
-- Data Quality and Monitoring Indexes
-- ================================

-- Feature data quality tracking
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_feature_vectors_data_coverage 
ON curated.ml_feature_vectors(data_source_coverage DESC, created_at DESC);

-- Model drift detection support
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_model_target_timestamp 
ON curated.ml_predictions(model_name, prediction_target, prediction_timestamp DESC);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON INDEX idx_ml_feature_vectors_game_id_cutoff IS 'Critical index for ML feature pipeline queries by game and cutoff time';
COMMENT ON INDEX idx_ml_predictions_game_id_timestamp IS 'Critical index for prediction queries and monitoring by game and timestamp';
COMMENT ON INDEX idx_ml_temporal_features_game_id_cutoff IS 'Critical index for temporal feature extraction by game and cutoff time';
COMMENT ON INDEX idx_ml_market_features_game_id_calc_time IS 'Critical index for market feature calculations by game and calculation time';
COMMENT ON INDEX idx_ml_feature_vectors_completeness_created IS 'Optimization index for high-quality feature vectors for ML training';
COMMENT ON INDEX idx_ml_predictions_confidence_created IS 'Real-time index for high-confidence predictions';