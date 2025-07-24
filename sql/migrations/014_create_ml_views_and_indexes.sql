-- Migration 014: ML Views and Performance Optimization
-- Purpose: Create unified views and comprehensive indexes for ML CURATED zone
-- Optimizes: Query performance and multi-source data integration
-- Date: 2025-07-24

-- ================================
-- Phase 10: Unified Integration Views
-- ================================

-- Unified line movements view combining all data sources
CREATE OR REPLACE VIEW curated.unified_line_movements AS
-- Action Network historical odds data
SELECT 
    g.id as game_id,
    g.home_team,
    g.away_team,
    g.game_datetime,
    'action_network' as data_source,
    h.market_type,
    h.side,
    h.odds,
    h.line_value,
    h.updated_at as movement_timestamp,
    h.sportsbook_name,
    h.sportsbook_external_id,
    NULL::DECIMAL(5,2) as bet_percentage,
    NULL::DECIMAL(5,2) as money_percentage,
    NULL::VARCHAR(10) as sharp_action_direction,
    h.is_current_odds,
    EXTRACT(EPOCH FROM (g.game_datetime - h.updated_at)) / 60 as minutes_before_game
FROM curated.enhanced_games g
JOIN staging.action_network_odds_historical h ON g.action_network_game_id = h.external_game_id::INTEGER
WHERE h.updated_at IS NOT NULL

UNION ALL

-- VSIN betting splits with implied line movements
SELECT 
    vs.game_id,
    g.home_team,
    g.away_team,
    g.game_datetime,
    'vsin' as data_source,
    vs.market_type,
    CASE 
        WHEN vs.market_type = 'moneyline' AND vs.bet_percentage_home IS NOT NULL THEN 'home'
        WHEN vs.market_type = 'moneyline' AND vs.bet_percentage_away IS NOT NULL THEN 'away'
        WHEN vs.market_type = 'total' AND vs.bet_percentage_over IS NOT NULL THEN 'over'
        WHEN vs.market_type = 'total' AND vs.bet_percentage_under IS NOT NULL THEN 'under'
        ELSE 'home'
    END as side,
    vs.current_home_ml as odds,
    vs.current_total_line as line_value,
    vs.collected_at as movement_timestamp,
    vs.sportsbook_name,
    vs.sportsbook_external_id,
    COALESCE(vs.bet_percentage_home, vs.bet_percentage_over) as bet_percentage,
    COALESCE(vs.money_percentage_home, vs.money_percentage_over) as money_percentage,
    vs.sharp_action_direction,
    TRUE as is_current_odds, -- VSIN data is typically current
    vs.minutes_before_game
FROM curated.unified_betting_splits vs
JOIN curated.enhanced_games g ON vs.game_id = g.id
WHERE vs.data_source = 'vsin'

UNION ALL

-- SBD betting splits with line data
SELECT 
    vs.game_id,
    g.home_team,
    g.away_team, 
    g.game_datetime,
    'sbd' as data_source,
    vs.market_type,
    CASE 
        WHEN vs.market_type = 'moneyline' AND vs.bet_percentage_home IS NOT NULL THEN 'home'
        WHEN vs.market_type = 'moneyline' AND vs.bet_percentage_away IS NOT NULL THEN 'away'
        WHEN vs.market_type = 'total' AND vs.bet_percentage_over IS NOT NULL THEN 'over'
        WHEN vs.market_type = 'total' AND vs.bet_percentage_under IS NOT NULL THEN 'under'
        ELSE 'home'
    END as side,
    COALESCE(vs.current_home_ml, vs.current_away_ml) as odds,
    vs.current_total_line as line_value,
    vs.collected_at as movement_timestamp,
    vs.sportsbook_name,
    vs.sportsbook_external_id,
    COALESCE(vs.bet_percentage_home, vs.bet_percentage_away, vs.bet_percentage_over) as bet_percentage,
    COALESCE(vs.money_percentage_home, vs.money_percentage_away, vs.money_percentage_over) as money_percentage,
    vs.sharp_action_direction,
    TRUE as is_current_odds,
    vs.minutes_before_game
FROM curated.unified_betting_splits vs
JOIN curated.enhanced_games g ON vs.game_id = g.id
WHERE vs.data_source = 'sbd';

-- ML feature summary view for quick model input preparation  
CREATE OR REPLACE VIEW curated.ml_feature_summary AS
SELECT 
    g.id as game_id,
    g.home_team,
    g.away_team,
    g.game_datetime,
    g.game_status,
    
    -- Outcome variables for model training
    CASE WHEN g.total_runs IS NOT NULL THEN g.total_runs ELSE NULL END as actual_total_runs,
    CASE WHEN g.winning_team = g.home_team THEN 1 WHEN g.winning_team = g.away_team THEN 0 ELSE NULL END as home_team_won,
    
    -- Feature availability indicators
    tf.id IS NOT NULL as has_temporal_features,
    mf.id IS NOT NULL as has_market_features,
    tmf.id IS NOT NULL as has_team_features,
    fv.id IS NOT NULL as has_feature_vector,
    
    -- Data source coverage
    g.source_coverage_score,
    g.mlb_correlation_confidence,
    
    -- Latest feature versions
    tf.feature_version as temporal_feature_version,
    mf.feature_version as market_feature_version,
    tmf.feature_version as team_feature_version,
    fv.feature_version as vector_feature_version,
    
    -- Feature completeness scores
    fv.feature_completeness_score,
    fv.data_source_coverage,
    
    -- Quick access to key metrics
    tf.sharp_action_intensity_60min,
    tf.cross_sbook_consensus_60min,
    mf.closing_line_efficiency,
    tmf.home_recent_form_weighted,
    tmf.away_recent_form_weighted,
    
    -- Prediction readiness
    (tf.id IS NOT NULL AND mf.id IS NOT NULL AND tmf.id IS NOT NULL AND fv.feature_completeness_score >= 0.7) as prediction_ready
    
FROM curated.enhanced_games g
LEFT JOIN curated.ml_temporal_features tf ON g.id = tf.game_id
LEFT JOIN curated.ml_market_features mf ON g.id = mf.game_id  
LEFT JOIN curated.ml_team_features tmf ON g.id = tmf.game_id
LEFT JOIN curated.ml_feature_vectors fv ON g.id = fv.game_id;

-- Model performance dashboard view
CREATE OR REPLACE VIEW curated.ml_model_dashboard AS
SELECT 
    mp.model_name,
    mp.model_version,
    mp.prediction_type,
    
    -- Recent performance (last 30 days)
    COUNT(CASE WHEN mp.created_at >= NOW() - INTERVAL '30 days' THEN 1 END) as predictions_last_30d,
    ROUND(AVG(CASE WHEN mp.created_at >= NOW() - INTERVAL '30 days' THEN mp.accuracy END), 4) as accuracy_last_30d,
    ROUND(AVG(CASE WHEN mp.created_at >= NOW() - INTERVAL '30 days' THEN mp.roi_percentage END), 2) as roi_last_30d,
    
    -- Overall performance
    COUNT(*) as total_predictions,
    ROUND(AVG(mp.accuracy), 4) as overall_accuracy,
    ROUND(AVG(mp.roi_percentage), 2) as overall_roi,
    ROUND(AVG(mp.sharpe_ratio), 3) as overall_sharpe_ratio,
    
    -- Best performance metrics
    MAX(mp.accuracy) as best_accuracy,
    MAX(mp.roi_percentage) as best_roi,
    MIN(mp.max_drawdown_pct) as worst_drawdown,
    
    -- Activity metrics
    MIN(mp.evaluation_period_start) as first_evaluation,
    MAX(mp.evaluation_period_end) as latest_evaluation,
    
    -- Model ranking (ROI-based)
    RANK() OVER (PARTITION BY mp.prediction_type ORDER BY AVG(mp.roi_percentage) DESC) as roi_ranking
    
FROM curated.ml_model_performance mp
GROUP BY mp.model_name, mp.model_version, mp.prediction_type;

-- Data quality monitoring view for multi-source validation
CREATE OR REPLACE VIEW curated.data_quality_monitoring AS
SELECT 
    -- Time period
    DATE(g.created_at) as data_date,
    
    -- Game coverage
    COUNT(*) as total_games,
    COUNT(g.mlb_stats_api_game_id) as games_with_mlb_id,
    COUNT(g.action_network_game_id) as games_with_action_network,
    COUNT(g.sbd_game_id) as games_with_sbd,
    COUNT(g.vsin_game_key) as games_with_vsin,
    
    -- Data source coverage percentages
    ROUND(COUNT(g.mlb_stats_api_game_id) * 100.0 / COUNT(*), 2) as mlb_coverage_pct,
    ROUND(COUNT(g.action_network_game_id) * 100.0 / COUNT(*), 2) as action_network_coverage_pct,
    ROUND(COUNT(g.sbd_game_id) * 100.0 / COUNT(*), 2) as sbd_coverage_pct,
    ROUND(COUNT(g.vsin_game_key) * 100.0 / COUNT(*), 2) as vsin_coverage_pct,
    
    -- Feature availability
    COUNT(tf.id) as games_with_temporal_features,
    COUNT(mf.id) as games_with_market_features,
    COUNT(tmf.id) as games_with_team_features,
    COUNT(fv.id) as games_with_feature_vectors,
    
    -- Feature completeness
    ROUND(AVG(g.source_coverage_score), 3) as avg_source_coverage,
    ROUND(AVG(fv.feature_completeness_score), 3) as avg_feature_completeness,
    ROUND(AVG(g.data_quality_score), 3) as avg_data_quality,
    
    -- Betting splits coverage
    COUNT(bs.id) as games_with_betting_splits,
    COUNT(DISTINCT bs.sportsbook_name) as distinct_sportsbooks,
    ROUND(AVG(bs.data_completeness_score), 3) as avg_splits_completeness,
    
    -- ML readiness
    COUNT(CASE WHEN fv.feature_completeness_score >= 0.7 THEN 1 END) as ml_ready_games,
    ROUND(COUNT(CASE WHEN fv.feature_completeness_score >= 0.7 THEN 1 END) * 100.0 / COUNT(*), 2) as ml_ready_pct
    
FROM curated.enhanced_games g
LEFT JOIN curated.ml_temporal_features tf ON g.id = tf.game_id
LEFT JOIN curated.ml_market_features mf ON g.id = mf.game_id
LEFT JOIN curated.ml_team_features tmf ON g.id = tmf.game_id
LEFT JOIN curated.ml_feature_vectors fv ON g.id = fv.game_id
LEFT JOIN curated.unified_betting_splits bs ON g.id = bs.game_id
WHERE g.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(g.created_at)
ORDER BY data_date DESC;

-- ================================
-- Phase 11: Comprehensive Performance Indexes
-- ================================

-- Enhanced Games Table Indexes
CREATE INDEX IF NOT EXISTS idx_enhanced_games_mlb_api_id ON curated.enhanced_games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_action_network_id ON curated.enhanced_games(action_network_game_id);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_game_datetime ON curated.enhanced_games(game_datetime);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_teams ON curated.enhanced_games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_season ON curated.enhanced_games(season, season_type);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_status ON curated.enhanced_games(game_status);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_date ON curated.enhanced_games(game_date);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_enhanced_games_season_teams ON curated.enhanced_games(season, home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_datetime_status ON curated.enhanced_games(game_datetime, game_status);

-- JSONB indexes for feature data
CREATE INDEX IF NOT EXISTS idx_enhanced_games_feature_data_gin ON curated.enhanced_games USING GIN(feature_data);
CREATE INDEX IF NOT EXISTS idx_enhanced_games_ml_metadata_gin ON curated.enhanced_games USING GIN(ml_metadata);

-- Unified Betting Splits Indexes
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_game_id ON curated.unified_betting_splits(game_id);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_source ON curated.unified_betting_splits(data_source);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_sportsbook ON curated.unified_betting_splits(sportsbook_name);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_market ON curated.unified_betting_splits(market_type);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_collected ON curated.unified_betting_splits(collected_at);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_minutes_before ON curated.unified_betting_splits(minutes_before_game);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_sharp_action ON curated.unified_betting_splits(sharp_action_direction, sharp_action_strength);

-- Composite indexes for complex queries
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_game_market ON curated.unified_betting_splits(game_id, market_type);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_source_market ON curated.unified_betting_splits(data_source, market_type);
CREATE INDEX IF NOT EXISTS idx_unified_betting_splits_game_cutoff ON curated.unified_betting_splits(game_id, minutes_before_game) WHERE minutes_before_game >= 60;

-- ML Temporal Features Indexes
CREATE INDEX IF NOT EXISTS idx_ml_temporal_features_game_id ON curated.ml_temporal_features(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_temporal_features_cutoff_time ON curated.ml_temporal_features(feature_cutoff_time);
CREATE INDEX IF NOT EXISTS idx_ml_temporal_features_version ON curated.ml_temporal_features(feature_version);
CREATE INDEX IF NOT EXISTS idx_ml_temporal_features_minutes_before ON curated.ml_temporal_features(minutes_before_game);

-- ML Market Features Indexes  
CREATE INDEX IF NOT EXISTS idx_ml_market_features_game_id ON curated.ml_market_features(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_market_features_version ON curated.ml_market_features(feature_version);
CREATE INDEX IF NOT EXISTS idx_ml_market_features_timestamp ON curated.ml_market_features(calculation_timestamp);

-- ML Team Features Indexes
CREATE INDEX IF NOT EXISTS idx_ml_team_features_game_id ON curated.ml_team_features(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_team_features_version ON curated.ml_team_features(feature_version);
CREATE INDEX IF NOT EXISTS idx_ml_team_features_mlb_updated ON curated.ml_team_features(mlb_api_last_updated);

-- ML Feature Vectors Indexes
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_game_id ON curated.ml_feature_vectors(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_hash ON curated.ml_feature_vectors(feature_hash);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_version ON curated.ml_feature_vectors(feature_version);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_cutoff ON curated.ml_feature_vectors(feature_cutoff_time);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_completeness ON curated.ml_feature_vectors(feature_completeness_score);

-- JSONB indexes for feature vectors
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_temporal_gin ON curated.ml_feature_vectors USING GIN(temporal_features);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_market_gin ON curated.ml_feature_vectors USING GIN(market_features);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_team_gin ON curated.ml_feature_vectors USING GIN(team_features);
CREATE INDEX IF NOT EXISTS idx_ml_feature_vectors_splits_gin ON curated.ml_feature_vectors USING GIN(betting_splits_features);

-- ML Predictions Indexes
CREATE INDEX IF NOT EXISTS idx_ml_predictions_game_id ON curated.ml_predictions(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_model ON curated.ml_predictions(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_timestamp ON curated.ml_predictions(prediction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_experiment ON curated.ml_predictions(experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_confidence ON curated.ml_predictions(confidence_threshold_met);

-- Composite indexes for prediction queries
CREATE INDEX IF NOT EXISTS idx_ml_predictions_model_game ON curated.ml_predictions(model_name, game_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_game_timestamp ON curated.ml_predictions(game_id, prediction_timestamp);

-- JSONB indexes for predictions
CREATE INDEX IF NOT EXISTS idx_ml_predictions_importance_gin ON curated.ml_predictions USING GIN(feature_importance);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_explanation_gin ON curated.ml_predictions USING GIN(prediction_explanation);

-- ML Model Performance Indexes
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_model ON curated.ml_model_performance(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_type ON curated.ml_model_performance(prediction_type);
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_period ON curated.ml_model_performance(evaluation_period_start, evaluation_period_end);
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_accuracy ON curated.ml_model_performance(accuracy DESC);
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_roi ON curated.ml_model_performance(roi_percentage DESC);

-- ML Experiments Indexes
CREATE INDEX IF NOT EXISTS idx_ml_experiments_mlflow_id ON curated.ml_experiments(mlflow_experiment_id);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_target ON curated.ml_experiments(prediction_target);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_status ON curated.ml_experiments(status);
CREATE INDEX IF NOT EXISTS idx_ml_experiments_type ON curated.ml_experiments(model_type);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON VIEW curated.unified_line_movements IS 'Unified view combining line movements from Action Network, VSIN, and SBD sources with betting splits integration';
COMMENT ON VIEW curated.ml_feature_summary IS 'Quick summary view for ML feature availability and prediction readiness assessment';
COMMENT ON VIEW curated.ml_model_dashboard IS 'Model performance dashboard with rankings and recent performance metrics';
COMMENT ON VIEW curated.data_quality_monitoring IS 'Multi-source data quality monitoring with coverage metrics and ML readiness indicators';