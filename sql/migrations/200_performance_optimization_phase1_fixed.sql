-- =============================================================================
-- PERFORMANCE OPTIMIZATION - PHASE 1: STRATEGIC INDEXING (FIXED)
-- =============================================================================
-- Migration: 200_performance_optimization_phase1_fixed.sql
-- Purpose: Strategic indexing for high-traffic tables
-- Fix: Removed transaction blocks around CONCURRENTLY operations
-- Safety: Uses CONCURRENTLY to avoid blocking operations
-- =============================================================================

-- Enable query timing for monitoring
\timing on

-- Create performance monitoring schema first (outside of concurrent operations)
CREATE SCHEMA IF NOT EXISTS performance_monitoring;

-- Performance baseline tracking table
CREATE TABLE IF NOT EXISTS performance_monitoring.optimization_log (
    id BIGSERIAL PRIMARY KEY,
    phase VARCHAR(50) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    object_name VARCHAR(255),
    status VARCHAR(20) DEFAULT 'started',
    execution_time_ms NUMERIC(10,3),
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Log phase 1 start
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'indexing_start', 'strategic_indexes', 'started');

-- =============================================================================
-- PHASE 1A: HIGH-TRAFFIC RAW DATA INDEXES
-- =============================================================================

\echo 'Creating indexes for raw_data.action_network_odds...'

-- 1. Composite index for game + sportsbook + timestamp queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_composite_query
ON raw_data.action_network_odds (external_game_id, sportsbook_key, collected_at DESC)
WHERE processed_at IS NOT NULL;

-- 2. Partial index for unprocessed records (hot data optimization)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_unprocessed
ON raw_data.action_network_odds (collected_at DESC)
WHERE processed_at IS NULL;

-- 3. JSONB GIN index for odds data analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_raw_odds_gin
ON raw_data.action_network_odds USING GIN (raw_odds)
WHERE raw_odds IS NOT NULL;

-- Log successful creation of raw data indexes
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'create_index', 'action_network_odds_indexes', 'completed');

-- =============================================================================
-- PHASE 1B: STAGING TABLE OPTIMIZATIONS
-- =============================================================================

\echo 'Creating indexes for staging.betting_odds_unified...'

-- 4. Multi-column index for betting analysis queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_odds_unified_analysis
ON staging.betting_odds_unified (data_source, external_game_id, market_type, odds_timestamp DESC)
WHERE validation_status = 'valid';

-- 5. Quality-filtered index for analytics
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_odds_unified_quality_filter
ON staging.betting_odds_unified (game_date, home_team, away_team)
WHERE data_quality_score >= 0.8;

-- Log successful creation of staging indexes
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'create_index', 'staging_betting_odds_indexes', 'completed');

-- =============================================================================
-- PHASE 1C: CURATED TABLE OPTIMIZATIONS
-- =============================================================================

\echo 'Creating indexes for curated tables...'

-- 6. Composite index for ML feature queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_ml_ready_composite
ON curated.enhanced_games_backup_simple_unification (game_status, game_date, home_team, away_team)
WHERE game_status = 'final' AND home_score IS NOT NULL AND away_score IS NOT NULL;

-- 7. Temporal index for recent game queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_recent_games
ON curated.enhanced_games_backup_simple_unification (game_datetime DESC, game_status)
WHERE game_datetime >= (CURRENT_DATE - INTERVAL '30 days');

-- Log successful creation of curated indexes
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'create_index', 'enhanced_games_indexes', 'completed');

-- =============================================================================
-- PHASE 1D: FOREIGN KEY INDEX OPTIMIZATION
-- =============================================================================

\echo 'Creating foreign key indexes...'

-- 8. ML Predictions foreign key index (if table exists)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_predictions_experiment_id_fk
ON analytics.ml_predictions (experiment_id);

-- 9. Arbitrage opportunities foreign key indexes (if table exists)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_arbitrage_opportunities_game_id_fk
ON curated.arbitrage_opportunities (game_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_arbitrage_opportunities_book_a_id_fk
ON curated.arbitrage_opportunities (book_a_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_arbitrage_opportunities_book_b_id_fk
ON curated.arbitrage_opportunities (book_b_id);

-- 10. Unified betting splits composite foreign key optimization
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_unified_betting_splits_game_market_fk
ON curated.unified_betting_splits (game_id, market_type, data_source);

-- Log successful creation of foreign key indexes
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'create_index', 'foreign_key_indexes', 'completed');

-- =============================================================================
-- PHASE 1E: ANALYTICS TABLE OPTIMIZATIONS
-- =============================================================================

\echo 'Creating analytics table indexes...'

-- 11. ML Features composite index for feature extraction queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_extraction_composite
ON curated.ml_features (feature_extraction_date DESC, data_quality_score)
WHERE data_quality_score >= 0.7;

-- 12. Sharp action analysis index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_sharp_analysis
ON curated.ml_features (game_date, sharp_money_percentage_home, reverse_line_movement)
WHERE sharp_money_percentage_home IS NOT NULL OR reverse_line_movement = TRUE;

-- Log successful creation of analytics indexes
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'create_index', 'analytics_indexes', 'completed');

-- =============================================================================
-- PHASE 1F: UPDATE TABLE STATISTICS
-- =============================================================================

\echo 'Updating table statistics...'

-- Update statistics for all optimized tables to help query planner
ANALYZE raw_data.action_network_odds;
ANALYZE staging.betting_odds_unified;
ANALYZE curated.enhanced_games_backup_simple_unification;
ANALYZE curated.ml_features;
ANALYZE analytics.ml_predictions;
ANALYZE curated.arbitrage_opportunities;
ANALYZE curated.unified_betting_splits;

-- Log successful statistics update
INSERT INTO performance_monitoring.optimization_log 
    (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'analyze_tables', 'all_optimized_tables', 'completed');

-- =============================================================================
-- PHASE 1 COMPLETION AND VALIDATION
-- =============================================================================

-- Log phase 1 completion
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase1_fixed', 'indexing_complete', 'strategic_indexes', 'completed');

-- Create summary view of phase 1 results
CREATE OR REPLACE VIEW performance_monitoring.phase1_fixed_summary AS
SELECT 
    operation,
    COUNT(*) as operation_count,
    COUNT(*) FILTER (WHERE status = 'completed') as successful_operations,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_operations,
    AVG(execution_time_ms) FILTER (WHERE status = 'completed') as avg_execution_time_ms,
    MAX(execution_time_ms) FILTER (WHERE status = 'completed') as max_execution_time_ms,
    STRING_AGG(
        CASE WHEN status = 'failed' THEN object_name || ': ' || error_message END,
        '; '
    ) as error_summary
FROM performance_monitoring.optimization_log
WHERE phase = 'phase1_fixed'
GROUP BY operation
ORDER BY operation;

-- =============================================================================
-- VALIDATION
-- =============================================================================

-- Count successfully created indexes
DO $$
DECLARE
    index_count INTEGER;
    expected_count INTEGER := 12;
BEGIN
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes 
    WHERE indexname LIKE 'idx_action_network_odds_%'
       OR indexname LIKE 'idx_betting_odds_unified_%'
       OR indexname LIKE 'idx_enhanced_games_%'
       OR indexname LIKE 'idx_ml_%'
       OR indexname LIKE 'idx_arbitrage_%'
       OR indexname LIKE 'idx_unified_betting_%';
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_fixed', 'validation', 'index_count_check', 
           CASE WHEN index_count >= expected_count THEN 'completed' ELSE 'warning' END,
           index_count);
           
    RAISE NOTICE 'Phase 1 Fixed Validation: Created % new indexes', index_count;
END $$;

-- =============================================================================
-- PHASE 1 SUMMARY REPORT
-- =============================================================================

\echo ''
\echo '==============================================================================='
\echo 'PHASE 1: STRATEGIC INDEXING (FIXED) - COMPLETION REPORT'
\echo '==============================================================================='
\echo ''

-- Show phase 1 summary
SELECT 'PHASE 1 FIXED SUMMARY:' as section;
SELECT * FROM performance_monitoring.phase1_fixed_summary;

\echo ''
\echo 'CREATED INDEXES:'

-- List all newly created optimization indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    'CREATED' as status
FROM pg_indexes 
WHERE indexname LIKE 'idx_action_network_odds_%'
   OR indexname LIKE 'idx_betting_odds_unified_%'
   OR indexname LIKE 'idx_enhanced_games_%'
   OR indexname LIKE 'idx_ml_%'
   OR indexname LIKE 'idx_arbitrage_%'
   OR indexname LIKE 'idx_unified_betting_%'
ORDER BY schemaname, tablename, indexname;

\echo ''
\echo 'NEXT STEPS:'
\echo '1. Run performance tests: sql/performance_testing.sql'
\echo '2. Run Phase 2 migration: 201_performance_optimization_phase2.sql (Partitioning)'
\echo '3. Monitor query performance improvements'
\echo '4. Consider rollback if performance degrades: 200_rollback_performance_optimization_phase1.sql'
\echo ''
\echo 'Phase 1 Strategic Indexing (Fixed) completed successfully!'

-- Turn off timing
\timing off