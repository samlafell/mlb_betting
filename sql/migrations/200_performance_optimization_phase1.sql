-- =============================================================================
-- PERFORMANCE OPTIMIZATION - PHASE 1: STRATEGIC INDEXING
-- =============================================================================
-- Migration: 200_performance_optimization_phase1.sql
-- Purpose: Strategic indexing for high-traffic tables
-- Safety: Uses CONCURRENTLY to avoid blocking operations
-- Rollback: 200_rollback_performance_optimization_phase1.sql
-- =============================================================================

-- Enable query timing for monitoring
\timing on

BEGIN;

-- Create performance monitoring schema first
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
VALUES ('phase1', 'indexing_start', 'strategic_indexes', 'started');

COMMIT;

-- =============================================================================
-- PHASE 1A: HIGH-TRAFFIC RAW DATA INDEXES
-- =============================================================================

-- 1. Action Network Odds - Primary ingestion table (9,903 records)
-- Composite index for game + sportsbook + timestamp queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Create composite query index
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_composite_query
    ON raw_data.action_network_odds (external_game_id, sportsbook_key, collected_at DESC)
    WHERE processed_at IS NOT NULL;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_action_network_odds_composite_query', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_action_network_odds_composite_query', 'failed', SQLERRM);
END $$;

-- 2. Partial index for unprocessed records (hot data optimization)
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_unprocessed
    ON raw_data.action_network_odds (collected_at DESC)
    WHERE processed_at IS NULL;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_action_network_odds_unprocessed', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_action_network_odds_unprocessed', 'failed', SQLERRM);
END $$;

-- 3. JSONB GIN index for odds data analysis
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_odds_raw_odds_gin
    ON raw_data.action_network_odds USING GIN (raw_odds)
    WHERE raw_odds IS NOT NULL;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_action_network_odds_raw_odds_gin', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_action_network_odds_raw_odds_gin', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1B: STAGING TABLE OPTIMIZATIONS
-- =============================================================================

-- 4. Betting Odds Unified - Core staging table (1,813 records)
-- Multi-column index for betting analysis queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_odds_unified_analysis
    ON staging.betting_odds_unified (data_source, external_game_id, market_type, odds_timestamp DESC)
    WHERE validation_status = 'valid';
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_betting_odds_unified_analysis', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_betting_odds_unified_analysis', 'failed', SQLERRM);
END $$;

-- 5. Quality-filtered index for analytics
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_odds_unified_quality_filter
    ON staging.betting_odds_unified (game_date, home_team, away_team)
    WHERE data_quality_score >= 0.8;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_betting_odds_unified_quality_filter', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_betting_odds_unified_quality_filter', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1C: CURATED TABLE OPTIMIZATIONS
-- =============================================================================

-- 6. Enhanced Games - Core game analysis table
-- Composite index for ML feature queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_ml_ready_composite
    ON curated.enhanced_games_backup_simple_unification (game_status, game_date, home_team, away_team)
    WHERE game_status = 'final' AND home_score IS NOT NULL AND away_score IS NOT NULL;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_enhanced_games_ml_ready_composite', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_enhanced_games_ml_ready_composite', 'failed', SQLERRM);
END $$;

-- 7. Temporal index for recent game queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_recent_games
    ON curated.enhanced_games_backup_simple_unification (game_datetime DESC, game_status)
    WHERE game_datetime >= (CURRENT_DATE - INTERVAL '30 days');
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_enhanced_games_recent_games', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_enhanced_games_recent_games', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1D: FOREIGN KEY INDEX OPTIMIZATION
-- =============================================================================

-- 8. ML Predictions foreign key index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Check if index already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_ml_predictions_experiment_id_fk'
    ) THEN
        CREATE INDEX CONCURRENTLY idx_ml_predictions_experiment_id_fk
        ON analytics.ml_predictions (experiment_id);
    END IF;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_ml_predictions_experiment_id_fk', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_ml_predictions_experiment_id_fk', 'failed', SQLERRM);
END $$;

-- 9. Arbitrage opportunities foreign key indexes
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Game ID foreign key index
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_arbitrage_opportunities_game_id_fk'
    ) THEN
        CREATE INDEX CONCURRENTLY idx_arbitrage_opportunities_game_id_fk
        ON curated.arbitrage_opportunities (game_id);
    END IF;
    
    -- Book A ID foreign key index
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_arbitrage_opportunities_book_a_id_fk'
    ) THEN
        CREATE INDEX CONCURRENTLY idx_arbitrage_opportunities_book_a_id_fk
        ON curated.arbitrage_opportunities (book_a_id);
    END IF;
    
    -- Book B ID foreign key index
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_arbitrage_opportunities_book_b_id_fk'
    ) THEN
        CREATE INDEX CONCURRENTLY idx_arbitrage_opportunities_book_b_id_fk
        ON curated.arbitrage_opportunities (book_b_id);
    END IF;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'arbitrage_opportunities_fk_indexes', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'arbitrage_opportunities_fk_indexes', 'failed', SQLERRM);
END $$;

-- 10. Unified betting splits composite foreign key optimization
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_unified_betting_splits_game_market_fk
    ON curated.unified_betting_splits (game_id, market_type, data_source);
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_unified_betting_splits_game_market_fk', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_unified_betting_splits_game_market_fk', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1E: ANALYTICS TABLE OPTIMIZATIONS
-- =============================================================================

-- 11. ML Features table performance indexes
-- Composite index for feature extraction queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_extraction_composite
    ON curated.ml_features (feature_extraction_date DESC, data_quality_score)
    WHERE data_quality_score >= 0.7;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_ml_features_extraction_composite', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_ml_features_extraction_composite', 'failed', SQLERRM);
END $$;

-- 12. Sharp action analysis index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_sharp_analysis
    ON curated.ml_features (game_date, sharp_money_percentage_home, reverse_line_movement)
    WHERE sharp_money_percentage_home IS NOT NULL OR reverse_line_movement = TRUE;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'create_index', 'idx_ml_features_sharp_analysis', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'create_index', 'idx_ml_features_sharp_analysis', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1F: UPDATE TABLE STATISTICS
-- =============================================================================

-- Update statistics for all optimized tables to help query planner
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    ANALYZE raw_data.action_network_odds;
    ANALYZE staging.betting_odds_unified;
    ANALYZE curated.enhanced_games_backup_simple_unification;
    ANALYZE curated.ml_features;
    ANALYZE analytics.ml_predictions;
    ANALYZE curated.arbitrage_opportunities;
    ANALYZE curated.unified_betting_splits;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'analyze_tables', 'all_optimized_tables', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1', 'analyze_tables', 'all_optimized_tables', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 1 COMPLETION AND VALIDATION
-- =============================================================================

BEGIN;

-- Log phase 1 completion
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase1', 'indexing_complete', 'strategic_indexes', 'completed');

-- Create summary view of phase 1 results
CREATE OR REPLACE VIEW performance_monitoring.phase1_summary AS
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
WHERE phase = 'phase1'
GROUP BY operation
ORDER BY operation;

COMMIT;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- Verify indexes were created successfully
DO $$
DECLARE
    index_count INTEGER;
    expected_count INTEGER := 12;
BEGIN
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes 
    WHERE indexname LIKE '%composite_query%' 
       OR indexname LIKE '%unprocessed%' 
       OR indexname LIKE '%analysis%'
       OR indexname LIKE '%quality_filter%'
       OR indexname LIKE '%ml_ready_composite%'
       OR indexname LIKE '%recent_games%'
       OR indexname LIKE '%_fk%'
       OR indexname LIKE '%extraction_composite%'
       OR indexname LIKE '%sharp_analysis%'
       OR indexname LIKE '%raw_odds_gin%';
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1', 'validation', 'index_count_check', 
           CASE WHEN index_count >= expected_count THEN 'completed' ELSE 'warning' END,
           index_count);
           
    RAISE NOTICE 'Phase 1 Validation: Created % indexes out of % expected', index_count, expected_count;
END $$;

-- =============================================================================
-- PHASE 1 SUMMARY REPORT
-- =============================================================================

\echo ''
\echo '==============================================================================='
\echo 'PHASE 1: STRATEGIC INDEXING - COMPLETION REPORT'
\echo '==============================================================================='
\echo ''

-- Show phase 1 summary
SELECT 'PHASE 1 SUMMARY:' as section;
SELECT * FROM performance_monitoring.phase1_summary;

\echo ''
\echo 'NEXT STEPS:'
\echo '1. Run Phase 2 migration: 201_performance_optimization_phase2.sql (Partitioning)'
\echo '2. Monitor query performance using pg_stat_user_tables'
\echo '3. Run performance validation tests'
\echo '4. Consider rollback if performance degrades: 200_rollback_performance_optimization_phase1.sql'
\echo ''
\echo 'Phase 1 Strategic Indexing completed successfully!'

-- Turn off timing
\timing off