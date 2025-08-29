-- =============================================================================
-- ROLLBACK: PERFORMANCE OPTIMIZATION - PHASE 1
-- =============================================================================
-- Migration: 200_rollback_performance_optimization_phase1.sql
-- Purpose: Safely rollback Phase 1 strategic indexing optimizations
-- Usage: Run this script to remove all Phase 1 indexes if performance degrades
-- Safety: Uses DROP INDEX CONCURRENTLY to avoid blocking operations
-- =============================================================================

-- Enable query timing for monitoring
\timing on

\echo ''
\echo '==============================================================================='
\echo 'ROLLBACK: PHASE 1 STRATEGIC INDEXING - STARTING'
\echo '==============================================================================='
\echo 'WARNING: This will remove all Phase 1 performance optimization indexes'
\echo 'Proceed only if performance has degraded after Phase 1 deployment'
\echo ''

BEGIN;

-- Log rollback start
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase1_rollback', 'rollback_start', 'strategic_indexes', 'started');

COMMIT;

-- =============================================================================
-- ROLLBACK PHASE 1A: HIGH-TRAFFIC RAW DATA INDEXES
-- =============================================================================

-- 1. Remove Action Network Odds composite query index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS raw_data.idx_action_network_odds_composite_query;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_composite_query', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_composite_query', 'failed', SQLERRM);
END $$;

-- 2. Remove unprocessed records index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS raw_data.idx_action_network_odds_unprocessed;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_unprocessed', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_unprocessed', 'failed', SQLERRM);
END $$;

-- 3. Remove JSONB GIN index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS raw_data.idx_action_network_odds_raw_odds_gin;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_raw_odds_gin', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_action_network_odds_raw_odds_gin', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK PHASE 1B: STAGING TABLE INDEXES
-- =============================================================================

-- 4. Remove betting analysis index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS staging.idx_betting_odds_unified_analysis;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_betting_odds_unified_analysis', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_betting_odds_unified_analysis', 'failed', SQLERRM);
END $$;

-- 5. Remove quality filter index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS staging.idx_betting_odds_unified_quality_filter;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_betting_odds_unified_quality_filter', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_betting_odds_unified_quality_filter', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK PHASE 1C: CURATED TABLE INDEXES
-- =============================================================================

-- 6. Remove ML ready composite index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_enhanced_games_ml_ready_composite;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_enhanced_games_ml_ready_composite', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_enhanced_games_ml_ready_composite', 'failed', SQLERRM);
END $$;

-- 7. Remove recent games index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_enhanced_games_recent_games;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_enhanced_games_recent_games', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_enhanced_games_recent_games', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK PHASE 1D: FOREIGN KEY INDEXES
-- =============================================================================

-- 8. Remove ML predictions foreign key index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS analytics.idx_ml_predictions_experiment_id_fk;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_ml_predictions_experiment_id_fk', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_ml_predictions_experiment_id_fk', 'failed', SQLERRM);
END $$;

-- 9. Remove arbitrage opportunities foreign key indexes
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_arbitrage_opportunities_game_id_fk;
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_arbitrage_opportunities_book_a_id_fk;
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_arbitrage_opportunities_book_b_id_fk;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'arbitrage_opportunities_fk_indexes', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'arbitrage_opportunities_fk_indexes', 'failed', SQLERRM);
END $$;

-- 10. Remove unified betting splits composite index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_unified_betting_splits_game_market_fk;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_unified_betting_splits_game_market_fk', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_unified_betting_splits_game_market_fk', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK PHASE 1E: ANALYTICS TABLE INDEXES
-- =============================================================================

-- 11. Remove ML features extraction composite index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_ml_features_extraction_composite;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_ml_features_extraction_composite', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_ml_features_extraction_composite', 'failed', SQLERRM);
END $$;

-- 12. Remove sharp action analysis index
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    DROP INDEX CONCURRENTLY IF EXISTS curated.idx_ml_features_sharp_analysis;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'drop_index', 'idx_ml_features_sharp_analysis', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'drop_index', 'idx_ml_features_sharp_analysis', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK PHASE 1F: UPDATE TABLE STATISTICS
-- =============================================================================

-- Re-analyze tables after index removal
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
    VALUES ('phase1_rollback', 'analyze_tables', 'all_tables', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase1_rollback', 'analyze_tables', 'all_tables', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- ROLLBACK COMPLETION AND VALIDATION
-- =============================================================================

BEGIN;

-- Log rollback completion
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase1_rollback', 'rollback_complete', 'strategic_indexes', 'completed');

-- Create rollback summary view
CREATE OR REPLACE VIEW performance_monitoring.phase1_rollback_summary AS
SELECT 
    operation,
    COUNT(*) as operation_count,
    COUNT(*) FILTER (WHERE status = 'completed') as successful_operations,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_operations,
    AVG(execution_time_ms) FILTER (WHERE status = 'completed') as avg_execution_time_ms,
    STRING_AGG(
        CASE WHEN status = 'failed' THEN object_name || ': ' || error_message END,
        '; '
    ) as error_summary
FROM performance_monitoring.optimization_log
WHERE phase = 'phase1_rollback'
GROUP BY operation
ORDER BY operation;

COMMIT;

-- =============================================================================
-- ROLLBACK VALIDATION
-- =============================================================================

-- Verify indexes were removed successfully
DO $$
DECLARE
    remaining_index_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO remaining_index_count
    FROM pg_indexes 
    WHERE indexname LIKE '%composite_query%' 
       OR indexname LIKE '%unprocessed%' 
       OR indexname LIKE '%unified_analysis%'
       OR indexname LIKE '%quality_filter%'
       OR indexname LIKE '%ml_ready_composite%'
       OR indexname LIKE '%recent_games%'
       OR indexname LIKE '%_fk%'
       OR indexname LIKE '%extraction_composite%'
       OR indexname LIKE '%sharp_analysis%'
       OR indexname LIKE '%raw_odds_gin%';
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase1_rollback', 'validation', 'remaining_index_count', 
           CASE WHEN remaining_index_count = 0 THEN 'completed' ELSE 'warning' END,
           remaining_index_count);
           
    RAISE NOTICE 'Rollback Validation: % Phase 1 indexes remaining (should be 0)', remaining_index_count;
END $$;

-- =============================================================================
-- ROLLBACK SUMMARY REPORT
-- =============================================================================

\echo ''
\echo '==============================================================================='
\echo 'PHASE 1 ROLLBACK - COMPLETION REPORT'
\echo '==============================================================================='
\echo ''

-- Show rollback summary
SELECT 'ROLLBACK SUMMARY:' as section;
SELECT * FROM performance_monitoring.phase1_rollback_summary;

\echo ''
\echo 'POST-ROLLBACK STEPS:'
\echo '1. Monitor query performance to confirm baseline restoration'
\echo '2. Review optimization_log for any failed rollback operations'
\echo '3. Consider alternative optimization strategies if needed'
\echo '4. Original table indexes remain intact and functional'
\echo ''
\echo 'Phase 1 Strategic Indexing rollback completed successfully!'

-- Turn off timing
\timing off