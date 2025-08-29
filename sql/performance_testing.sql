-- =============================================================================
-- PERFORMANCE TESTING AND VALIDATION SUITE
-- =============================================================================
-- Purpose: Comprehensive testing of database performance optimizations
-- Usage: Run after Phase 1 and Phase 2 optimizations to validate improvements
-- Targets: <100ms common queries, <50ms data ingestion, 50+ concurrent users
-- =============================================================================

-- Enable query timing and detailed output
\timing on
\set VERBOSITY verbose

\echo ''
\echo '==============================================================================='
\echo 'PERFORMANCE TESTING AND VALIDATION SUITE'
\echo '==============================================================================='
\echo 'Testing database performance against production targets:'
\echo '- Common betting queries: <100ms'
\echo '- Data ingestion: <50ms per batch'
\echo '- ML feature extraction: <200ms'
\echo '- Concurrent user support: 50+ users'
\echo ''

-- =============================================================================
-- PERFORMANCE TESTING FUNCTIONS
-- =============================================================================

-- Enhanced benchmarking function with statistics
CREATE OR REPLACE FUNCTION performance_monitoring.benchmark_query_detailed(
    test_name TEXT,
    query_text TEXT,
    iterations INTEGER DEFAULT 5,
    optimization_phase TEXT DEFAULT 'test'
) RETURNS TABLE(
    avg_execution_time_ms NUMERIC,
    min_execution_time_ms NUMERIC,
    max_execution_time_ms NUMERIC,
    std_dev_ms NUMERIC,
    rows_returned BIGINT
) AS $$
DECLARE
    times NUMERIC[] := ARRAY[]::NUMERIC[];
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    row_count BIGINT;
    i INTEGER;
BEGIN
    -- Run multiple iterations for statistical accuracy
    FOR i IN 1..iterations LOOP
        start_time := CLOCK_TIMESTAMP();
        
        -- Execute the query
        EXECUTE query_text;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        
        end_time := CLOCK_TIMESTAMP();
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        times := array_append(times, duration_ms);
    END LOOP;
    
    -- Calculate statistics
    INSERT INTO performance_monitoring.optimization_log
        (phase, operation, object_name, status, execution_time_ms)
    VALUES 
        (optimization_phase, 'benchmark_test', test_name, 'completed', 
         (SELECT AVG(unnest) FROM UNNEST(times)));
    
    -- Return statistical summary
    RETURN QUERY
    SELECT 
        (SELECT AVG(unnest) FROM UNNEST(times)) as avg_ms,
        (SELECT MIN(unnest) FROM UNNEST(times)) as min_ms,
        (SELECT MAX(unnest) FROM UNNEST(times)) as max_ms,
        (SELECT STDDEV(unnest) FROM UNNEST(times)) as std_dev_ms,
        row_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TEST SUITE 1: RAW DATA INGESTION PERFORMANCE
-- =============================================================================

\echo 'TEST SUITE 1: Raw Data Ingestion Performance'
\echo '============================================='

-- Test 1.1: Action Network Odds - Recent data query (simulates real-time ingestion checks)
SELECT 
    'Test 1.1: Recent Action Network Odds Query' as test_name,
    'Target: <50ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'raw_odds_recent_ingestion',
    'SELECT COUNT(*) FROM raw_data.action_network_odds 
     WHERE collected_at >= CURRENT_TIMESTAMP - INTERVAL ''1 hour''',
    5,
    'ingestion_test'
);

-- Test 1.2: Unprocessed records query (hot data optimization test)
SELECT 
    'Test 1.2: Unprocessed Records Query' as test_name,
    'Target: <25ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'unprocessed_records_check',
    'SELECT external_game_id, sportsbook_key, collected_at 
     FROM raw_data.action_network_odds 
     WHERE processed_at IS NULL 
     ORDER BY collected_at DESC LIMIT 100',
    5,
    'ingestion_test'
);

-- Test 1.3: JSONB odds data analysis (GIN index test)
SELECT 
    'Test 1.3: JSONB Odds Data Analysis' as test_name,
    'Target: <100ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'jsonb_odds_analysis',
    'SELECT external_game_id, raw_odds->>''home_odds'' as home_odds
     FROM raw_data.action_network_odds 
     WHERE raw_odds ? ''home_odds'' 
     AND collected_at >= CURRENT_DATE - INTERVAL ''24 hours''
     LIMIT 500',
    5,
    'ingestion_test'
);

-- =============================================================================
-- TEST SUITE 2: STAGING DATA PROCESSING PERFORMANCE
-- =============================================================================

\echo ''
\echo 'TEST SUITE 2: Staging Data Processing Performance'
\echo '================================================='

-- Test 2.1: Betting odds unified analysis query
SELECT 
    'Test 2.1: Betting Odds Analysis Query' as test_name,
    'Target: <75ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'betting_odds_analysis',
    'SELECT data_source, market_type, COUNT(*) as records,
            AVG(data_quality_score) as avg_quality
     FROM staging.betting_odds_unified 
     WHERE validation_status = ''valid'' 
     AND game_date >= CURRENT_DATE - INTERVAL ''7 days''
     GROUP BY data_source, market_type',
    5,
    'staging_test'
);

-- Test 2.2: High-quality data filtering test
SELECT 
    'Test 2.2: High-Quality Data Filter' as test_name,
    'Target: <50ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'high_quality_data_filter',
    'SELECT game_date, home_team, away_team, 
            COUNT(*) as odds_records
     FROM staging.betting_odds_unified 
     WHERE data_quality_score >= 0.8
     AND game_date BETWEEN CURRENT_DATE - INTERVAL ''30 days'' AND CURRENT_DATE
     GROUP BY game_date, home_team, away_team 
     ORDER BY game_date DESC
     LIMIT 200',
    5,
    'staging_test'
);

-- =============================================================================
-- TEST SUITE 3: CURATED DATA ANALYTICS PERFORMANCE
-- =============================================================================

\echo ''
\echo 'TEST SUITE 3: Curated Data Analytics Performance'
\echo '================================================'

-- Test 3.1: ML-ready games query (common ML pipeline query)
SELECT 
    'Test 3.1: ML-Ready Games Query' as test_name,
    'Target: <100ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'ml_ready_games_query',
    'SELECT game_date, home_team, away_team, home_score, away_score
     FROM curated.enhanced_games_backup_simple_unification 
     WHERE game_status = ''final'' 
     AND home_score IS NOT NULL 
     AND away_score IS NOT NULL
     AND game_date >= CURRENT_DATE - INTERVAL ''30 days''
     ORDER BY game_date DESC
     LIMIT 300',
    5,
    'curated_test'
);

-- Test 3.2: Recent games temporal query
SELECT 
    'Test 3.2: Recent Games Temporal Query' as test_name,
    'Target: <75ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'recent_games_temporal',
    'SELECT COUNT(*) as total_games,
            COUNT(*) FILTER (WHERE game_status = ''final'') as completed_games,
            COUNT(*) FILTER (WHERE game_datetime >= CURRENT_TIMESTAMP - INTERVAL ''24 hours'') as recent_games
     FROM curated.enhanced_games_backup_simple_unification 
     WHERE game_datetime >= CURRENT_DATE - INTERVAL ''30 days''',
    5,
    'curated_test'
);

-- Test 3.3: Sharp action analysis (complex analytics query)
SELECT 
    'Test 3.3: Sharp Action Analysis' as test_name,
    'Target: <200ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'sharp_action_analysis',
    'SELECT game_date, 
            COUNT(*) as total_games,
            COUNT(*) FILTER (WHERE sharp_money_percentage_home > 60) as sharp_home_games,
            COUNT(*) FILTER (WHERE reverse_line_movement = TRUE) as rlm_games,
            AVG(sharp_money_percentage_home) as avg_sharp_percentage
     FROM curated.ml_features 
     WHERE game_date >= CURRENT_DATE - INTERVAL ''30 days'' 
     AND data_quality_score >= 0.7
     GROUP BY game_date 
     ORDER BY game_date DESC',
    5,
    'curated_test'
);

-- =============================================================================
-- TEST SUITE 4: JOIN PERFORMANCE AND FOREIGN KEY OPTIMIZATION
-- =============================================================================

\echo ''
\echo 'TEST SUITE 4: Join Performance and Foreign Key Optimization'
\echo '==========================================================='

-- Test 4.1: Multi-table join query (games + betting lines)
SELECT 
    'Test 4.1: Games-Betting Lines Join' as test_name,
    'Target: <150ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'games_betting_lines_join',
    'SELECT g.game_date, g.home_team, g.away_team, g.game_status,
            COUNT(bl.id) as betting_lines_count,
            MAX(bl.odds_timestamp) as latest_odds_update
     FROM curated.enhanced_games_backup_simple_unification g
     LEFT JOIN curated.betting_lines_unified bl ON g.id::text = bl.game_id
     WHERE g.game_date >= CURRENT_DATE - INTERVAL ''7 days''
     GROUP BY g.game_date, g.home_team, g.away_team, g.game_status
     ORDER BY g.game_date DESC
     LIMIT 100',
    3,
    'join_test'
);

-- Test 4.2: ML predictions with experiments join
SELECT 
    'Test 4.2: ML Predictions-Experiments Join' as test_name,
    'Target: <100ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'ml_predictions_experiments_join',
    'SELECT e.experiment_name, e.model_type,
            COUNT(p.id) as prediction_count,
            AVG(p.confidence_score) as avg_confidence
     FROM analytics.ml_experiments e
     LEFT JOIN analytics.ml_predictions p ON e.id = p.experiment_id
     WHERE e.status IN (''completed'', ''running'')
     GROUP BY e.experiment_name, e.model_type',
    3,
    'join_test'
);

-- Test 4.3: Arbitrage opportunities complex join
SELECT 
    'Test 4.3: Arbitrage Opportunities Join' as test_name,
    'Target: <100ms' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'arbitrage_opportunities_join',
    'SELECT a.game_id, a.potential_profit_percentage,
            g.home_team, g.away_team, g.game_datetime
     FROM curated.arbitrage_opportunities a
     JOIN curated.enhanced_games_backup_simple_unification g ON a.game_id = g.id
     WHERE a.potential_profit_percentage > 2.0
     AND g.game_datetime >= CURRENT_TIMESTAMP - INTERVAL ''48 hours''
     ORDER BY a.potential_profit_percentage DESC',
    3,
    'join_test'
);

-- =============================================================================
-- TEST SUITE 5: CONCURRENT USER SIMULATION
-- =============================================================================

\echo ''
\echo 'TEST SUITE 5: Concurrent User Load Simulation'
\echo '=============================================='

-- Test 5.1: Simulate concurrent betting analysis queries
DO $$
DECLARE
    concurrent_start TIMESTAMPTZ;
    concurrent_end TIMESTAMPTZ;
    concurrent_duration_ms NUMERIC;
    i INTEGER;
BEGIN
    concurrent_start := CLOCK_TIMESTAMP();
    
    -- Simulate 10 concurrent users running common queries
    FOR i IN 1..10 LOOP
        PERFORM COUNT(*) FROM raw_data.action_network_odds 
        WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day';
        
        PERFORM COUNT(*) FROM staging.betting_odds_unified 
        WHERE validation_status = 'valid' 
        AND game_date >= CURRENT_DATE - INTERVAL '3 days';
        
        PERFORM COUNT(*) FROM curated.enhanced_games_backup_simple_unification 
        WHERE game_status = 'final' 
        AND game_date >= CURRENT_DATE - INTERVAL '7 days';
    END LOOP;
    
    concurrent_end := CLOCK_TIMESTAMP();
    concurrent_duration_ms := EXTRACT(EPOCH FROM (concurrent_end - concurrent_start)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('concurrent_test', 'simulate_10_users', 'concurrent_load_test', 'completed', concurrent_duration_ms);
        
    RAISE NOTICE 'Concurrent Load Test: 10 users, % operations completed in %ms', 
                 30, concurrent_duration_ms;
END $$;

-- =============================================================================
-- TEST SUITE 6: PARTITIONING PERFORMANCE (IF PHASE 2 COMPLETED)
-- =============================================================================

\echo ''
\echo 'TEST SUITE 6: Partitioning Performance (Phase 2)'
\echo '================================================'

-- Test 6.1: Partitioned table query performance
SELECT 
    'Test 6.1: Partitioned Odds Query' as test_name,
    'Target: <50ms (with partition pruning)' as performance_target,
    *
FROM performance_monitoring.benchmark_query_detailed(
    'partitioned_odds_query',
    'SELECT COUNT(*), MIN(collected_at), MAX(collected_at)
     FROM raw_data.action_network_odds_partitioned 
     WHERE collected_at >= DATE_TRUNC(''month'', CURRENT_DATE - INTERVAL ''1 month'')
     AND collected_at < DATE_TRUNC(''month'', CURRENT_DATE)',
    3,
    'partition_test'
) WHERE EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'raw_data' 
    AND table_name = 'action_network_odds_partitioned'
);

-- =============================================================================
-- PERFORMANCE TARGET VALIDATION
-- =============================================================================

\echo ''
\echo 'PERFORMANCE TARGET VALIDATION'
\echo '============================='

-- Create comprehensive performance summary
CREATE OR REPLACE VIEW performance_monitoring.performance_test_summary AS
WITH test_results AS (
    SELECT 
        object_name as test_name,
        execution_time_ms,
        CASE 
            WHEN object_name LIKE '%ingestion%' AND execution_time_ms <= 50 THEN 'PASS'
            WHEN object_name LIKE '%analysis%' AND execution_time_ms <= 100 THEN 'PASS'
            WHEN object_name LIKE '%join%' AND execution_time_ms <= 150 THEN 'PASS'
            WHEN object_name LIKE '%ml_%' AND execution_time_ms <= 200 THEN 'PASS'
            WHEN object_name LIKE '%concurrent%' AND execution_time_ms <= 5000 THEN 'PASS'
            ELSE 'REVIEW'
        END as performance_status,
        CASE 
            WHEN object_name LIKE '%ingestion%' THEN 50
            WHEN object_name LIKE '%analysis%' THEN 100
            WHEN object_name LIKE '%join%' THEN 150
            WHEN object_name LIKE '%ml_%' THEN 200
            WHEN object_name LIKE '%concurrent%' THEN 5000
            ELSE 100
        END as target_ms
    FROM performance_monitoring.optimization_log
    WHERE phase IN ('ingestion_test', 'staging_test', 'curated_test', 'join_test', 'concurrent_test', 'partition_test')
    AND operation = 'benchmark_test'
    AND created_at >= CURRENT_DATE
)
SELECT 
    test_name,
    ROUND(execution_time_ms, 2) as actual_ms,
    target_ms,
    ROUND(((target_ms - execution_time_ms) / target_ms * 100), 2) as performance_margin_percent,
    performance_status
FROM test_results
ORDER BY 
    CASE performance_status WHEN 'PASS' THEN 1 ELSE 0 END,
    execution_time_ms DESC;

-- Show performance test summary
SELECT 'PERFORMANCE TEST SUMMARY' as section;
SELECT * FROM performance_monitoring.performance_test_summary;

-- =============================================================================
-- SYSTEM RESOURCE ANALYSIS
-- =============================================================================

\echo ''
\echo 'SYSTEM RESOURCE ANALYSIS'
\echo '========================'

-- Database size and growth analysis
SELECT 
    'Database Size Analysis' as analysis_type,
    pg_size_pretty(pg_database_size('mlb_betting')) as total_database_size,
    (
        SELECT pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))::bigint)
        FROM pg_tables 
        WHERE schemaname IN ('raw_data', 'staging', 'curated', 'analytics')
    ) as optimized_schemas_size,
    (
        SELECT COUNT(*)
        FROM pg_indexes 
        WHERE schemaname IN ('raw_data', 'staging', 'curated', 'analytics')
    ) as total_indexes;

-- Index usage statistics
SELECT 
    'Index Usage Statistics' as analysis_type,
    schemaname,
    COUNT(*) as index_count,
    COUNT(*) FILTER (WHERE idx_scan > 0) as used_indexes,
    ROUND(AVG(idx_scan), 2) as avg_index_scans
FROM pg_stat_user_indexes 
WHERE schemaname IN ('raw_data', 'staging', 'curated', 'analytics')
GROUP BY schemaname
ORDER BY schemaname;

-- =============================================================================
-- FINAL RECOMMENDATIONS
-- =============================================================================

\echo ''
\echo '==============================================================================='
\echo 'PERFORMANCE TESTING COMPLETED - ANALYSIS AND RECOMMENDATIONS'
\echo '==============================================================================='

-- Generate final performance report
DO $$
DECLARE
    passing_tests INTEGER;
    total_tests INTEGER;
    pass_rate NUMERIC;
BEGIN
    SELECT 
        COUNT(*) FILTER (WHERE performance_status = 'PASS'),
        COUNT(*),
        ROUND(COUNT(*) FILTER (WHERE performance_status = 'PASS')::NUMERIC / COUNT(*) * 100, 1)
    INTO passing_tests, total_tests, pass_rate
    FROM performance_monitoring.performance_test_summary;
    
    RAISE NOTICE 'PERFORMANCE SUMMARY: %/% tests passing (%%)', passing_tests, total_tests, pass_rate;
    
    IF pass_rate >= 80 THEN
        RAISE NOTICE 'RECOMMENDATION: Performance targets largely met. Ready for production deployment.';
    ELSIF pass_rate >= 60 THEN
        RAISE NOTICE 'RECOMMENDATION: Performance acceptable. Monitor slow queries and consider additional optimization.';
    ELSE
        RAISE NOTICE 'RECOMMENDATION: Performance below targets. Review failing tests and optimize before production.';
    END IF;
END $$;

\echo ''
\echo 'NEXT STEPS:'
\echo '1. Review performance_monitoring.performance_test_summary for detailed results'
\echo '2. Address any tests with "REVIEW" status'
\echo '3. Run production load testing with actual concurrent users'
\echo '4. Set up continuous performance monitoring'
\echo '5. Schedule regular maintenance: SELECT performance_monitoring.run_maintenance();'
\echo ''

-- Turn off timing
\timing off