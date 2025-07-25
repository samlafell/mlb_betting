-- ==================================================================================
-- MLB Sharp Betting System - Final Validation Suite
-- ==================================================================================
-- 
-- This script performs comprehensive validation of the consolidated schema system
-- before removing legacy schemas. Tests data integrity, relationships, and 
-- system functionality across all consolidated schemas.
-- ==================================================================================

-- Create validation results table
CREATE TABLE IF NOT EXISTS operational.final_validation_results (
    id SERIAL PRIMARY KEY,
    test_category VARCHAR(50) NOT NULL,
    test_name VARCHAR(100) NOT NULL,
    test_query TEXT NOT NULL,
    expected_result TEXT,
    actual_result TEXT,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Validation logging function
CREATE OR REPLACE FUNCTION log_validation_result(
    p_category VARCHAR(50),
    p_test_name VARCHAR(100),
    p_query TEXT,
    p_expected TEXT DEFAULT NULL,
    p_actual TEXT DEFAULT NULL,
    p_status VARCHAR(20) DEFAULT 'passed',
    p_error TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.final_validation_results (
        test_category, test_name, test_query, expected_result, 
        actual_result, status, error_message
    ) VALUES (
        p_category, p_test_name, p_query, p_expected, 
        p_actual, p_status, p_error
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Final System Validation';
    RAISE NOTICE 'Timestamp: %', NOW();
    
    -- Clear previous validation results
    DELETE FROM operational.final_validation_results;
END $$;

-- ==================================================================================
-- DATA INTEGRITY VALIDATION
-- ==================================================================================

-- Test 1: Verify migration completeness
DO $$
DECLARE
    source_action_teams INTEGER;
    target_core_teams INTEGER;
    source_splits_games INTEGER;
    target_supplementary_games INTEGER;
    source_strategy_configs INTEGER;
    target_strategy_configs INTEGER;
    source_threshold_configs INTEGER;
    target_threshold_configs INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    -- Check Action Network data migration
    SELECT COUNT(*) INTO source_action_teams FROM action.dim_teams;
    SELECT COUNT(*) INTO target_core_teams FROM curated.teams_master WHERE action_network_id IS NOT NULL;
    
    -- Check Splits data migration  
    SELECT COUNT(*) INTO source_splits_games FROM splits.games;
    SELECT COUNT(*) INTO target_supplementary_games FROM curated.games_complete;
    
    -- Check Strategy configurations migration
    SELECT COUNT(*) INTO source_strategy_configs FROM backtesting.strategy_configurations;
    SELECT COUNT(*) INTO target_strategy_configs FROM operational.strategy_configurations WHERE strategy_id LIKE 'bt_%';
    
    -- Check Threshold configurations migration
    SELECT COUNT(*) INTO source_threshold_configs FROM backtesting.threshold_configurations;
    SELECT COUNT(*) INTO target_threshold_configs FROM operational.threshold_configurations;
    
    -- Validate migration completeness
    IF source_action_teams != target_core_teams THEN
        test_status := 'failed';
        error_msg := format('Action teams mismatch: source=%s, target=%s', source_action_teams, target_core_teams);
    ELSIF source_splits_games != target_supplementary_games THEN
        test_status := 'failed';
        error_msg := format('Splits games mismatch: source=%s, target=%s', source_splits_games, target_supplementary_games);
    ELSIF source_strategy_configs != target_strategy_configs THEN
        test_status := 'failed';
        error_msg := format('Strategy configs mismatch: source=%s, target=%s', source_strategy_configs, target_strategy_configs);
    ELSIF source_threshold_configs != target_threshold_configs THEN
        test_status := 'failed';
        error_msg := format('Threshold configs mismatch: source=%s, target=%s', source_threshold_configs, target_threshold_configs);
    END IF;
    
    PERFORM log_validation_result(
        'data_integrity', 
        'migration_completeness',
        'Compare source and target record counts',
        'All migrations should match source counts',
        format('Teams: %s->%s, Games: %s->%s, Strategies: %s->%s, Thresholds: %s->%s', 
               source_action_teams, target_core_teams, source_splits_games, target_supplementary_games,
               source_strategy_configs, target_strategy_configs, source_threshold_configs, target_threshold_configs),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Migration completeness validation: %', test_status;
END $$;

-- Test 2: Verify core betting data integrity
DO $$
DECLARE
    games_count INTEGER;
    moneyline_count INTEGER;
    spreads_count INTEGER;
    totals_count INTEGER;
    teams_count INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    SELECT COUNT(*) INTO games_count FROM curated.games_complete;
    SELECT COUNT(*) INTO moneyline_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline';
    SELECT COUNT(*) INTO spreads_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's;
    SELECT COUNT(*) INTO totals_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals';
    SELECT COUNT(*) INTO teams_count FROM curated.teams_master;
    
    -- Validate minimum expected data
    IF games_count < 1000 THEN
        test_status := 'failed';
        error_msg := format('Insufficient games data: %s', games_count);
    ELSIF moneyline_count < 5000 THEN
        test_status := 'failed';
        error_msg := format('Insufficient moneyline data: %s', moneyline_count);
    ELSIF spreads_count < 5000 THEN
        test_status := 'failed';
        error_msg := format('Insufficient spreads data: %s', spreads_count);
    ELSIF totals_count < 5000 THEN
        test_status := 'failed';
        error_msg := format('Insufficient totals data: %s', totals_count);
    ELSIF teams_count < 30 THEN
        test_status := 'failed';
        error_msg := format('Insufficient teams data: %s', teams_count);
    END IF;
    
    PERFORM log_validation_result(
        'data_integrity', 
        'core_betting_data_volume',
        'Verify core betting data volumes',
        'Games>1000, Moneyline>5000, Spreads>5000, Totals>5000, Teams>=30',
        format('Games: %s, Moneyline: %s, Spreads: %s, Totals: %s, Teams: %s', 
               games_count, moneyline_count, spreads_count, totals_count, teams_count),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Core betting data volume validation: %', test_status;
END $$;

-- Test 3: Verify foreign key relationships
DO $$
DECLARE
    orphaned_moneyline INTEGER;
    orphaned_spreads INTEGER;
    orphaned_totals INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    -- Check for orphaned betting lines
    SELECT COUNT(*) INTO orphaned_moneyline 
    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' ml
    LEFT JOIN curated.games_complete g ON ml.game_id = g.id
    WHERE g.id IS NULL;
    
    SELECT COUNT(*) INTO orphaned_spreads 
    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's s
    LEFT JOIN curated.games_complete g ON s.game_id = g.id
    WHERE g.id IS NULL;
    
    SELECT COUNT(*) INTO orphaned_totals 
    FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' t
    LEFT JOIN curated.games_complete g ON t.game_id = g.id
    WHERE g.id IS NULL;
    
    IF orphaned_moneyline > 0 OR orphaned_spreads > 0 OR orphaned_totals > 0 THEN
        test_status := 'failed';
        error_msg := format('Orphaned records found - Moneyline: %s, Spreads: %s, Totals: %s', 
                           orphaned_moneyline, orphaned_spreads, orphaned_totals);
    END IF;
    
    PERFORM log_validation_result(
        'data_integrity', 
        'foreign_key_relationships',
        'Check for orphaned betting lines',
        'No orphaned records',
        format('Orphaned - Moneyline: %s, Spreads: %s, Totals: %s', 
               orphaned_moneyline, orphaned_spreads, orphaned_totals),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Foreign key relationships validation: %', test_status;
END $$;

-- ==================================================================================
-- SCHEMA STRUCTURE VALIDATION
-- ==================================================================================

-- Test 4: Verify consolidated schema structure
DO $$
DECLARE
    raw_data_tables INTEGER;
    core_betting_tables INTEGER;
    analytics_tables INTEGER;
    operational_tables INTEGER;
    expected_schemas INTEGER := 4;
    actual_schemas INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    -- Count tables in each consolidated schema
    SELECT COUNT(*) INTO raw_data_tables 
    FROM information_schema.tables 
    WHERE table_schema = 'raw_data';
    
    SELECT COUNT(*) INTO core_betting_tables 
    FROM information_schema.tables 
    WHERE table_schema = 'core_betting';
    
    SELECT COUNT(*) INTO analytics_tables 
    FROM information_schema.tables 
    WHERE table_schema = 'analytics';
    
    SELECT COUNT(*) INTO operational_tables 
    FROM information_schema.tables 
    WHERE table_schema = 'operational';
    
    -- Count consolidated schemas
    SELECT COUNT(*) INTO actual_schemas
    FROM information_schema.schemata 
    WHERE schema_name IN ('raw_data', 'core_betting', 'analytics', 'operational');
    
    -- Validate schema structure
    IF actual_schemas != expected_schemas THEN
        test_status := 'failed';
        error_msg := format('Missing consolidated schemas: expected=%s, actual=%s', expected_schemas, actual_schemas);
    ELSIF raw_data_tables < 5 THEN
        test_status := 'failed';
        error_msg := format('Insufficient raw_data tables: %s', raw_data_tables);
    ELSIF core_betting_tables < 10 THEN
        test_status := 'failed';
        error_msg := format('Insufficient core_betting tables: %s', core_betting_tables);
    ELSIF analytics_tables < 5 THEN
        test_status := 'failed';
        error_msg := format('Insufficient analytics tables: %s', analytics_tables);
    ELSIF operational_tables < 15 THEN
        test_status := 'failed';
        error_msg := format('Insufficient operational tables: %s', operational_tables);
    END IF;
    
    PERFORM log_validation_result(
        'schema_structure', 
        'consolidated_schema_completeness',
        'Verify all consolidated schemas and tables exist',
        'All 4 consolidated schemas with expected table counts',
        format('Schemas: %s, Raw: %s, Core: %s, Analytics: %s, Operational: %s', 
               actual_schemas, raw_data_tables, core_betting_tables, analytics_tables, operational_tables),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Consolidated schema structure validation: %', test_status;
END $$;

-- ==================================================================================
-- FUNCTIONAL VALIDATION
-- ==================================================================================

-- Test 5: Verify cross-schema queries work
DO $$
DECLARE
    cross_schema_result INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    -- Test simple cross-schema query
    SELECT COUNT(*) INTO cross_schema_result
    FROM curated.games_complete g
    JOIN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' ml ON g.id = ml.game_id
    WHERE g.game_date >= CURRENT_DATE - INTERVAL '30 days';
    
    IF cross_schema_result < 10 THEN
        test_status := 'failed';
        error_msg := format('Cross-schema query returned insufficient results: %s', cross_schema_result);
    END IF;
    
    PERFORM log_validation_result(
        'functionality', 
        'cross_schema_queries',
        'Test complex cross-schema join queries',
        'Should return substantial results for recent games',
        format('Cross-schema query result count: %s', cross_schema_result),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Cross-schema query validation: %', test_status;
END $$;

-- Test 6: Verify operational schema functionality
DO $$
DECLARE
    strategy_configs INTEGER;
    threshold_configs INTEGER;
    migration_logs INTEGER;
    test_status VARCHAR(20) := 'passed';
    error_msg TEXT := '';
BEGIN
    SELECT COUNT(*) INTO strategy_configs FROM operational.strategy_configurations;
    SELECT COUNT(*) INTO threshold_configs FROM operational.threshold_configurations;
    SELECT COUNT(*) INTO migration_logs FROM operational.strategy_migration_log;
    
    IF strategy_configs < 50 THEN
        test_status := 'failed';
        error_msg := format('Insufficient strategy configurations: %s', strategy_configs);
    ELSIF threshold_configs < 10 THEN
        test_status := 'failed';
        error_msg := format('Insufficient threshold configurations: %s', threshold_configs);
    ELSIF migration_logs < 5 THEN
        test_status := 'failed';
        error_msg := format('Insufficient migration logs: %s', migration_logs);
    END IF;
    
    PERFORM log_validation_result(
        'functionality', 
        'operational_schema_functionality',
        'Verify operational schema has migrated data',
        'Strategy configs>50, Threshold configs>10, Migration logs>5',
        format('Strategy configs: %s, Threshold configs: %s, Migration logs: %s', 
               strategy_configs, threshold_configs, migration_logs),
        test_status,
        error_msg
    );
    
    RAISE NOTICE 'Operational schema functionality validation: %', test_status;
END $$;

-- ==================================================================================
-- VALIDATION SUMMARY
-- ==================================================================================

DO $$
DECLARE
    total_tests INTEGER;
    passed_tests INTEGER;
    failed_tests INTEGER;
    pass_rate NUMERIC;
    validation_summary TEXT;
BEGIN
    -- Count test results
    SELECT COUNT(*) INTO total_tests FROM operational.final_validation_results;
    SELECT COUNT(*) INTO passed_tests FROM operational.final_validation_results WHERE status = 'passed';
    SELECT COUNT(*) INTO failed_tests FROM operational.final_validation_results WHERE status = 'failed';
    
    pass_rate := CASE WHEN total_tests > 0 THEN (passed_tests::NUMERIC / total_tests::NUMERIC) * 100 ELSE 0 END;
    
    RAISE NOTICE 'Final Validation Results:';
    RAISE NOTICE 'Total tests: %', total_tests;
    RAISE NOTICE 'Passed tests: %', passed_tests;
    RAISE NOTICE 'Failed tests: %', failed_tests;
    RAISE NOTICE 'Pass rate: %%%', ROUND(pass_rate, 1);
    
    -- Show failed tests if any
    IF failed_tests > 0 THEN
        RAISE NOTICE 'FAILED TESTS:';
        FOR validation_summary IN 
            SELECT format('- %s.%s: %s', test_category, test_name, COALESCE(error_message, 'Unknown error'))
            FROM operational.final_validation_results 
            WHERE status = 'failed'
            ORDER BY test_category, test_name
        LOOP
            RAISE NOTICE '%', validation_summary;
        END LOOP;
    END IF;
    
    -- Overall validation status
    IF failed_tests = 0 THEN
        RAISE NOTICE 'VALIDATION STATUS: ✅ ALL TESTS PASSED - SYSTEM READY FOR CLEANUP';
    ELSE
        RAISE NOTICE 'VALIDATION STATUS: ❌ SOME TESTS FAILED - REVIEW BEFORE CLEANUP';
    END IF;
END $$; 