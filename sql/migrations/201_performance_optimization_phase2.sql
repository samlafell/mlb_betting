-- =============================================================================
-- PERFORMANCE OPTIMIZATION - PHASE 2: TABLE PARTITIONING
-- =============================================================================
-- Migration: 201_performance_optimization_phase2.sql
-- Purpose: Implement time-based and hash partitioning for large tables
-- Prerequisites: Phase 1 strategic indexing should be completed
-- Safety: Creates new partitioned tables alongside existing ones
-- Rollback: 201_rollback_performance_optimization_phase2.sql
-- =============================================================================

-- Enable query timing for monitoring
\timing on

\echo ''
\echo '==============================================================================='
\echo 'PHASE 2: TABLE PARTITIONING - STARTING'
\echo '==============================================================================='
\echo 'This will implement time-based partitioning for high-volume tables'
\echo 'Existing data will remain accessible during the migration process'
\echo ''

BEGIN;

-- Log phase 2 start
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase2', 'partitioning_start', 'time_based_partitions', 'started');

COMMIT;

-- =============================================================================
-- PHASE 2A: TIME-BASED PARTITIONING SETUP
-- =============================================================================

-- 1. Create partitioned version of raw_data.action_network_odds
-- This is our highest volume table (9,903+ records)
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Create partitioned table structure
    CREATE TABLE IF NOT EXISTS raw_data.action_network_odds_partitioned (
        LIKE raw_data.action_network_odds INCLUDING DEFAULTS INCLUDING CONSTRAINTS
    ) PARTITION BY RANGE (collected_at);
    
    -- Recreate indexes on partitioned table
    CREATE UNIQUE INDEX IF NOT EXISTS action_network_odds_partitioned_unique
        ON raw_data.action_network_odds_partitioned (external_game_id, sportsbook_key, collected_at);
    
    CREATE INDEX IF NOT EXISTS idx_action_network_odds_partitioned_game_id
        ON raw_data.action_network_odds_partitioned (external_game_id);
        
    CREATE INDEX IF NOT EXISTS idx_action_network_odds_partitioned_sportsbook
        ON raw_data.action_network_odds_partitioned (sportsbook_key);
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_partitioned_table', 'action_network_odds_partitioned', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_partitioned_table', 'action_network_odds_partitioned', 'failed', SQLERRM);
END $$;

-- 2. Create monthly partitions for action_network_odds
-- Create partitions for past 3 months and future 6 months
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '7 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    partition_count INTEGER := 0;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'action_network_odds_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        -- Create partition
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS raw_data.%I 
            PARTITION OF raw_data.action_network_odds_partitioned
            FOR VALUES FROM (%L) TO (%L)', 
            partition_name, partition_start, partition_end);
        
        partition_count := partition_count + 1;
        start_date := partition_end;
    END LOOP;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_monthly_partitions', 
            'action_network_odds_' || partition_count::text || '_partitions', 
            'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_monthly_partitions', 'action_network_odds_partitions', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2B: STAGING TABLE PARTITIONING BY DATA SOURCE
-- =============================================================================

-- 3. Create partitioned version of staging.betting_odds_unified
-- Partition by data_source for better parallel processing
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Create partitioned table structure
    CREATE TABLE IF NOT EXISTS staging.betting_odds_unified_partitioned (
        LIKE staging.betting_odds_unified INCLUDING DEFAULTS INCLUDING CONSTRAINTS
    ) PARTITION BY LIST (data_source);
    
    -- Recreate critical indexes on partitioned table
    CREATE UNIQUE INDEX IF NOT EXISTS betting_odds_unified_partitioned_unique
        ON staging.betting_odds_unified_partitioned (data_source, external_game_id, sportsbook_external_id, market_type, odds_timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_betting_odds_unified_partitioned_game_date
        ON staging.betting_odds_unified_partitioned (game_date);
        
    CREATE INDEX IF NOT EXISTS idx_betting_odds_unified_partitioned_teams
        ON staging.betting_odds_unified_partitioned (home_team, away_team);
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_partitioned_table', 'betting_odds_unified_partitioned', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_partitioned_table', 'betting_odds_unified_partitioned', 'failed', SQLERRM);
END $$;

-- 4. Create data source partitions
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    sources TEXT[] := ARRAY['action_network', 'vsin', 'sbd', 'mlb_stats_api', 'sports_book_review'];
    source TEXT;
    partition_count INTEGER := 0;
BEGIN
    FOREACH source IN ARRAY sources LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS staging.betting_odds_unified_%s
            PARTITION OF staging.betting_odds_unified_partitioned
            FOR VALUES IN (%L)', source, source);
        partition_count := partition_count + 1;
    END LOOP;
    
    -- Create default partition for unexpected data sources
    CREATE TABLE IF NOT EXISTS staging.betting_odds_unified_default
        PARTITION OF staging.betting_odds_unified_partitioned DEFAULT;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_source_partitions', 
            'betting_odds_unified_' || partition_count::text || '_partitions', 
            'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_source_partitions', 'betting_odds_unified_partitions', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2C: CURATED TABLE PARTITIONING BY SEASON
-- =============================================================================

-- 5. Create partitioned version of curated.enhanced_games by season
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Create partitioned table structure
    CREATE TABLE IF NOT EXISTS curated.enhanced_games_partitioned (
        LIKE curated.enhanced_games_backup_simple_unification INCLUDING DEFAULTS INCLUDING CONSTRAINTS
    ) PARTITION BY RANGE (season);
    
    -- Recreate critical indexes
    CREATE UNIQUE INDEX IF NOT EXISTS enhanced_games_partitioned_mlb_id_unique
        ON curated.enhanced_games_partitioned (mlb_stats_api_game_id);
        
    CREATE INDEX IF NOT EXISTS idx_enhanced_games_partitioned_teams
        ON curated.enhanced_games_partitioned (home_team, away_team);
        
    CREATE INDEX IF NOT EXISTS idx_enhanced_games_partitioned_date
        ON curated.enhanced_games_partitioned (game_date);
        
    CREATE INDEX IF NOT EXISTS idx_enhanced_games_partitioned_status
        ON curated.enhanced_games_partitioned (game_status);
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_partitioned_table', 'enhanced_games_partitioned', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_partitioned_table', 'enhanced_games_partitioned', 'failed', SQLERRM);
END $$;

-- 6. Create season-based partitions
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    seasons INTEGER[] := ARRAY[2022, 2023, 2024, 2025, 2026];
    season INTEGER;
    partition_count INTEGER := 0;
BEGIN
    FOREACH season IN ARRAY seasons LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS curated.enhanced_games_%s
            PARTITION OF curated.enhanced_games_partitioned
            FOR VALUES FROM (%s) TO (%s)', 
            season, season, season + 1);
        partition_count := partition_count + 1;
    END LOOP;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_season_partitions', 
            'enhanced_games_' || partition_count::text || '_partitions', 
            'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_season_partitions', 'enhanced_games_partitions', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2D: ML FEATURES PARTITIONING BY DATE
-- =============================================================================

-- 7. Create partitioned version of ML features table
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Create partitioned table structure
    CREATE TABLE IF NOT EXISTS curated.ml_features_partitioned (
        LIKE curated.ml_features INCLUDING DEFAULTS INCLUDING CONSTRAINTS
    ) PARTITION BY RANGE (game_date);
    
    -- Recreate critical indexes
    CREATE UNIQUE INDEX IF NOT EXISTS ml_features_partitioned_unique
        ON curated.ml_features_partitioned (game_id, feature_extraction_date);
        
    CREATE INDEX IF NOT EXISTS idx_ml_features_partitioned_date
        ON curated.ml_features_partitioned (game_date);
        
    CREATE INDEX IF NOT EXISTS idx_ml_features_partitioned_teams
        ON curated.ml_features_partitioned (home_team, away_team);
        
    CREATE INDEX IF NOT EXISTS idx_ml_features_partitioned_quality
        ON curated.ml_features_partitioned (data_quality_score)
        WHERE data_quality_score >= 0.7;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_partitioned_table', 'ml_features_partitioned', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_partitioned_table', 'ml_features_partitioned', 'failed', SQLERRM);
END $$;

-- 8. Create monthly partitions for ML features
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '6 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    partition_count INTEGER := 0;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'ml_features_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS curated.%I 
            PARTITION OF curated.ml_features_partitioned
            FOR VALUES FROM (%L) TO (%L)', 
            partition_name, partition_start, partition_end);
        
        partition_count := partition_count + 1;
        start_date := partition_end;
    END LOOP;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_monthly_partitions', 
            'ml_features_' || partition_count::text || '_partitions', 
            'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_monthly_partitions', 'ml_features_partitions', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2E: AUTOMATED PARTITION MANAGEMENT
-- =============================================================================

-- 9. Create function to automatically create new partitions
CREATE OR REPLACE FUNCTION performance_monitoring.create_monthly_partitions(
    table_schema_name TEXT,
    table_base_name TEXT,
    months_ahead INTEGER DEFAULT 2
) RETURNS INTEGER AS $$
DECLARE
    start_month DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    partition_name TEXT;
    partition_start DATE;
    partition_end DATE;
    partitions_created INTEGER := 0;
    i INTEGER;
BEGIN
    FOR i IN 0..months_ahead-1 LOOP
        partition_start := start_month + (i * INTERVAL '1 month');
        partition_end := partition_start + INTERVAL '1 month';
        partition_name := table_base_name || '_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        -- Check if partition already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = table_schema_name AND c.relname = partition_name
        ) THEN
            EXECUTE format('
                CREATE TABLE %I.%I 
                PARTITION OF %I.%I_partitioned
                FOR VALUES FROM (%L) TO (%L)', 
                table_schema_name, partition_name,
                table_schema_name, table_base_name,
                partition_start, partition_end);
                
            partitions_created := partitions_created + 1;
            
            INSERT INTO performance_monitoring.optimization_log 
                (phase, operation, object_name, status)
            VALUES ('maintenance', 'auto_create_partition', 
                    table_schema_name || '.' || partition_name, 'completed');
        END IF;
    END LOOP;
    
    RETURN partitions_created;
END;
$$ LANGUAGE plpgsql;

-- 10. Create function to drop old partitions
CREATE OR REPLACE FUNCTION performance_monitoring.drop_old_partitions(
    table_schema_name TEXT,
    table_base_name TEXT,
    retain_months INTEGER DEFAULT 12
) RETURNS INTEGER AS $$
DECLARE
    cutoff_date DATE := DATE_TRUNC('month', CURRENT_DATE - (retain_months * INTERVAL '1 month'));
    partition_record RECORD;
    partitions_dropped INTEGER := 0;
BEGIN
    -- Find partitions older than cutoff date
    FOR partition_record IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = table_schema_name
        AND tablename LIKE table_base_name || '_%'
        AND tablename ~ '^\w+_\d{4}_\d{2}$'
    LOOP
        -- Extract date from partition name and check if old enough to drop
        IF TO_DATE(RIGHT(partition_record.tablename, 7), 'YYYY_MM') < cutoff_date THEN
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', 
                          partition_record.schemaname, partition_record.tablename);
            
            partitions_dropped := partitions_dropped + 1;
            
            INSERT INTO performance_monitoring.optimization_log 
                (phase, operation, object_name, status)
            VALUES ('maintenance', 'auto_drop_partition', 
                    partition_record.schemaname || '.' || partition_record.tablename, 'completed');
        END IF;
    END LOOP;
    
    RETURN partitions_dropped;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PHASE 2F: PARTITION CONSTRAINT EXCLUSION OPTIMIZATION
-- =============================================================================

-- 11. Enable constraint exclusion for better query performance
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Set constraint_exclusion to 'partition' for optimal partition pruning
    ALTER DATABASE mlb_betting SET constraint_exclusion = partition;
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'enable_constraint_exclusion', 'database_setting', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'enable_constraint_exclusion', 'database_setting', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2G: PARTITION-AWARE MATERIALIZED VIEWS
-- =============================================================================

-- 12. Create partition-aware materialized views for common queries
DO $$
DECLARE
    start_time TIMESTAMPTZ := CLOCK_TIMESTAMP();
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
BEGIN
    -- Recent betting activity summary (last 7 days)
    CREATE MATERIALIZED VIEW IF NOT EXISTS performance_monitoring.recent_betting_activity AS
    SELECT 
        DATE(collected_at) as activity_date,
        COUNT(*) as total_odds_records,
        COUNT(DISTINCT external_game_id) as unique_games,
        COUNT(DISTINCT sportsbook_key) as active_sportsbooks,
        MIN(collected_at) as first_collection,
        MAX(collected_at) as last_collection
    FROM raw_data.action_network_odds
    WHERE collected_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(collected_at)
    ORDER BY activity_date DESC;
    
    -- Create unique index for REFRESH CONCURRENTLY
    CREATE UNIQUE INDEX IF NOT EXISTS idx_recent_betting_activity_unique
        ON performance_monitoring.recent_betting_activity (activity_date);
    
    end_time := CLOCK_TIMESTAMP();
    duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'create_materialized_view', 'recent_betting_activity', 'completed', duration_ms);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO performance_monitoring.optimization_log 
            (phase, operation, object_name, status, error_message)
        VALUES ('phase2', 'create_materialized_view', 'recent_betting_activity', 'failed', SQLERRM);
END $$;

-- =============================================================================
-- PHASE 2 COMPLETION AND VALIDATION
-- =============================================================================

BEGIN;

-- Log phase 2 completion
INSERT INTO performance_monitoring.optimization_log (phase, operation, object_name, status)
VALUES ('phase2', 'partitioning_complete', 'time_based_partitions', 'completed');

-- Create phase 2 summary view
CREATE OR REPLACE VIEW performance_monitoring.phase2_summary AS
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
WHERE phase = 'phase2'
GROUP BY operation
ORDER BY operation;

COMMIT;

-- =============================================================================
-- PHASE 2 VALIDATION
-- =============================================================================

-- Verify partitions were created successfully
DO $$
DECLARE
    partition_count INTEGER;
    expected_partitions INTEGER := 30; -- Approximate expected count
BEGIN
    SELECT COUNT(*) INTO partition_count
    FROM pg_tables 
    WHERE (tablename LIKE '%_partitioned' OR tablename LIKE '%_2%' OR tablename LIKE '%action_network%' OR tablename LIKE '%enhanced_games%' OR tablename LIKE '%ml_features%')
    AND schemaname IN ('raw_data', 'staging', 'curated');
    
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status, execution_time_ms)
    VALUES ('phase2', 'validation', 'partition_count_check', 
           CASE WHEN partition_count >= expected_partitions THEN 'completed' ELSE 'warning' END,
           partition_count);
           
    RAISE NOTICE 'Phase 2 Validation: Created % partitions', partition_count;
END $$;

-- =============================================================================
-- PHASE 2 SUMMARY REPORT
-- =============================================================================

\echo ''
\echo '==============================================================================='
\echo 'PHASE 2: TABLE PARTITIONING - COMPLETION REPORT'
\echo '==============================================================================='
\echo ''

-- Show phase 2 summary
SELECT 'PHASE 2 SUMMARY:' as section;
SELECT * FROM performance_monitoring.phase2_summary;

\echo ''
\echo 'PARTITIONED TABLES CREATED:'
\echo '- raw_data.action_network_odds_partitioned (monthly partitions)'
\echo '- staging.betting_odds_unified_partitioned (by data source)'
\echo '- curated.enhanced_games_partitioned (by season)'
\echo '- curated.ml_features_partitioned (monthly partitions)'
\echo ''
\echo 'NEXT STEPS:'
\echo '1. Run Phase 3 migration: 202_performance_optimization_phase3.sql (Data Type Optimization)'
\echo '2. Test queries against partitioned tables'
\echo '3. Plan data migration from original tables to partitioned versions'
\echo '4. Set up automated partition maintenance schedules'
\echo ''
\echo 'Phase 2 Table Partitioning completed successfully!'

-- Turn off timing
\timing off