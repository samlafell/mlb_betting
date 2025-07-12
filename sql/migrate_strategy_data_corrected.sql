-- ==================================================================================
-- MLB Sharp Betting System - Strategy Data Migration (Corrected)
-- ==================================================================================
-- 
-- This script migrates strategy data from multiple schemas to the consolidated schema:
-- - tracking.* → operational.* (strategy tracking and recommendations)
-- - backtesting.* → operational.* (strategy configurations and performance)
-- - validation.* → operational.* (strategy validation)
--
-- NON-DESTRUCTIVE: Preserves source tables for validation
-- CORRECTED: Uses proper column mappings based on actual table structures
-- ==================================================================================

-- Create migration tracking
CREATE TABLE IF NOT EXISTS operational.strategy_migration_log (
    id SERIAL PRIMARY KEY,
    migration_step VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    records_migrated INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started',
    error_message TEXT,
    notes TEXT
);

-- Migration logging function
CREATE OR REPLACE FUNCTION log_strategy_migration(
    p_step VARCHAR(100),
    p_source VARCHAR(100),
    p_target VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.strategy_migration_log (
        migration_step, source_table, target_table, 
        records_migrated, completed_at, status, error_message, notes
    ) VALUES (
        p_step, p_source, p_target, 
        p_records, NOW(), p_status, p_error, p_notes
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Strategy Data Migration (Corrected)';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- MIGRATE TRACKING SCHEMA DATA
-- ==================================================================================

-- Migrate tracking.active_strategy_configs
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.active_strategy_configs;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from tracking.active_strategy_configs', source_count;
        
        INSERT INTO operational.strategy_configurations (
            strategy_id, configuration, enabled, created_at, updated_at
        )
        SELECT 
            strategy_id,
            configuration,
            enabled,
            created_at,
            updated_at
        FROM tracking.active_strategy_configs
        ON CONFLICT (strategy_id) DO UPDATE SET
            configuration = EXCLUDED.configuration,
            enabled = EXCLUDED.enabled,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('active_strategy_configs', 'tracking.active_strategy_configs', 'operational.strategy_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % active strategy config records', record_count;
    ELSE
        PERFORM log_strategy_migration('active_strategy_configs', 'tracking.active_strategy_configs', 'operational.strategy_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate tracking.active_high_roi_strategies (skip - no matching target table)
DO $$
DECLARE
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.active_high_roi_strategies;
    
    RAISE NOTICE 'Skipping % records from tracking.active_high_roi_strategies - no matching target table', source_count;
    PERFORM log_strategy_migration('active_high_roi_strategies', 'tracking.active_high_roi_strategies', 'N/A', 0, 'skipped', NULL, 'No matching target table structure');
END $$;

-- Migrate tracking.pre_game_recommendations (skip - no matching target table)
DO $$
DECLARE
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.pre_game_recommendations;
    
    RAISE NOTICE 'Skipping % records from tracking.pre_game_recommendations - no matching target table', source_count;
    PERFORM log_strategy_migration('pre_game_recommendations', 'tracking.pre_game_recommendations', 'N/A', 0, 'skipped', NULL, 'No matching target table structure');
END $$;

-- Migrate tracking.strategy_integration_log (skip - no matching target table)
DO $$
DECLARE
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.strategy_integration_log;
    
    RAISE NOTICE 'Skipping % records from tracking.strategy_integration_log - no matching target table', source_count;
    PERFORM log_strategy_migration('strategy_integration_log', 'tracking.strategy_integration_log', 'N/A', 0, 'skipped', NULL, 'No matching target table structure');
END $$;

-- ==================================================================================
-- MIGRATE BACKTESTING SCHEMA DATA
-- ==================================================================================

-- Migrate backtesting.strategy_configurations
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM backtesting.strategy_configurations;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from backtesting.strategy_configurations', source_count;
        
        INSERT INTO operational.strategy_configurations (
            strategy_id, strategy_name, source_book_type, split_type, 
            configuration, enabled, win_rate, roi_per_100_unit, 
            confidence_threshold, created_at, updated_at
        )
        SELECT 
            'bt_' || id::text as strategy_id,  -- Create unique strategy_id
            strategy_name,
            source_book_type,
            split_type,
            jsonb_build_object(
                'win_rate', win_rate,
                'roi_per_100', roi_per_100,
                'total_bets', total_bets,
                'confidence_level', confidence_level,
                'min_threshold', min_threshold,
                'moderate_threshold', moderate_threshold,
                'high_threshold', high_threshold,
                'max_drawdown', max_drawdown,
                'sharpe_ratio', sharpe_ratio,
                'kelly_criterion', kelly_criterion
            ) as configuration,
            COALESCE(is_active, true) as enabled,
            win_rate,
            roi_per_100 as roi_per_100_unit,
            CASE 
                WHEN confidence_level = 'high' THEN high_threshold
                WHEN confidence_level = 'moderate' THEN moderate_threshold
                ELSE min_threshold
            END as confidence_threshold,
            created_at,
            last_updated as updated_at
        FROM backtesting.strategy_configurations
        ON CONFLICT (strategy_id) DO UPDATE SET
            strategy_name = EXCLUDED.strategy_name,
            source_book_type = EXCLUDED.source_book_type,
            split_type = EXCLUDED.split_type,
            configuration = EXCLUDED.configuration,
            enabled = EXCLUDED.enabled,
            win_rate = EXCLUDED.win_rate,
            roi_per_100_unit = EXCLUDED.roi_per_100_unit,
            confidence_threshold = EXCLUDED.confidence_threshold,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('backtesting_strategy_configurations', 'backtesting.strategy_configurations', 'operational.strategy_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % backtesting strategy config records', record_count;
    ELSE
        PERFORM log_strategy_migration('backtesting_strategy_configurations', 'backtesting.strategy_configurations', 'operational.strategy_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate backtesting.threshold_configurations
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM backtesting.threshold_configurations;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from backtesting.threshold_configurations', source_count;
        
        INSERT INTO operational.threshold_configurations (
            source, strategy_type, high_confidence_threshold, 
            moderate_confidence_threshold, low_confidence_threshold,
            roi_threshold, win_rate_threshold, created_at, updated_at
        )
        SELECT 
            source,
            strategy_type,
            high_confidence_threshold,
            moderate_confidence_threshold,
            minimum_threshold as low_confidence_threshold,
            steam_threshold as roi_threshold,  -- Map steam_threshold to roi_threshold
            min_win_rate as win_rate_threshold,
            created_at,
            last_validated as updated_at
        FROM backtesting.threshold_configurations;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('threshold_configurations', 'backtesting.threshold_configurations', 'operational.threshold_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % threshold configuration records', record_count;
    ELSE
        PERFORM log_strategy_migration('threshold_configurations', 'backtesting.threshold_configurations', 'operational.threshold_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Skip other backtesting tables that don't have matching target tables
DO $$
DECLARE
    skip_tables TEXT[] := ARRAY[
        'orchestrator_update_triggers',
        'recent_config_changes',
        'standardization_audit_log',
        'strategy_config_history'
    ];
    table_name TEXT;
    source_count INTEGER;
BEGIN
    FOREACH table_name IN ARRAY skip_tables
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM backtesting.%I', table_name) INTO source_count;
        RAISE NOTICE 'Skipping % records from backtesting.% - no matching target table', source_count, table_name;
        PERFORM log_strategy_migration(table_name, 'backtesting.' || table_name, 'N/A', 0, 'skipped', NULL, 'No matching target table structure');
    END LOOP;
END $$;

-- ==================================================================================
-- MIGRATION VALIDATION
-- ==================================================================================

DO $$
DECLARE
    total_migrated INTEGER := 0;
    strategy_configs INTEGER;
    threshold_configs INTEGER;
BEGIN
    -- Count migrated records
    SELECT COUNT(*) INTO strategy_configs FROM operational.strategy_configurations;
    SELECT COUNT(*) INTO threshold_configs FROM operational.threshold_configurations;
    
    total_migrated := strategy_configs + threshold_configs;
    
    RAISE NOTICE 'Migration Validation Results:';
    RAISE NOTICE 'Strategy configurations: %', strategy_configs;
    RAISE NOTICE 'Threshold configurations: %', threshold_configs;
    RAISE NOTICE 'Total migrated records: %', total_migrated;
    
    -- Log validation results
    PERFORM log_strategy_migration('validation_summary', 'multiple', 'operational.*', 
        total_migrated, 'completed', NULL, 
        format('Strategy configs: %s, Threshold configs: %s', 
               strategy_configs, threshold_configs));
END $$;

-- ==================================================================================
-- COMPLETION SUMMARY
-- ==================================================================================

DO $$ 
DECLARE
    total_migrated INTEGER;
    total_skipped INTEGER;
    migration_summary TEXT;
BEGIN
    -- Calculate totals
    SELECT 
        SUM(CASE WHEN status = 'completed' THEN records_migrated ELSE 0 END),
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END)
    INTO total_migrated, total_skipped
    FROM operational.strategy_migration_log;
    
    RAISE NOTICE 'Strategy Data Migration completed!';
    RAISE NOTICE 'Total records migrated: %', COALESCE(total_migrated, 0);
    RAISE NOTICE 'Total tables skipped: %', COALESCE(total_skipped, 0);
    RAISE NOTICE 'Completion timestamp: %', NOW();
    
    -- Show migration summary
    WITH ordered_logs AS (
        SELECT migration_step, status, records_migrated, id
        FROM operational.strategy_migration_log 
        ORDER BY id
    )
    SELECT string_agg(
        migration_step || ': ' || 
        CASE 
            WHEN status = 'completed' THEN records_migrated || ' records migrated'
            WHEN status = 'skipped' THEN 'skipped (no target table)'
            ELSE 'unknown status'
        END, 
        E'\n'
    ) INTO migration_summary
    FROM ordered_logs;
    
    IF migration_summary IS NOT NULL THEN
        RAISE NOTICE 'Migration summary:';
        RAISE NOTICE '%', migration_summary;
    END IF;
END $$; 