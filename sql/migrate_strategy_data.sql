-- ==================================================================================
-- MLB Sharp Betting System - Strategy Data Migration
-- ==================================================================================
-- 
-- This script migrates strategy data from multiple schemas to the consolidated schema:
-- - tracking.* → operational.* (strategy tracking and recommendations)
-- - backtesting.* → operational.* (strategy configurations and performance)
-- - validation.* → operational.* (strategy validation)
--
-- NON-DESTRUCTIVE: Preserves source tables for validation
-- ==================================================================================

-- Enable detailed logging
-- Note: Running via Python psycopg2, not psql

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
    RAISE NOTICE 'Starting Strategy Data Migration';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- MIGRATE TRACKING SCHEMA DATA
-- ==================================================================================

-- Migrate tracking.active_high_roi_strategies
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.active_high_roi_strategies;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from tracking.active_high_roi_strategies', source_count;
        
        -- Check if target table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'operational' AND table_name = 'strategy_performance_tracking'
        ) THEN
            INSERT INTO operational.strategy_performance_tracking (
                strategy_id, performance_metric, metric_value, calculation_date,
                roi_percentage, confidence_score, data_source, notes,
                created_at, updated_at
            )
            SELECT 
                strategy_id,
                'high_roi' as performance_metric,
                roi_percentage::text as metric_value,
                updated_at::date as calculation_date,
                roi_percentage,
                confidence_score,
                'tracking_migration' as data_source,
                notes,
                created_at,
                updated_at
            FROM tracking.active_high_roi_strategies;
        ELSE
            RAISE NOTICE 'Target table operational.strategy_performance_tracking does not exist, skipping migration';
            record_count := 0;
        END IF;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('active_high_roi_strategies', 'tracking.active_high_roi_strategies', 'operational.strategy_performance_tracking', record_count);
        RAISE NOTICE 'Successfully migrated % high ROI strategy records', record_count;
    ELSE
        PERFORM log_strategy_migration('active_high_roi_strategies', 'tracking.active_high_roi_strategies', 'operational.strategy_performance_tracking', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

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
            strategy_id, strategy_name, configuration_data, is_active,
            performance_threshold, confidence_threshold, created_at, updated_at
        )
        SELECT 
            strategy_id,
            strategy_name,
            configuration_data,
            is_active,
            performance_threshold,
            confidence_threshold,
            created_at,
            updated_at
        FROM tracking.active_strategy_configs
        ON CONFLICT (strategy_id) DO UPDATE SET
            strategy_name = EXCLUDED.strategy_name,
            configuration_data = EXCLUDED.configuration_data,
            is_active = EXCLUDED.is_active,
            performance_threshold = EXCLUDED.performance_threshold,
            confidence_threshold = EXCLUDED.confidence_threshold,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('active_strategy_configs', 'tracking.active_strategy_configs', 'operational.strategy_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % active strategy config records', record_count;
    ELSE
        PERFORM log_strategy_migration('active_strategy_configs', 'tracking.active_strategy_configs', 'operational.strategy_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate tracking.pre_game_recommendations
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.pre_game_recommendations;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from tracking.pre_game_recommendations', source_count;
        
        -- Check if target table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'operational' AND table_name = 'betting_recommendations'
        ) THEN
            INSERT INTO operational.betting_recommendations (
                game_id, strategy_id, bet_type, recommendation, confidence_score,
                expected_value, recommended_stake, reasoning, created_at, updated_at
            )
            SELECT 
                game_id,
                strategy_id,
                bet_type,
                recommendation,
                confidence_score,
                expected_value,
                recommended_stake,
                reasoning,
                created_at,
                updated_at
            FROM tracking.pre_game_recommendations;
        ELSE
            RAISE NOTICE 'Target table operational.betting_recommendations does not exist, skipping migration';
            record_count := 0;
        END IF;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('pre_game_recommendations', 'tracking.pre_game_recommendations', 'operational.betting_recommendations', record_count);
        RAISE NOTICE 'Successfully migrated % pre-game recommendation records', record_count;
    ELSE
        PERFORM log_strategy_migration('pre_game_recommendations', 'tracking.pre_game_recommendations', 'operational.betting_recommendations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate tracking.strategy_integration_log
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM tracking.strategy_integration_log;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from tracking.strategy_integration_log', source_count;
        
        -- Check if target table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'operational' AND table_name = 'system_activity_log'
        ) THEN
            INSERT INTO operational.system_activity_log (
                activity_type, entity_type, entity_id, description, 
                metadata, status, created_at
            )
            SELECT 
                'strategy_integration' as activity_type,
                'strategy' as entity_type,
                strategy_id as entity_id,
                operation_type as description,
                jsonb_build_object(
                    'operation_details', operation_details,
                    'status', status,
                    'execution_time', execution_time
                ) as metadata,
                CASE 
                    WHEN status = 'success' THEN 'completed'
                    WHEN status = 'error' THEN 'failed'
                    ELSE 'pending'
                END as status,
                created_at
            FROM tracking.strategy_integration_log;
        ELSE
            RAISE NOTICE 'Target table operational.system_activity_log does not exist, skipping migration';
            record_count := 0;
        END IF;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('strategy_integration_log', 'tracking.strategy_integration_log', 'operational.system_activity_log', record_count);
        RAISE NOTICE 'Successfully migrated % strategy integration log records', record_count;
    ELSE
        PERFORM log_strategy_migration('strategy_integration_log', 'tracking.strategy_integration_log', 'operational.system_activity_log', 0, 'completed', NULL, 'Source table empty');
    END IF;
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
            strategy_id, strategy_name, configuration_data, is_active,
            performance_threshold, confidence_threshold, created_at, updated_at
        )
        SELECT 
            strategy_id,
            strategy_name,
            configuration_data,
            is_active,
            performance_threshold,
            confidence_threshold,
            created_at,
            updated_at
        FROM backtesting.strategy_configurations
        ON CONFLICT (strategy_id) DO UPDATE SET
            strategy_name = EXCLUDED.strategy_name,
            configuration_data = EXCLUDED.configuration_data,
            is_active = EXCLUDED.is_active,
            performance_threshold = EXCLUDED.performance_threshold,
            confidence_threshold = EXCLUDED.confidence_threshold,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('backtesting_strategy_configurations', 'backtesting.strategy_configurations', 'operational.strategy_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % backtesting strategy config records', record_count;
    ELSE
        PERFORM log_strategy_migration('backtesting_strategy_configurations', 'backtesting.strategy_configurations', 'operational.strategy_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate backtesting.orchestrator_update_triggers
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM backtesting.orchestrator_update_triggers;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from backtesting.orchestrator_update_triggers', source_count;
        
        -- Check if target table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'operational' AND table_name = 'system_activity_log'
        ) THEN
            INSERT INTO operational.system_activity_log (
                activity_type, entity_type, entity_id, description, 
                metadata, status, created_at
            )
            SELECT 
                'orchestrator_update' as activity_type,
                'trigger' as entity_type,
                trigger_id as entity_id,
                trigger_event as description,
                jsonb_build_object(
                    'trigger_condition', trigger_condition,
                    'action_taken', action_taken,
                    'execution_time', execution_time
                ) as metadata,
                'completed' as status,
                created_at
            FROM backtesting.orchestrator_update_triggers;
        ELSE
            RAISE NOTICE 'Target table operational.system_activity_log does not exist, skipping migration';
            record_count := 0;
        END IF;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('orchestrator_update_triggers', 'backtesting.orchestrator_update_triggers', 'operational.system_activity_log', record_count);
        RAISE NOTICE 'Successfully migrated % orchestrator update trigger records', record_count;
    ELSE
        PERFORM log_strategy_migration('orchestrator_update_triggers', 'backtesting.orchestrator_update_triggers', 'operational.system_activity_log', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- Migrate backtesting.recent_config_changes
DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO source_count FROM backtesting.recent_config_changes;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from backtesting.recent_config_changes', source_count;
        
        -- Check if target table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'operational' AND table_name = 'system_activity_log'
        ) THEN
            INSERT INTO operational.system_activity_log (
                activity_type, entity_type, entity_id, description, 
                metadata, status, created_at
            )
            SELECT 
                'config_change' as activity_type,
                'strategy' as entity_type,
                strategy_id as entity_id,
                change_type as description,
                jsonb_build_object(
                    'old_config', old_config,
                    'new_config', new_config,
                    'change_reason', change_reason
                ) as metadata,
                'completed' as status,
                created_at
            FROM backtesting.recent_config_changes;
        ELSE
            RAISE NOTICE 'Target table operational.system_activity_log does not exist, skipping migration';
            record_count := 0;
        END IF;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('recent_config_changes', 'backtesting.recent_config_changes', 'operational.system_activity_log', record_count);
        RAISE NOTICE 'Successfully migrated % recent config change records', record_count;
    ELSE
        PERFORM log_strategy_migration('recent_config_changes', 'backtesting.recent_config_changes', 'operational.system_activity_log', 0, 'completed', NULL, 'Source table empty');
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
            threshold_id, threshold_name, threshold_type, threshold_value,
            comparison_operator, is_active, strategy_id, created_at, updated_at
        )
        SELECT 
            threshold_id,
            threshold_name,
            threshold_type,
            threshold_value,
            comparison_operator,
            is_active,
            strategy_id,
            created_at,
            updated_at
        FROM backtesting.threshold_configurations
        ON CONFLICT (threshold_id) DO UPDATE SET
            threshold_name = EXCLUDED.threshold_name,
            threshold_type = EXCLUDED.threshold_type,
            threshold_value = EXCLUDED.threshold_value,
            comparison_operator = EXCLUDED.comparison_operator,
            is_active = EXCLUDED.is_active,
            strategy_id = EXCLUDED.strategy_id,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_strategy_migration('threshold_configurations', 'backtesting.threshold_configurations', 'operational.threshold_configurations', record_count);
        RAISE NOTICE 'Successfully migrated % threshold configuration records', record_count;
    ELSE
        PERFORM log_strategy_migration('threshold_configurations', 'backtesting.threshold_configurations', 'operational.threshold_configurations', 0, 'completed', NULL, 'Source table empty');
    END IF;
END $$;

-- ==================================================================================
-- MIGRATION VALIDATION
-- ==================================================================================

DO $$
DECLARE
    total_migrated INTEGER := 0;
    strategy_configs INTEGER;
    performance_tracking INTEGER;
    betting_recommendations INTEGER;
    activity_logs INTEGER;
    threshold_configs INTEGER;
BEGIN
    -- Count migrated records (handle missing tables)
    SELECT COUNT(*) INTO strategy_configs FROM operational.strategy_configurations;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'strategy_performance_tracking') THEN
        SELECT COUNT(*) INTO performance_tracking FROM operational.strategy_performance_tracking;
    ELSE
        performance_tracking := 0;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'betting_recommendations') THEN
        SELECT COUNT(*) INTO betting_recommendations FROM operational.betting_recommendations;
    ELSE
        betting_recommendations := 0;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'system_activity_log') THEN
        SELECT COUNT(*) INTO activity_logs FROM operational.system_activity_log;
    ELSE
        activity_logs := 0;
    END IF;
    
    SELECT COUNT(*) INTO threshold_configs FROM operational.threshold_configurations;
    
    total_migrated := strategy_configs + performance_tracking + betting_recommendations + activity_logs + threshold_configs;
    
    RAISE NOTICE 'Migration Validation Results:';
    RAISE NOTICE 'Strategy configurations: %', strategy_configs;
    RAISE NOTICE 'Performance tracking: %', performance_tracking;
    RAISE NOTICE 'Betting recommendations: %', betting_recommendations;
    RAISE NOTICE 'Activity logs: %', activity_logs;
    RAISE NOTICE 'Threshold configurations: %', threshold_configs;
    RAISE NOTICE 'Total migrated records: %', total_migrated;
    
    -- Log validation results
    PERFORM log_strategy_migration('validation_summary', 'multiple', 'operational.*', 
        total_migrated, 'completed', NULL, 
        format('Configs: %s, Performance: %s, Recommendations: %s, Logs: %s, Thresholds: %s', 
               strategy_configs, performance_tracking, betting_recommendations, activity_logs, threshold_configs));
END $$;

-- ==================================================================================
-- COMPLETION SUMMARY
-- ==================================================================================

DO $$ 
DECLARE
    total_migrated INTEGER;
    migration_summary TEXT;
BEGIN
    -- Calculate total migrated records
    SELECT SUM(records_migrated) INTO total_migrated 
    FROM operational.strategy_migration_log 
    WHERE status = 'completed';
    
    RAISE NOTICE 'Strategy Data Migration completed successfully!';
    RAISE NOTICE 'Total records processed: %', COALESCE(total_migrated, 0);
    RAISE NOTICE 'Completion timestamp: %', NOW();
    
    -- Show migration summary
    SELECT string_agg(
        migration_step || ': ' || records_migrated || ' records', 
        E'\n'
    ) INTO migration_summary
    FROM operational.strategy_migration_log 
    WHERE status = 'completed' AND records_migrated > 0
    ORDER BY id;
    
    IF migration_summary IS NOT NULL THEN
        RAISE NOTICE 'Migration summary:';
        RAISE NOTICE '%', migration_summary;
    END IF;
END $$; 