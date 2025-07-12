-- ==================================================================================
-- MLB Sharp Betting System - Final Schema Cleanup
-- ==================================================================================
-- 
-- This script removes legacy schemas and tables after successful validation.
-- This is a DESTRUCTIVE operation that consolidates the database to the final
-- 4-schema architecture: raw_data, core_betting, analytics, operational
--
-- ⚠️  WARNING: This operation is IRREVERSIBLE - ensure all validations passed!
-- ==================================================================================

-- Create cleanup results table
CREATE TABLE IF NOT EXISTS operational.final_cleanup_results (
    id SERIAL PRIMARY KEY,
    cleanup_step VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50),
    table_name VARCHAR(100),
    operation VARCHAR(20) NOT NULL,
    records_affected INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Cleanup logging function
CREATE OR REPLACE FUNCTION log_cleanup_result(
    p_step VARCHAR(100),
    p_schema VARCHAR(50) DEFAULT NULL,
    p_table VARCHAR(100) DEFAULT NULL,
    p_operation VARCHAR(20) DEFAULT 'drop',
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.final_cleanup_results (
        cleanup_step, schema_name, table_name, operation, 
        records_affected, status, error_message
    ) VALUES (
        p_step, p_schema, p_table, p_operation, 
        p_records, p_status, p_error
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Final Schema Cleanup';
    RAISE NOTICE 'Timestamp: %', NOW();
    RAISE NOTICE 'WARNING: This operation is DESTRUCTIVE and IRREVERSIBLE!';
    
    -- Clear previous cleanup results
    DELETE FROM operational.final_cleanup_results;
END $$;

-- ==================================================================================
-- VALIDATION CHECK BEFORE CLEANUP
-- ==================================================================================

DO $$
DECLARE
    validation_passed INTEGER;
    validation_failed INTEGER;
BEGIN
    -- Check if final validation passed
    SELECT COUNT(*) INTO validation_passed 
    FROM operational.final_validation_results 
    WHERE status = 'passed';
    
    SELECT COUNT(*) INTO validation_failed 
    FROM operational.final_validation_results 
    WHERE status = 'failed';
    
    IF validation_failed > 0 THEN
        RAISE EXCEPTION 'CLEANUP ABORTED: % validation tests failed. Review validation results before cleanup.', validation_failed;
    END IF;
    
    IF validation_passed < 6 THEN
        RAISE EXCEPTION 'CLEANUP ABORTED: Insufficient validation tests passed (%). Expected at least 6.', validation_passed;
    END IF;
    
    RAISE NOTICE 'Pre-cleanup validation: % tests passed, % tests failed', validation_passed, validation_failed;
    PERFORM log_cleanup_result('pre_cleanup_validation', NULL, NULL, 'validate', validation_passed, 'passed');
END $$;

-- ==================================================================================
-- REMOVE ACTION SCHEMA (4 tables)
-- ==================================================================================

DO $$
DECLARE
    table_record RECORD;
    total_records INTEGER := 0;
    table_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Removing ACTION schema tables...';
    
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'action'
        ORDER BY table_name
    LOOP
        BEGIN
            -- Count records before dropping
            EXECUTE format('SELECT COUNT(*) FROM action.%I', table_record.table_name) INTO total_records;
            
            -- Drop table
            EXECUTE format('DROP TABLE IF EXISTS action.%I CASCADE', table_record.table_name);
            
            table_count := table_count + 1;
            RAISE NOTICE 'Dropped action.%: % records', table_record.table_name, total_records;
            
            PERFORM log_cleanup_result('drop_action_table', 'action', table_record.table_name, 'drop', total_records, 'completed');
            
        EXCEPTION WHEN OTHERS THEN
            PERFORM log_cleanup_result('drop_action_table', 'action', table_record.table_name, 'drop', 0, 'failed', SQLERRM);
            RAISE NOTICE 'Failed to drop action.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS action CASCADE;
    RAISE NOTICE 'Dropped ACTION schema (% tables removed)', table_count;
    PERFORM log_cleanup_result('drop_action_schema', 'action', NULL, 'drop_schema', table_count, 'completed');
END $$;

-- ==================================================================================
-- REMOVE SPLITS SCHEMA (2 tables)
-- ==================================================================================

DO $$
DECLARE
    table_record RECORD;
    total_records INTEGER := 0;
    table_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Removing SPLITS schema tables...';
    
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'splits'
        ORDER BY table_name
    LOOP
        BEGIN
            -- Count records before dropping
            EXECUTE format('SELECT COUNT(*) FROM splits.%I', table_record.table_name) INTO total_records;
            
            -- Drop table
            EXECUTE format('DROP TABLE IF EXISTS splits.%I CASCADE', table_record.table_name);
            
            table_count := table_count + 1;
            RAISE NOTICE 'Dropped splits.%: % records', table_record.table_name, total_records;
            
            PERFORM log_cleanup_result('drop_splits_table', 'splits', table_record.table_name, 'drop', total_records, 'completed');
            
        EXCEPTION WHEN OTHERS THEN
            PERFORM log_cleanup_result('drop_splits_table', 'splits', table_record.table_name, 'drop', 0, 'failed', SQLERRM);
            RAISE NOTICE 'Failed to drop splits.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS splits CASCADE;
    RAISE NOTICE 'Dropped SPLITS schema (% tables removed)', table_count;
    PERFORM log_cleanup_result('drop_splits_schema', 'splits', NULL, 'drop_schema', table_count, 'completed');
END $$;

-- ==================================================================================
-- REMOVE TRACKING SCHEMA (4 tables)
-- ==================================================================================

DO $$
DECLARE
    table_record RECORD;
    total_records INTEGER := 0;
    table_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Removing TRACKING schema tables...';
    
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'tracking'
        ORDER BY table_name
    LOOP
        BEGIN
            -- Count records before dropping
            EXECUTE format('SELECT COUNT(*) FROM tracking.%I', table_record.table_name) INTO total_records;
            
            -- Drop table
            EXECUTE format('DROP TABLE IF EXISTS tracking.%I CASCADE', table_record.table_name);
            
            table_count := table_count + 1;
            RAISE NOTICE 'Dropped tracking.%: % records', table_record.table_name, total_records;
            
            PERFORM log_cleanup_result('drop_tracking_table', 'tracking', table_record.table_name, 'drop', total_records, 'completed');
            
        EXCEPTION WHEN OTHERS THEN
            PERFORM log_cleanup_result('drop_tracking_table', 'tracking', table_record.table_name, 'drop', 0, 'failed', SQLERRM);
            RAISE NOTICE 'Failed to drop tracking.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS tracking CASCADE;
    RAISE NOTICE 'Dropped TRACKING schema (% tables removed)', table_count;
    PERFORM log_cleanup_result('drop_tracking_schema', 'tracking', NULL, 'drop_schema', table_count, 'completed');
END $$;

-- ==================================================================================
-- REMOVE BACKTESTING SCHEMA (12 tables)
-- ==================================================================================

DO $$
DECLARE
    table_record RECORD;
    total_records INTEGER := 0;
    table_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Removing BACKTESTING schema tables...';
    
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'backtesting'
        ORDER BY table_name
    LOOP
        BEGIN
            -- Count records before dropping
            EXECUTE format('SELECT COUNT(*) FROM backtesting.%I', table_record.table_name) INTO total_records;
            
            -- Drop table
            EXECUTE format('DROP TABLE IF EXISTS backtesting.%I CASCADE', table_record.table_name);
            
            table_count := table_count + 1;
            RAISE NOTICE 'Dropped backtesting.%: % records', table_record.table_name, total_records;
            
            PERFORM log_cleanup_result('drop_backtesting_table', 'backtesting', table_record.table_name, 'drop', total_records, 'completed');
            
        EXCEPTION WHEN OTHERS THEN
            PERFORM log_cleanup_result('drop_backtesting_table', 'backtesting', table_record.table_name, 'drop', 0, 'failed', SQLERRM);
            RAISE NOTICE 'Failed to drop backtesting.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS backtesting CASCADE;
    RAISE NOTICE 'Dropped BACKTESTING schema (% tables removed)', table_count;
    PERFORM log_cleanup_result('drop_backtesting_schema', 'backtesting', NULL, 'drop_schema', table_count, 'completed');
END $$;

-- ==================================================================================
-- REMOVE VALIDATION SCHEMA (1 table)
-- ==================================================================================

DO $$
DECLARE
    table_record RECORD;
    total_records INTEGER := 0;
    table_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Removing VALIDATION schema tables...';
    
    FOR table_record IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'validation'
        ORDER BY table_name
    LOOP
        BEGIN
            -- Count records before dropping
            EXECUTE format('SELECT COUNT(*) FROM validation.%I', table_record.table_name) INTO total_records;
            
            -- Drop table
            EXECUTE format('DROP TABLE IF EXISTS validation.%I CASCADE', table_record.table_name);
            
            table_count := table_count + 1;
            RAISE NOTICE 'Dropped validation.%: % records', table_record.table_name, total_records;
            
            PERFORM log_cleanup_result('drop_validation_table', 'validation', table_record.table_name, 'drop', total_records, 'completed');
            
        EXCEPTION WHEN OTHERS THEN
            PERFORM log_cleanup_result('drop_validation_table', 'validation', table_record.table_name, 'drop', 0, 'failed', SQLERRM);
            RAISE NOTICE 'Failed to drop validation.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS validation CASCADE;
    RAISE NOTICE 'Dropped VALIDATION schema (% tables removed)', table_count;
    PERFORM log_cleanup_result('drop_validation_schema', 'validation', NULL, 'drop_schema', table_count, 'completed');
END $$;

-- ==================================================================================
-- FINAL VERIFICATION
-- ==================================================================================

DO $$
DECLARE
    remaining_schemas INTEGER;
    consolidated_schemas INTEGER;
    total_tables_dropped INTEGER;
    total_records_affected INTEGER;
    final_status TEXT;
BEGIN
    -- Count remaining schemas
    SELECT COUNT(*) INTO remaining_schemas
    FROM information_schema.schemata 
    WHERE schema_name IN ('action', 'splits', 'tracking', 'backtesting', 'validation');
    
    -- Count consolidated schemas
    SELECT COUNT(*) INTO consolidated_schemas
    FROM information_schema.schemata 
    WHERE schema_name IN ('raw_data', 'core_betting', 'analytics', 'operational');
    
    -- Count cleanup results
    SELECT COUNT(*), SUM(records_affected) INTO total_tables_dropped, total_records_affected
    FROM operational.final_cleanup_results 
    WHERE operation = 'drop' AND status = 'completed';
    
    -- Final status
    IF remaining_schemas = 0 AND consolidated_schemas = 4 THEN
        final_status := 'SUCCESS';
        RAISE NOTICE 'CLEANUP COMPLETED SUCCESSFULLY!';
    ELSE
        final_status := 'PARTIAL';
        RAISE NOTICE 'CLEANUP PARTIALLY COMPLETED - Review results';
    END IF;
    
    RAISE NOTICE 'Final Schema State:';
    RAISE NOTICE '- Legacy schemas remaining: %', remaining_schemas;
    RAISE NOTICE '- Consolidated schemas: %', consolidated_schemas;
    RAISE NOTICE '- Tables dropped: %', total_tables_dropped;
    RAISE NOTICE '- Records affected: %', COALESCE(total_records_affected, 0);
    
    PERFORM log_cleanup_result('final_verification', NULL, NULL, 'verify', 
        total_tables_dropped, final_status, 
        format('Legacy: %s, Consolidated: %s, Tables: %s, Records: %s', 
               remaining_schemas, consolidated_schemas, total_tables_dropped, total_records_affected));
END $$;

-- ==================================================================================
-- CLEANUP SUMMARY
-- ==================================================================================

DO $$
DECLARE
    cleanup_summary TEXT;
    total_operations INTEGER;
    successful_operations INTEGER;
    failed_operations INTEGER;
BEGIN
    -- Count operations
    SELECT COUNT(*) INTO total_operations FROM operational.final_cleanup_results;
    SELECT COUNT(*) INTO successful_operations FROM operational.final_cleanup_results WHERE status IN ('completed', 'passed', 'SUCCESS');
    SELECT COUNT(*) INTO failed_operations FROM operational.final_cleanup_results WHERE status = 'failed';
    
    RAISE NOTICE 'Final Cleanup Summary:';
    RAISE NOTICE 'Total operations: %', total_operations;
    RAISE NOTICE 'Successful operations: %', successful_operations;
    RAISE NOTICE 'Failed operations: %', failed_operations;
    
    -- Show detailed results
    RAISE NOTICE 'Detailed Results:';
    FOR cleanup_summary IN 
        SELECT format('- %s: %s (%s)', cleanup_step, status, 
                     CASE WHEN records_affected > 0 THEN records_affected || ' records' ELSE 'N/A' END)
        FROM operational.final_cleanup_results 
        ORDER BY id
    LOOP
        RAISE NOTICE '%', cleanup_summary;
    END LOOP;
    
    -- Final message
    IF failed_operations = 0 THEN
        RAISE NOTICE '🎉 SCHEMA CONSOLIDATION PROJECT COMPLETED SUCCESSFULLY!';
        RAISE NOTICE 'Database reduced from 9+ schemas to 4 consolidated schemas.';
    ELSE
        RAISE NOTICE '⚠️  SCHEMA CONSOLIDATION COMPLETED WITH SOME ISSUES - Review failed operations.';
    END IF;
END $$; 