-- ==================================================================================
-- MLB Sharp Betting System - Simple Schema Cleanup
-- ==================================================================================
-- 
-- This script removes legacy schemas and tables after successful validation.
-- This is a DESTRUCTIVE operation that consolidates the database to the final
-- 4-schema architecture: raw_data, core_betting, analytics, operational
--
-- ⚠️  WARNING: This operation is IRREVERSIBLE - ensure all validations passed!
-- ==================================================================================

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Final Schema Cleanup';
    RAISE NOTICE 'Timestamp: %', NOW();
    RAISE NOTICE 'WARNING: This operation is DESTRUCTIVE and IRREVERSIBLE!';
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
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop action.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS action CASCADE;
    RAISE NOTICE 'Dropped ACTION schema (% tables removed)', table_count;
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
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop splits.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS splits CASCADE;
    RAISE NOTICE 'Dropped SPLITS schema (% tables removed)', table_count;
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
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop tracking.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS tracking CASCADE;
    RAISE NOTICE 'Dropped TRACKING schema (% tables removed)', table_count;
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
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop backtesting.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS backtesting CASCADE;
    RAISE NOTICE 'Dropped BACKTESTING schema (% tables removed)', table_count;
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
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop validation.%: %', table_record.table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Drop schema
    DROP SCHEMA IF EXISTS validation CASCADE;
    RAISE NOTICE 'Dropped VALIDATION schema (% tables removed)', table_count;
END $$;

-- ==================================================================================
-- FINAL VERIFICATION
-- ==================================================================================

DO $$
DECLARE
    remaining_schemas INTEGER;
    consolidated_schemas INTEGER;
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
    
    -- Final message
    IF remaining_schemas = 0 THEN
        RAISE NOTICE '🎉 SCHEMA CONSOLIDATION PROJECT COMPLETED SUCCESSFULLY!';
        RAISE NOTICE 'Database reduced from 9+ schemas to 4 consolidated schemas.';
    ELSE
        RAISE NOTICE '⚠️  SCHEMA CONSOLIDATION COMPLETED WITH SOME ISSUES - Review remaining schemas.';
    END IF;
END $$; 