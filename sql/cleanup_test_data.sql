-- ==================================================================================
-- MLB Sharp Betting System - Test Data Cleanup Script
-- ==================================================================================
-- 
-- This script safely removes test/benchmark tables from the public schema
-- while preserving valuable production data.
--
-- DESTRUCTIVE OPERATION: This will permanently delete test data
-- ==================================================================================

-- Enable detailed logging
\set ON_ERROR_STOP on

-- Create cleanup log
CREATE TABLE IF NOT EXISTS operational.test_data_cleanup_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    records_deleted INTEGER DEFAULT 0,
    cleanup_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'completed',
    notes TEXT
);

-- Cleanup logging function
CREATE OR REPLACE FUNCTION log_test_cleanup(
    p_table VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.test_data_cleanup_log (
        table_name, records_deleted, status, notes
    ) VALUES (
        p_table, p_records, p_status, p_notes
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Test Data Cleanup for Public Schema';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- REMOVE TEST/BENCHMARK TABLES
-- ==================================================================================

-- Remove benchmark_performance table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.benchmark_performance;
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Removing % records from public.benchmark_performance', record_count;
        DROP TABLE IF EXISTS public.benchmark_performance CASCADE;
        PERFORM log_test_cleanup('public.benchmark_performance', record_count, 'completed', 'Test performance data removed');
    ELSE
        PERFORM log_test_cleanup('public.benchmark_performance', 0, 'completed', 'Table was empty');
    END IF;
END $$;

-- Remove benchmark_test table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.benchmark_test;
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Removing % records from public.benchmark_test', record_count;
        DROP TABLE IF EXISTS public.benchmark_test CASCADE;
        PERFORM log_test_cleanup('public.benchmark_test', record_count, 'completed', 'Test benchmark data removed');
    ELSE
        PERFORM log_test_cleanup('public.benchmark_test', 0, 'completed', 'Table was empty');
    END IF;
END $$;

-- Remove concurrency_test table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.concurrency_test;
    
    RAISE NOTICE 'Removing % records from public.concurrency_test', record_count;
    DROP TABLE IF EXISTS public.concurrency_test CASCADE;
    PERFORM log_test_cleanup('public.concurrency_test', record_count, 'completed', 'Concurrency test data removed');
END $$;

-- Remove performance_test table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.performance_test;
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Removing % records from public.performance_test', record_count;
        DROP TABLE IF EXISTS public.performance_test CASCADE;
        PERFORM log_test_cleanup('public.performance_test', record_count, 'completed', 'Performance test data removed');
    ELSE
        PERFORM log_test_cleanup('public.performance_test', 0, 'completed', 'Table was empty');
    END IF;
END $$;

-- Remove test_parallel table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.test_parallel;
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Removing % records from public.test_parallel', record_count;
        DROP TABLE IF EXISTS public.test_parallel CASCADE;
        PERFORM log_test_cleanup('public.test_parallel', record_count, 'completed', 'Parallel test data removed');
    ELSE
        PERFORM log_test_cleanup('public.test_parallel', 0, 'completed', 'Table was empty');
    END IF;
END $$;

-- Remove test_table
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.test_table;
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Removing % records from public.test_table', record_count;
        DROP TABLE IF EXISTS public.test_table CASCADE;
        PERFORM log_test_cleanup('public.test_table', record_count, 'completed', 'Generic test table removed');
    ELSE
        PERFORM log_test_cleanup('public.test_table', 0, 'completed', 'Table was empty');
    END IF;
END $$;

-- ==================================================================================
-- VERIFY REMAINING TABLES IN PUBLIC SCHEMA
-- ==================================================================================

DO $$
DECLARE
    remaining_tables TEXT;
    table_count INTEGER;
BEGIN
    -- Get list of remaining tables in public schema
    SELECT string_agg(table_name, ', '), COUNT(*)
    INTO remaining_tables, table_count
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';
    
    RAISE NOTICE 'Test data cleanup completed successfully!';
    RAISE NOTICE 'Remaining tables in public schema (%): %', table_count, remaining_tables;
    
    -- Log summary
    PERFORM log_test_cleanup('CLEANUP_SUMMARY', table_count, 'completed', 
        'Test data cleanup completed. Remaining tables: ' || remaining_tables);
END $$;

-- ==================================================================================
-- CLEANUP COMPLETION SUMMARY
-- ==================================================================================

DO $$ 
DECLARE
    total_deleted INTEGER;
    cleanup_summary TEXT;
BEGIN
    -- Calculate total deleted records
    SELECT SUM(records_deleted) INTO total_deleted 
    FROM operational.test_data_cleanup_log 
    WHERE table_name != 'CLEANUP_SUMMARY';
    
    -- Generate summary
    SELECT string_agg(
        table_name || ': ' || records_deleted || ' records', 
        E'\n'
    ) INTO cleanup_summary
    FROM operational.test_data_cleanup_log 
    WHERE table_name != 'CLEANUP_SUMMARY'
    ORDER BY id;
    
    RAISE NOTICE 'Test Data Cleanup Summary:';
    RAISE NOTICE 'Total records removed: %', COALESCE(total_deleted, 0);
    RAISE NOTICE 'Tables cleaned:';
    RAISE NOTICE '%', cleanup_summary;
    RAISE NOTICE 'Completion timestamp: %', NOW();
END $$; 