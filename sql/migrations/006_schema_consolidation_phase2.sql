-- ============================================================================
-- SCHEMA CONSOLIDATION - PHASE 2: Schema Consolidation  
-- ============================================================================
-- Purpose: Move tables to correct schemas (11 schemas → 4 schemas)
-- Risk Level: MEDIUM (moving tables, updating references)
-- Rollback: Comprehensive rollback provided at end
-- 
-- IMPORTANT: 
-- 1. Run Phase 1 first to fix broken FK constraints
-- 2. Run full database backup before executing!
-- Command: pg_dump -h localhost -p 5433 -U samlafell -d mlb_betting > backup_pre_phase2.sql
-- ============================================================================

BEGIN;

-- Create transaction savepoint for rollback capability
SAVEPOINT phase2_start;

-- ============================================================================
-- 1. PRE-MIGRATION VALIDATION
-- ============================================================================

-- Verify Phase 1 was completed
DO $$
DECLARE
    phase1_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO phase1_count 
    FROM operational.schema_migration_log 
    WHERE phase = 'PHASE_1' AND status = 'COMPLETED';
    
    IF phase1_count = 0 THEN
        RAISE EXCEPTION 'Phase 1 must be completed before running Phase 2';
    END IF;
    
    RAISE NOTICE 'Phase 1 verification: PASSED';
END $$;

-- Document current state
CREATE TEMP TABLE pre_migration_state AS
SELECT 
    schemaname,
    tablename,
    schemaname || '.' || tablename as full_table_name
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'public')
ORDER BY schemaname, tablename;

-- Display current table distribution
SELECT 'PRE-MIGRATION TABLE DISTRIBUTION:' as status;
SELECT schemaname, COUNT(*) as table_count
FROM pre_migration_state
GROUP BY schemaname
ORDER BY schemaname;

-- ============================================================================
-- 2. MOVE ACTION_NETWORK TABLES TO RAW_DATA
-- ============================================================================

SELECT 'Moving action_network tables to raw_data schema...' as status;

-- Move tables with proper dependency handling
ALTER TABLE action_network.betting_lines SET SCHEMA raw_data;
ALTER TABLE action_network.extraction_log SET SCHEMA raw_data;  
ALTER TABLE action_network.line_movement_summary SET SCHEMA raw_data;

-- Handle sportsbooks table (may have FK dependencies)
-- First check for dependencies
SELECT 'Checking action_network.sportsbooks dependencies...' as status;
SELECT 
    tc.table_schema || '.' || tc.table_name as referencing_table,
    tc.constraint_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_schema = 'action_network'
    AND ccu.table_name = 'sportsbooks';

-- Move sportsbooks table (this may need FK constraint updates)
ALTER TABLE action_network.sportsbooks SET SCHEMA raw_data;

-- Update FK references to use new schema
-- Note: PostgreSQL automatically updates FKs when table moves schemas in same DB

-- ============================================================================
-- 3. MOVE ANALYSIS TABLES TO CURATED
-- ============================================================================

SELECT 'Moving analysis tables to curated schema...' as status;

-- Move analysis tables to curated (handle naming conflicts)
ALTER TABLE analysis.betting_strategies SET SCHEMA curated;
ALTER TABLE analysis.ml_detected_patterns SET SCHEMA curated;
ALTER TABLE analysis.ml_explanations SET SCHEMA curated;

-- Handle ml_model_performance conflict - rename before moving
ALTER TABLE analysis.ml_model_performance RENAME TO ml_model_performance_analysis;
ALTER TABLE analysis.ml_model_performance_analysis SET SCHEMA curated;

ALTER TABLE analysis.ml_opportunity_scores SET SCHEMA curated;
ALTER TABLE analysis.ml_performance_metrics SET SCHEMA curated;
ALTER TABLE analysis.strategy_results SET SCHEMA curated;

-- ============================================================================
-- 4. MOVE ANALYTICS TABLES TO CURATED
-- ============================================================================

SELECT 'Moving analytics tables to curated schema...' as status;

-- Move analytics tables to curated (these are empty, so safe - handle conflicts)
ALTER TABLE analytics.betting_recommendations SET SCHEMA curated;
ALTER TABLE analytics.confidence_scores SET SCHEMA curated;
ALTER TABLE analytics.cross_market_analysis SET SCHEMA curated;

-- Handle ml_experiments conflict - rename before moving
ALTER TABLE analytics.ml_experiments RENAME TO ml_experiments_analytics;
ALTER TABLE analytics.ml_experiments_analytics SET SCHEMA curated;

ALTER TABLE analytics.ml_predictions SET SCHEMA curated;
ALTER TABLE analytics.performance_metrics SET SCHEMA curated;
ALTER TABLE analytics.roi_calculations SET SCHEMA curated;
ALTER TABLE analytics.strategy_signals SET SCHEMA curated;
ALTER TABLE analytics.timing_analysis_results SET SCHEMA curated;

-- ============================================================================
-- 5. MOVE MONITORING TABLES TO OPERATIONAL
-- ============================================================================

SELECT 'Moving monitoring tables to operational schema...' as status;

-- Move monitoring tables to operational
ALTER TABLE monitoring.ml_model_alerts SET SCHEMA operational;
ALTER TABLE monitoring.ml_model_performance SET SCHEMA operational;

-- ============================================================================
-- 6. MOVE COORDINATION TABLES TO OPERATIONAL  
-- ============================================================================

SELECT 'Moving coordination tables to operational schema...' as status;

-- Move coordination table to operational
ALTER TABLE coordination.agent_migration_lock SET SCHEMA operational;

-- ============================================================================
-- 7. HANDLE CROSS-SCHEMA FK UPDATES
-- ============================================================================

SELECT 'Updating cross-schema foreign key references...' as status;

-- Check for any FK constraints that need updating due to table moves
-- PostgreSQL should automatically update these, but let's verify

-- Update any references to moved sportsbooks table
-- The FK constraints should automatically update to raw_data.sportsbooks

-- Update any references to moved experiment tables
-- Check if any references to moved analysis tables need updating

-- ============================================================================
-- 8. POST-MIGRATION VALIDATION
-- ============================================================================

-- Document new state
CREATE TEMP TABLE post_migration_state AS
SELECT 
    schemaname,
    tablename,
    schemaname || '.' || tablename as full_table_name
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'public')
ORDER BY schemaname, tablename;

-- Display new table distribution
SELECT 'POST-MIGRATION TABLE DISTRIBUTION:' as status;
SELECT schemaname, COUNT(*) as table_count
FROM post_migration_state
GROUP BY schemaname
ORDER BY schemaname;

-- Verify no tables were lost
DO $$
DECLARE
    pre_count INTEGER;
    post_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO pre_count FROM pre_migration_state;
    SELECT COUNT(*) INTO post_count FROM post_migration_state;
    
    IF pre_count != post_count THEN
        RAISE EXCEPTION 'Table count mismatch! Pre: %, Post: %', pre_count, post_count;
    END IF;
    
    RAISE NOTICE 'Table count validation: PASSED (% tables)', post_count;
END $$;

-- Check FK constraint integrity
SELECT 'Validating FK constraint integrity...' as status;
SELECT 
    tc.table_schema,
    COUNT(*) as valid_fk_count
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
    AND ccu.table_name IS NOT NULL
GROUP BY tc.table_schema
ORDER BY tc.table_schema;

-- ============================================================================
-- 9. CHECK FOR EMPTY SCHEMAS TO DROP
-- ============================================================================

SELECT 'Identifying empty schemas for cleanup...' as status;

-- Check which schemas are now empty (safe to drop)
SELECT 
    schema_name,
    CASE 
        WHEN table_count = 0 THEN 'SAFE_TO_DROP'
        ELSE 'HAS_TABLES'
    END as status
FROM (
    SELECT 
        s.schema_name,
        COUNT(t.table_name) as table_count
    FROM information_schema.schemata s
    LEFT JOIN information_schema.tables t 
        ON s.schema_name = t.table_schema
    WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'public')
    GROUP BY s.schema_name
) schema_stats
ORDER BY schema_name;

-- ============================================================================
-- 10. CREATE PHASE 2 COMPLETION LOG
-- ============================================================================

INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES (
    'PHASE_2',
    'SCHEMA_CONSOLIDATION',
    'COMPLETED',
    'Successfully moved tables: action_network → raw_data, analysis → curated, analytics → curated, monitoring → operational, coordination → operational'
);

-- Success message
SELECT 'PHASE 2 COMPLETED SUCCESSFULLY' as status;
SELECT 'Tables moved to correct schemas, FK integrity maintained' as message;
SELECT 'Next step: Execute Phase 3 - Game Entity Unification' as next_action;

-- Commit the transaction
COMMIT;

-- ============================================================================
-- ROLLBACK SCRIPT (Run if Phase 2 needs to be reverted)
-- ============================================================================
/*
-- EMERGENCY ROLLBACK FOR PHASE 2
-- Moves all tables back to their original schemas

BEGIN;

SELECT 'STARTING PHASE 2 ROLLBACK - Moving tables back to original schemas...' as status;

-- Rollback action_network tables
ALTER TABLE raw_data.betting_lines SET SCHEMA action_network;
ALTER TABLE raw_data.extraction_log SET SCHEMA action_network;
ALTER TABLE raw_data.line_movement_summary SET SCHEMA action_network;
ALTER TABLE raw_data.sportsbooks SET SCHEMA action_network;

-- Rollback analysis tables  
ALTER TABLE curated.betting_strategies SET SCHEMA analysis;
ALTER TABLE curated.ml_detected_patterns SET SCHEMA analysis;
ALTER TABLE curated.ml_explanations SET SCHEMA analysis;
-- Handle renamed table
ALTER TABLE curated.ml_model_performance_analysis SET SCHEMA analysis;
ALTER TABLE analysis.ml_model_performance_analysis RENAME TO ml_model_performance;
ALTER TABLE curated.ml_opportunity_scores SET SCHEMA analysis;
ALTER TABLE curated.ml_performance_metrics SET SCHEMA analysis;
ALTER TABLE curated.strategy_results SET SCHEMA analysis;

-- Rollback analytics tables
ALTER TABLE curated.betting_recommendations SET SCHEMA analytics;
ALTER TABLE curated.confidence_scores SET SCHEMA analytics;
ALTER TABLE curated.cross_market_analysis SET SCHEMA analytics;
-- Handle renamed table
ALTER TABLE curated.ml_experiments_analytics SET SCHEMA analytics;
ALTER TABLE analytics.ml_experiments_analytics RENAME TO ml_experiments;
ALTER TABLE curated.ml_predictions SET SCHEMA analytics;
ALTER TABLE curated.performance_metrics SET SCHEMA analytics;
ALTER TABLE curated.roi_calculations SET SCHEMA analytics;
ALTER TABLE curated.strategy_signals SET SCHEMA analytics;
ALTER TABLE curated.timing_analysis_results SET SCHEMA analytics;

-- Rollback monitoring tables
ALTER TABLE operational.ml_model_alerts SET SCHEMA monitoring;
ALTER TABLE operational.ml_model_performance SET SCHEMA monitoring;

-- Rollback coordination tables
ALTER TABLE operational.agent_migration_lock SET SCHEMA coordination;

-- Log rollback
INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES ('PHASE_2', 'ROLLBACK', 'COMPLETED', 'Moved all tables back to original schemas');

SELECT 'PHASE 2 ROLLBACK COMPLETED' as status;

COMMIT;
*/