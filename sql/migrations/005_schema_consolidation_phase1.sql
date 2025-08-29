-- ============================================================================
-- SCHEMA CONSOLIDATION - PHASE 1: Emergency Stabilization
-- ============================================================================
-- Purpose: Fix broken foreign key constraints to ensure system stability
-- Risk Level: LOW (removing broken constraints only)
-- Rollback: Provided at end of script
-- 
-- IMPORTANT: Run full database backup before executing!
-- Command: pg_dump -h localhost -p 5433 -U samlafell -d mlb_betting > backup_pre_phase1.sql
-- ============================================================================

BEGIN;

-- Create transaction savepoint for rollback capability
SAVEPOINT phase1_start;

-- ============================================================================
-- 1. DOCUMENT CURRENT BROKEN FK CONSTRAINTS
-- ============================================================================

-- Log the broken constraints we're about to fix
CREATE TEMP TABLE broken_fk_log AS
SELECT 
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    'ORPHANED - Missing target table' as issue_type
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
    AND (ccu.table_name IS NULL OR ccu.table_name = '');

-- Display what we found
SELECT 'BROKEN FK CONSTRAINTS TO FIX:' as status;
SELECT * FROM broken_fk_log;

-- ============================================================================
-- 2. FIX ORPHANED ANALYTICS SCHEMA CONSTRAINTS
-- ============================================================================

-- These tables are empty and have broken game_id FK references
SELECT 'Removing orphaned analytics FK constraints...' as status;

-- Remove broken game_id constraints from analytics schema
ALTER TABLE analytics.betting_recommendations 
DROP CONSTRAINT IF EXISTS betting_recommendations_game_id_fkey;

ALTER TABLE analytics.confidence_scores 
DROP CONSTRAINT IF EXISTS confidence_scores_game_id_fkey;

ALTER TABLE analytics.cross_market_analysis 
DROP CONSTRAINT IF EXISTS cross_market_analysis_game_id_fkey;

ALTER TABLE analytics.strategy_signals 
DROP CONSTRAINT IF EXISTS strategy_signals_game_id_fkey;

-- Verify tables are empty (safety check)
DO $$
DECLARE
    rec RECORD;
    table_count INTEGER;
BEGIN
    FOR rec IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'analytics' 
        AND table_name IN ('betting_recommendations', 'confidence_scores', 'cross_market_analysis', 'strategy_signals')
    LOOP
        EXECUTE 'SELECT COUNT(*) FROM analytics.' || rec.table_name INTO table_count;
        IF table_count > 0 THEN
            RAISE EXCEPTION 'Table analytics.% contains % records - cannot safely proceed!', rec.table_name, table_count;
        END IF;
        RAISE NOTICE 'Verified analytics.% is empty (% records)', rec.table_name, table_count;
    END LOOP;
END $$;

-- ============================================================================
-- 3. FIX ORPHANED MONITORING SCHEMA CONSTRAINTS  
-- ============================================================================

-- These tables have broken experiment_id FK references
SELECT 'Removing orphaned monitoring FK constraints...' as status;

ALTER TABLE monitoring.ml_model_alerts 
DROP CONSTRAINT IF EXISTS ml_model_alerts_experiment_id_fkey;

ALTER TABLE monitoring.ml_model_performance 
DROP CONSTRAINT IF EXISTS ml_model_performance_experiment_id_fkey;

-- Check if these tables have data
DO $$
DECLARE
    alert_count INTEGER;
    perf_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO alert_count FROM monitoring.ml_model_alerts;
    SELECT COUNT(*) INTO perf_count FROM monitoring.ml_model_performance;
    
    RAISE NOTICE 'monitoring.ml_model_alerts: % records', alert_count;
    RAISE NOTICE 'monitoring.ml_model_performance: % records', perf_count;
    
    -- If there's data, we need to handle it carefully
    IF alert_count > 0 OR perf_count > 0 THEN
        RAISE WARNING 'Monitoring tables contain data - FK removal may cause issues if experiment_id references are needed';
    END IF;
END $$;

-- ============================================================================
-- 4. VALIDATE PHASE 1 SUCCESS
-- ============================================================================

-- Check that broken constraints are removed
SELECT 'VALIDATION: Checking for remaining broken FK constraints...' as status;

SELECT 
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    'STILL BROKEN' as status
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
    AND (ccu.table_name IS NULL OR ccu.table_name = '');

-- Count remaining FK constraints by schema
SELECT 'VALIDATION: FK constraints by schema after Phase 1:' as status;
SELECT 
    tc.table_schema,
    COUNT(*) as fk_count
FROM information_schema.table_constraints AS tc
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
GROUP BY tc.table_schema
ORDER BY tc.table_schema;

-- ============================================================================
-- 5. CREATE PHASE 1 COMPLETION LOG
-- ============================================================================

-- Log successful completion
CREATE TABLE IF NOT EXISTS operational.schema_migration_log (
    id SERIAL PRIMARY KEY,
    phase VARCHAR(50) NOT NULL,
    operation VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    details TEXT,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES (
    'PHASE_1',
    'EMERGENCY_STABILIZATION',
    'COMPLETED',
    'Removed broken FK constraints from analytics and monitoring schemas'
);

-- Success message
SELECT 'PHASE 1 COMPLETED SUCCESSFULLY' as status;
SELECT 'Broken FK constraints removed, system stabilized' as message;
SELECT 'Next step: Execute Phase 2 - Schema Consolidation' as next_action;

-- Commit the transaction
COMMIT;

-- ============================================================================
-- ROLLBACK SCRIPT (Run if Phase 1 needs to be reverted)
-- ============================================================================
/*
-- EMERGENCY ROLLBACK FOR PHASE 1
-- Only run this if you need to restore the broken FK constraints

BEGIN;

-- Restore broken analytics constraints (will fail, but for completeness)
ALTER TABLE analytics.betting_recommendations 
ADD CONSTRAINT betting_recommendations_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES games(id);  -- This will fail - no games table

ALTER TABLE analytics.confidence_scores 
ADD CONSTRAINT confidence_scores_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES games(id);

ALTER TABLE analytics.cross_market_analysis 
ADD CONSTRAINT cross_market_analysis_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES games(id);

ALTER TABLE analytics.strategy_signals 
ADD CONSTRAINT strategy_signals_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES games(id);

-- Restore broken monitoring constraints  
ALTER TABLE monitoring.ml_model_alerts 
ADD CONSTRAINT ml_model_alerts_experiment_id_fkey 
FOREIGN KEY (experiment_id) REFERENCES experiments(id);

ALTER TABLE monitoring.ml_model_performance 
ADD CONSTRAINT ml_model_performance_experiment_id_fkey 
FOREIGN KEY (experiment_id) REFERENCES experiments(id);

-- Log rollback
INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES ('PHASE_1', 'ROLLBACK', 'COMPLETED', 'Restored broken FK constraints');

COMMIT;

-- NOTE: This rollback will restore the broken state - only use for emergency revert
*/