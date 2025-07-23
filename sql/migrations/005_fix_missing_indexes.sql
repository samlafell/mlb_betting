-- Migration: Fix Missing Indexes Identified in Phase 1 Validation
-- Purpose: Add critical performance indexes missing from the three-tier pipeline
-- Reference: tests/integration/test_three_tier_pipeline_validation.py
-- Created: 2025-07-23

-- ================================
-- Missing Staging Table Indexes
-- ================================

-- Games table indexes (if not already exists)
CREATE INDEX IF NOT EXISTS idx_an_games_external_id ON staging.action_network_games(external_game_id);
CREATE INDEX IF NOT EXISTS idx_an_games_mlb_id ON staging.action_network_games(mlb_stats_api_game_id);

-- ================================
-- Missing Pipeline Execution Log Indexes
-- ================================

-- Pipeline execution log indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_id ON public.pipeline_execution_log(execution_id);

-- Additional performance indexes for pipeline monitoring
CREATE INDEX IF NOT EXISTS idx_pipeline_log_stage_status ON public.pipeline_execution_log(pipeline_stage, status);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_created_at ON public.pipeline_execution_log(created_at);

-- ================================
-- Verify Index Creation
-- ================================

-- Query to verify all expected indexes exist
DO $$
DECLARE 
    missing_indexes TEXT[];
    expected_indexes TEXT[] := ARRAY[
        'idx_an_games_external_id',
        'idx_an_games_mlb_id', 
        'idx_historical_odds_game_id',
        'idx_historical_odds_updated_at',
        'idx_pipeline_log_execution_id'
    ];
    idx TEXT;
    index_exists BOOLEAN;
BEGIN
    FOREACH idx IN ARRAY expected_indexes
    LOOP
        SELECT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE indexname = idx
        ) INTO index_exists;
        
        IF NOT index_exists THEN
            missing_indexes := array_append(missing_indexes, idx);
        END IF;
    END LOOP;
    
    IF array_length(missing_indexes, 1) > 0 THEN
        RAISE WARNING 'Missing indexes still remain: %', array_to_string(missing_indexes, ', ');
    ELSE
        RAISE NOTICE 'All expected indexes are now present';
    END IF;
END $$;

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON INDEX idx_an_games_external_id IS 'Performance index for external game ID lookups in staging.action_network_games';
COMMENT ON INDEX idx_an_games_mlb_id IS 'Performance index for MLB Stats API cross-system integration';
COMMENT ON INDEX idx_pipeline_log_execution_id IS 'Performance index for pipeline execution tracking and monitoring';