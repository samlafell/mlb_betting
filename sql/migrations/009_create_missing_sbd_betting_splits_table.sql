-- Migration 009: Create Missing SBD Betting Splits Table
-- Purpose: Fix pipeline "processed_at" column missing error by creating the missing raw_data.sbd_betting_splits table
-- Issue: Table was defined in .old migration but never created, causing pipeline failures
-- Date: 2025-07-23

-- ================================
-- Create raw_data.sbd_betting_splits Table
-- ================================

-- This table combines the columns that SBD collectors insert with the processed_at column that pipeline processors expect
CREATE TABLE IF NOT EXISTS raw_data.sbd_betting_splits (
    id BIGSERIAL PRIMARY KEY,
    
    -- Core columns from original design
    external_matchup_id VARCHAR(255),
    raw_response JSONB NOT NULL,
    api_endpoint TEXT,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Pipeline processing column (THIS WAS MISSING!)
    processed_at TIMESTAMPTZ NULL,  -- NULL means not yet processed by pipeline
    
    -- Team information columns from migration 005
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_team_abbr VARCHAR(10),
    away_team_abbr VARCHAR(10),
    home_team_id VARCHAR(50),
    away_team_id VARCHAR(50),
    game_name VARCHAR(150)
);

-- ================================
-- Create Indexes for Performance
-- ================================

-- Core functionality indexes
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_external_matchup_id 
ON raw_data.sbd_betting_splits(external_matchup_id);

CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_collected_at 
ON raw_data.sbd_betting_splits(collected_at);

-- CRITICAL: Pipeline processing index (this was causing the error!)
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_processed_at 
ON raw_data.sbd_betting_splits(processed_at) 
WHERE processed_at IS NULL;  -- Partial index for unprocessed records

-- Team information indexes from migration 005
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_teams 
ON raw_data.sbd_betting_splits(home_team_abbr, away_team_abbr);

CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_game_name 
ON raw_data.sbd_betting_splits(game_name);

-- ================================
-- Add Table Documentation
-- ================================

COMMENT ON TABLE raw_data.sbd_betting_splits IS 
'Raw SBD betting splits data with pipeline processing support. Includes team information extracted from competitors section and processed_at tracking for pipeline flow.';

COMMENT ON COLUMN raw_data.sbd_betting_splits.processed_at IS 
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.sbd_betting_splits.external_matchup_id IS 
'External identifier for the matchup from SBD source.';

COMMENT ON COLUMN raw_data.sbd_betting_splits.raw_response IS 
'Complete raw JSON response from SBD API stored as JSONB for efficient querying.';

-- ================================
-- Grant Permissions
-- ================================

GRANT SELECT, INSERT, UPDATE ON raw_data.sbd_betting_splits TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE raw_data.sbd_betting_splits_id_seq TO PUBLIC;

-- ================================
-- Summary
-- ================================

-- This migration fixes the pipeline error by:
-- 1. Creating the missing raw_data.sbd_betting_splits table
-- 2. Including the processed_at column that pipeline processors expect
-- 3. Including all team information columns from migration 005
-- 4. Adding proper indexes including the critical processed_at index
-- 5. Providing comprehensive documentation

-- The table now supports:
-- - SBD collector insertions (external_matchup_id, raw_response, api_endpoint, team fields)
-- - Pipeline processing (processed_at column with proper indexing)
-- - Performance (indexes on key columns)
-- - Data completeness (team information fields)

COMMIT;