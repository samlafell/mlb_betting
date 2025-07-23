-- Migration 010: Add processed_at Column to All Raw Data Tables
-- Purpose: Fix persistent "processed_at does not exist" errors by adding column to all raw_data tables
-- Issue: Pipeline processors expect processed_at column on all raw_data tables for tracking processing status
-- Date: 2025-07-23

-- ================================
-- Add processed_at Column to Missing Tables
-- ================================

-- Action Network tables
ALTER TABLE raw_data.action_network_odds 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

ALTER TABLE raw_data.action_network_games 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;  

ALTER TABLE raw_data.action_network_history 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

-- VSIN tables  
ALTER TABLE raw_data.vsin_data 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

-- MLB Stats API tables
ALTER TABLE raw_data.mlb_stats_api_games 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

-- Additional raw_data tables that may be processed by pipeline
ALTER TABLE raw_data.odds_api_responses 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

ALTER TABLE raw_data.sbr_games 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

ALTER TABLE raw_data.mlb_stats_api 
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

-- ================================
-- Create Performance Indexes
-- ================================

-- Critical indexes for pipeline processing (WHERE processed_at IS NULL queries)
CREATE INDEX IF NOT EXISTS idx_action_network_odds_processed_at 
ON raw_data.action_network_odds(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_action_network_games_processed_at 
ON raw_data.action_network_games(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_action_network_history_processed_at 
ON raw_data.action_network_history(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_vsin_data_processed_at 
ON raw_data.vsin_data(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mlb_stats_api_games_processed_at 
ON raw_data.mlb_stats_api_games(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_odds_api_responses_processed_at 
ON raw_data.odds_api_responses(processed_at) WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_sbr_games_processed_at 
ON raw_data.sbr_games(processed_at) WHERE processed_at IS NULL;  

CREATE INDEX IF NOT EXISTS idx_mlb_stats_api_processed_at 
ON raw_data.mlb_stats_api(processed_at) WHERE processed_at IS NULL;

-- ================================
-- Add Column Comments
-- ================================

COMMENT ON COLUMN raw_data.action_network_odds.processed_at IS 
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.action_network_games.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.action_network_history.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.vsin_data.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.mlb_stats_api_games.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.odds_api_responses.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.sbr_games.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.mlb_stats_api.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

-- ================================
-- Summary
-- ================================

-- This migration fixes the persistent pipeline error by:
-- 1. Adding processed_at column to all raw_data tables that were missing it
-- 2. Creating partial indexes for efficient pipeline queries (WHERE processed_at IS NULL)
-- 3. Adding proper documentation for the column purpose
-- 4. Ensuring consistent schema across all raw_data tables

-- Tables updated:
-- - raw_data.action_network_odds (pipeline orchestrator queries this)
-- - raw_data.action_network_games (pipeline processing)
-- - raw_data.action_network_history (pipeline processing)
-- - raw_data.vsin_data (pipeline processing)
-- - raw_data.mlb_stats_api_games (pipeline processing)
-- - raw_data.odds_api_responses (pipeline processing)
-- - raw_data.sbr_games (pipeline processing)
-- - raw_data.mlb_stats_api (pipeline processing)

-- Note: raw_data.sbd_betting_splits already has processed_at (migration 009)
-- Note: raw_data.mlb_game_outcomes already has processed_at (existing)

COMMIT;