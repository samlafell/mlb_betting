-- Migration: Add raw_data.mlb_game_outcomes table for MLB Stats API responses
-- Purpose: Store raw MLB Stats API responses for game outcomes before processing
-- Created: 2025-01-27
-- Part of: Enhanced Game Outcome Integration Plan

-- ================================
-- Phase 1: Raw Data Storage
-- ================================

-- Raw MLB Stats API game outcome responses
CREATE TABLE IF NOT EXISTS raw_data.mlb_game_outcomes (
    id BIGSERIAL PRIMARY KEY,
    
    -- MLB Stats API identification
    mlb_game_pk VARCHAR(20) NOT NULL, -- Primary key from MLB Stats API
    mlb_stats_api_game_id VARCHAR(20) NOT NULL, -- Same as game_pk, for consistency
    
    -- Request metadata
    api_endpoint VARCHAR(200) NOT NULL, -- e.g., '/api/v1/game/{game_pk}/feed/live'
    request_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Raw API response
    raw_response JSONB NOT NULL, -- Complete JSON response from MLB Stats API
    response_status INTEGER NOT NULL DEFAULT 200, -- HTTP status code
    response_headers JSONB, -- HTTP response headers
    
    -- Game identification from response
    game_date DATE, -- Extracted from response
    home_team VARCHAR(100), -- Extracted from response
    away_team VARCHAR(100), -- Extracted from response
    game_status VARCHAR(50), -- e.g., 'Final', 'Official', 'In Progress'
    
    -- Processing status
    processing_status VARCHAR(20) DEFAULT 'pending' CHECK (processing_status IN ('pending', 'processed', 'failed', 'skipped')),
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_error TEXT,
    
    -- Data quality
    data_quality VARCHAR(20) DEFAULT 'HIGH',
    validation_errors JSONB DEFAULT '[]',
    
    -- Outcome extraction (for quick access without JSON parsing)
    home_score INTEGER, -- Extracted from response
    away_score INTEGER, -- Extracted from response
    is_final_game BOOLEAN DEFAULT FALSE, -- TRUE if game is completed
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ================================
-- Indexes for Performance
-- ================================

-- Primary lookup by MLB game PK
CREATE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_game_pk 
ON raw_data.mlb_game_outcomes(mlb_game_pk);

-- Lookup by game date for date-range queries
CREATE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_game_date 
ON raw_data.mlb_game_outcomes(game_date);

-- Processing status for workflow management
CREATE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_processing_status 
ON raw_data.mlb_game_outcomes(processing_status);

-- Final games for outcome processing
CREATE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_final_games 
ON raw_data.mlb_game_outcomes(is_final_game, processing_status);

-- Request timestamp for audit trail
CREATE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_request_timestamp 
ON raw_data.mlb_game_outcomes(request_timestamp);

-- ================================
-- Comments for Documentation
-- ================================

COMMENT ON TABLE raw_data.mlb_game_outcomes IS 'Raw MLB Stats API responses for game outcomes - stores complete JSON responses before processing';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.mlb_game_pk IS 'Primary key from MLB Stats API (game PK)';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.raw_response IS 'Complete JSON response from MLB Stats API /game/{pk}/feed/live endpoint';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.processing_status IS 'Status of outcome processing: pending, processed, failed, skipped';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.is_final_game IS 'TRUE if game status indicates completion (Final, Official)';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.home_score IS 'Home team final score (extracted for quick access)';
COMMENT ON COLUMN raw_data.mlb_game_outcomes.away_score IS 'Away team final score (extracted for quick access)';

-- ================================
-- Data Quality Constraints
-- ================================

-- Unique constraint to prevent duplicate raw responses for same game/timestamp
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_mlb_outcomes_unique_game_timestamp 
ON raw_data.mlb_game_outcomes(mlb_game_pk, request_timestamp);

-- Check constraint for valid game status values
ALTER TABLE raw_data.mlb_game_outcomes 
ADD CONSTRAINT chk_game_status 
CHECK (game_status IN ('Scheduled', 'Live', 'Final', 'Official', 'Postponed', 'Cancelled', 'Suspended', 'In Progress', 'Delayed Start', 'Warmup', 'Preview') OR game_status IS NULL);

-- Check constraint for valid HTTP status codes
ALTER TABLE raw_data.mlb_game_outcomes 
ADD CONSTRAINT chk_response_status 
CHECK (response_status BETWEEN 200 AND 599);

-- ================================
-- Trigger for Updated Timestamp
-- ================================

-- Create trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add trigger to update updated_at on changes
DROP TRIGGER IF EXISTS trigger_update_mlb_outcomes_updated_at ON raw_data.mlb_game_outcomes;
CREATE TRIGGER trigger_update_mlb_outcomes_updated_at
    BEFORE UPDATE ON raw_data.mlb_game_outcomes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ================================
-- Grant Permissions
-- ================================

-- Grant permissions to application role (if exists)
-- Note: Adjust role name based on your database setup
-- GRANT SELECT, INSERT, UPDATE ON raw_data.mlb_game_outcomes TO app_role;
-- GRANT USAGE ON SEQUENCE raw_data.mlb_game_outcomes_id_seq TO app_role;

-- ================================
-- Migration Verification
-- ================================

-- Verify table was created successfully
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'raw_data' 
               AND table_name = 'mlb_game_outcomes') THEN
        RAISE NOTICE 'SUCCESS: raw_data.mlb_game_outcomes table created successfully';
    ELSE
        RAISE EXCEPTION 'FAILED: raw_data.mlb_game_outcomes table was not created';
    END IF;
END $$;