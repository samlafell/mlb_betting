-- Migration: Create raw_data.vsin table for VSIN data collection
-- Date: 2025-08-11
-- Purpose: Add table to store raw VSIN betting data

CREATE TABLE IF NOT EXISTS raw_data.vsin (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255),
    sport VARCHAR(50) DEFAULT 'mlb',
    sportsbook VARCHAR(100),
    betting_data JSONB,
    sharp_action_detected BOOLEAN DEFAULT FALSE,
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    batch_id UUID,
    data_quality_score DECIMAL(5,2) DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Add unique constraint to prevent duplicate records
    UNIQUE(external_game_id, sportsbook, collection_timestamp)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_vsin_external_game_id ON raw_data.vsin(external_game_id);
CREATE INDEX IF NOT EXISTS idx_vsin_sportsbook ON raw_data.vsin(sportsbook);
CREATE INDEX IF NOT EXISTS idx_vsin_collection_timestamp ON raw_data.vsin(collection_timestamp);
CREATE INDEX IF NOT EXISTS idx_vsin_batch_id ON raw_data.vsin(batch_id);
CREATE INDEX IF NOT EXISTS idx_vsin_sharp_action ON raw_data.vsin(sharp_action_detected);

-- Add JSONB indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_vsin_betting_data_gin ON raw_data.vsin USING GIN (betting_data);

-- Add comment
COMMENT ON TABLE raw_data.vsin IS 'Raw VSIN betting data collection table';
COMMENT ON COLUMN raw_data.vsin.external_game_id IS 'External game identifier from VSIN';
COMMENT ON COLUMN raw_data.vsin.betting_data IS 'Full VSIN betting record as JSON';
COMMENT ON COLUMN raw_data.vsin.sharp_action_detected IS 'Whether sharp action was detected in this record';
COMMENT ON COLUMN raw_data.vsin.data_quality_score IS 'Data completeness and quality score (0-100)';