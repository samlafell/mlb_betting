-- Migration: Create Missing Staging Tables
-- Purpose: Create missing staging zone tables required by pipeline
-- Addresses: staging.betting_lines and public.pipeline_execution_log tables
-- Date: 2025-08-11

-- ================================
-- Create staging.betting_lines table
-- ================================

CREATE TABLE IF NOT EXISTS staging.betting_lines (
    id BIGSERIAL PRIMARY KEY,
    raw_betting_lines_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    bet_type VARCHAR(20) CHECK (bet_type IN ('moneyline', 'spread', 'total')),
    line_value DECIMAL(5,2),
    odds_american INTEGER,
    team_type VARCHAR(10) CHECK (team_type IN ('home', 'away', 'over', 'under')),
    team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'valid', 'invalid', 'warning')),
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================
-- Create indexes for performance
-- ================================

CREATE INDEX IF NOT EXISTS idx_staging_betting_lines_game_id ON staging.betting_lines(game_id);
CREATE INDEX IF NOT EXISTS idx_staging_betting_lines_sportsbook ON staging.betting_lines(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_staging_betting_lines_bet_type ON staging.betting_lines(bet_type);
CREATE INDEX IF NOT EXISTS idx_staging_betting_lines_processed_at ON staging.betting_lines(processed_at);
CREATE INDEX IF NOT EXISTS idx_staging_betting_lines_validation_status ON staging.betting_lines(validation_status);

-- ================================
-- Create pipeline execution log table
-- ================================

CREATE TABLE IF NOT EXISTS public.pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    execution_id UUID DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL,
    zone VARCHAR(20) NOT NULL CHECK (zone IN ('raw', 'staging', 'curated')),
    status VARCHAR(20) NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'skipped')),
    records_processed INTEGER DEFAULT 0,
    records_successful INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    error_message TEXT,
    metadata JSONB DEFAULT '{}',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================
-- Create indexes for pipeline log
-- ================================

CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_id ON public.pipeline_execution_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_pipeline_name ON public.pipeline_execution_log(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_zone ON public.pipeline_execution_log(zone);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_status ON public.pipeline_execution_log(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_started_at ON public.pipeline_execution_log(started_at);

-- ================================
-- Add processed_at column to action_network_history if missing
-- ================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'raw_data' 
        AND table_name = 'action_network_history' 
        AND column_name = 'processed_at'
    ) THEN
        ALTER TABLE raw_data.action_network_history 
        ADD COLUMN processed_at TIMESTAMPTZ;
    END IF;
END $$;

-- ================================
-- Create missing raw data tables referenced by collectors
-- ================================

-- Create sbd_betting_splits table for SBD collector
CREATE TABLE IF NOT EXISTS raw_data.sbd_betting_splits (
    id BIGSERIAL PRIMARY KEY,
    external_matchup_id VARCHAR(255) UNIQUE NOT NULL,
    raw_response JSONB NOT NULL,
    api_endpoint VARCHAR(100),
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_team_abbr VARCHAR(10),
    away_team_abbr VARCHAR(10),
    home_team_id VARCHAR(50),
    away_team_id VARCHAR(50),
    game_name VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create mlb_stats_api_games table
CREATE TABLE IF NOT EXISTS raw_data.mlb_stats_api_games (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE NOT NULL,
    raw_response JSONB NOT NULL,
    api_endpoint VARCHAR(100),
    sport VARCHAR(50) DEFAULT 'mlb',
    collection_timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create mlb_stats_api table (general)
CREATE TABLE IF NOT EXISTS raw_data.mlb_stats_api (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255) NOT NULL,
    endpoint VARCHAR(100) NOT NULL,
    raw_response JSONB NOT NULL,
    collection_timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(external_id, endpoint, collection_timestamp)
);

-- ================================
-- Create indexes for raw data tables
-- ================================

CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_external_id ON raw_data.sbd_betting_splits(external_matchup_id);
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_teams ON raw_data.sbd_betting_splits(home_team_abbr, away_team_abbr);
CREATE INDEX IF NOT EXISTS idx_mlb_stats_api_games_external_id ON raw_data.mlb_stats_api_games(external_game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_stats_api_external_id ON raw_data.mlb_stats_api(external_id);
CREATE INDEX IF NOT EXISTS idx_mlb_stats_api_endpoint ON raw_data.mlb_stats_api(endpoint);

-- ================================
-- Comments and documentation
-- ================================

COMMENT ON TABLE staging.betting_lines IS 'Staging zone table for normalized betting lines from all sources';
COMMENT ON TABLE public.pipeline_execution_log IS 'Pipeline execution tracking and monitoring';
COMMENT ON TABLE raw_data.sbd_betting_splits IS 'Raw data from SBD API with betting splits and sharp action';
COMMENT ON TABLE raw_data.mlb_stats_api_games IS 'Raw game data from MLB Stats API';
COMMENT ON TABLE raw_data.mlb_stats_api IS 'General raw data from MLB Stats API endpoints';

COMMENT ON COLUMN staging.betting_lines.raw_betting_lines_id IS 'Reference to source raw table record';
COMMENT ON COLUMN staging.betting_lines.validation_status IS 'Data validation status: pending, valid, invalid, warning';
COMMENT ON COLUMN public.pipeline_execution_log.execution_id IS 'Unique identifier for pipeline execution batch';
COMMENT ON COLUMN public.pipeline_execution_log.metadata IS 'Additional execution metadata and context';