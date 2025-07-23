-- Migration: Create Source-Specific RAW/STAGING/CURATED Pipeline Zones
-- Purpose: Implement three-tier data pipeline with source-specific raw tables
-- Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
-- Created: 2025-07-22 (Replaces generic table approach)

-- ================================
-- Phase 1: Create Pipeline Schemas
-- ================================

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

-- Add comments to schemas
COMMENT ON SCHEMA raw_data IS 'RAW Zone - Source-specific tables storing unprocessed data exactly as received';
COMMENT ON SCHEMA staging IS 'STAGING Zone - Unified, normalized data ready for analysis (LONG + Historical structure)';
COMMENT ON SCHEMA curated IS 'CURATED Zone - Feature-enriched, analysis-ready datasets with ML features';

-- ================================
-- RAW Zone: Source-Specific Tables
-- ================================

-- Action Network Raw Data (Games + Odds + History)
-- These are the current source-specific tables that already exist
-- No need to recreate - they are managed by collectors

-- SBD Raw Data 
-- Managed by SBD collector - source-specific structure

-- VSIN Raw Data
-- Managed by VSIN collector - source-specific structure

-- ================================
-- STAGING Zone: Unified Structure
-- ================================

-- Games Table (Unified across all sources)
CREATE TABLE IF NOT EXISTS staging.action_network_games (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE NOT NULL,
    mlb_stats_api_game_id VARCHAR(50), -- Cross-system integration
    
    -- Game Details
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    
    -- Game Timing
    game_date DATE NOT NULL,
    game_time TIME,
    game_status VARCHAR(50),
    
    -- Venue Information
    venue_name VARCHAR(200),
    venue_location VARCHAR(200),
    
    -- Data Quality and Processing
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    data_source VARCHAR(50) DEFAULT 'action_network',
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- STAGING Zone: Historical Odds (Temporal)
-- ================================
-- This is the unified approach: Historical odds with exact timestamps
-- Combines both LONG structure and temporal data in one table

CREATE TABLE IF NOT EXISTS staging.action_network_odds_historical (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game and sportsbook identification
    external_game_id VARCHAR(255) NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    sportsbook_external_id VARCHAR(50) NOT NULL,
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(255),
    
    -- Market and side identification  
    market_type VARCHAR(20) NOT NULL,   -- moneyline, spread, total
    side VARCHAR(10) NOT NULL,          -- home, away, over, under
    
    -- Odds data
    odds INTEGER NOT NULL,
    line_value DECIMAL(4,1),            -- spread/total value, NULL for moneyline
    
    -- CRITICAL: Exact timing from JSON
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,  -- From JSON history.updated_at
    data_collection_time TIMESTAMP WITH TIME ZONE, -- When we pulled from API  
    data_processing_time TIMESTAMP WITH TIME ZONE, -- When we processed this record
    
    -- Line status and metadata
    line_status VARCHAR(50),            -- opener, normal, suspended
    is_current_odds BOOLEAN DEFAULT FALSE,  -- TRUE if this is the latest odds
    
    -- Action Network metadata
    market_id BIGINT,
    outcome_id BIGINT,
    period VARCHAR(50) DEFAULT 'event',
    
    -- Data quality and lineage
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(50) DEFAULT 'valid',
    raw_data_id BIGINT, -- References raw_data.action_network_odds(id) or raw_data.action_network_history(id)
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at_record TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(external_game_id, sportsbook_external_id, market_type, side, updated_at),
    
    -- Validate market types and sides
    CONSTRAINT valid_market_types CHECK (market_type IN ('moneyline', 'spread', 'total')),
    CONSTRAINT valid_sides CHECK (side IN ('home', 'away', 'over', 'under')),
    
    -- Ensure side/market type compatibility
    CONSTRAINT side_market_compatibility CHECK (
        (market_type = 'moneyline' AND side IN ('home', 'away')) OR
        (market_type = 'spread' AND side IN ('home', 'away')) OR  
        (market_type = 'total' AND side IN ('over', 'under'))
    ),
    
    -- Line value logic
    CONSTRAINT line_value_logic CHECK (
        (market_type = 'moneyline' AND line_value IS NULL) OR
        (market_type IN ('spread', 'total') AND line_value IS NOT NULL)
    )
);

-- ================================
-- Indexes for Performance
-- ================================

-- Games table indexes
CREATE INDEX IF NOT EXISTS idx_an_games_external_id ON staging.action_network_games(external_game_id);
CREATE INDEX IF NOT EXISTS idx_an_games_mlb_id ON staging.action_network_games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_an_games_date ON staging.action_network_games(game_date);
CREATE INDEX IF NOT EXISTS idx_an_games_teams ON staging.action_network_games(home_team_normalized, away_team_normalized);

-- Historical odds indexes
CREATE INDEX IF NOT EXISTS idx_historical_odds_game_id ON staging.action_network_odds_historical(external_game_id);
CREATE INDEX IF NOT EXISTS idx_historical_odds_mlb_id ON staging.action_network_odds_historical(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_historical_odds_sportsbook ON staging.action_network_odds_historical(sportsbook_external_id, sportsbook_name);
CREATE INDEX IF NOT EXISTS idx_historical_odds_market_side ON staging.action_network_odds_historical(market_type, side);
CREATE INDEX IF NOT EXISTS idx_historical_odds_updated_at ON staging.action_network_odds_historical(updated_at);
CREATE INDEX IF NOT EXISTS idx_historical_odds_current ON staging.action_network_odds_historical(external_game_id, market_type, is_current_odds);
CREATE INDEX IF NOT EXISTS idx_historical_odds_game_market_time ON staging.action_network_odds_historical(external_game_id, market_type, side, updated_at);

-- ================================
-- CURATED Zone: Analysis Tables
-- ================================

-- Placeholder for future analysis tables
-- Will be populated based on specific analytics needs

-- ================================
-- Pipeline Execution Tracking
-- ================================

CREATE TABLE IF NOT EXISTS public.pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL,
    pipeline_stage VARCHAR(50) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed'
    records_processed INTEGER DEFAULT 0,
    records_successful INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    metadata TEXT, -- JSON string for additional metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_id ON public.pipeline_execution_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_stage ON public.pipeline_execution_log(pipeline_stage);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_status ON public.pipeline_execution_log(status);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE staging.action_network_games IS 'Unified games table with MLB Stats API integration for cross-system compatibility';
COMMENT ON TABLE staging.action_network_odds_historical IS 'Complete temporal odds data combining LONG structure with historical timestamps for sophisticated betting analysis';
COMMENT ON TABLE public.pipeline_execution_log IS 'Pipeline execution tracking for monitoring and debugging';

COMMENT ON COLUMN staging.action_network_odds_historical.updated_at IS 'Exact timestamp when odds changed, extracted from JSON history.updated_at';
COMMENT ON COLUMN staging.action_network_odds_historical.is_current_odds IS 'TRUE if this represents the most recent odds for this market/side/sportsbook';
COMMENT ON COLUMN staging.action_network_odds_historical.line_status IS 'Line status from JSON: opener, normal, suspended, etc.';