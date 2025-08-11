-- Migration 031: Create Missing Action Network Raw Data Tables
-- Purpose: Fix PostgreSQL Docker migration by creating missing raw_data tables
-- Issue: Collectors expect raw_data.action_network_odds and raw_data.action_network_games but they don't exist
-- Date: 2025-08-11
-- Related Error: UndefinedTableError: relation "raw_data.action_network_odds" does not exist

-- ================================
-- Ensure raw_data schema exists
-- ================================
CREATE SCHEMA IF NOT EXISTS raw_data;

-- ================================
-- Create missing raw_data.action_network_odds table
-- ================================
CREATE TABLE IF NOT EXISTS raw_data.action_network_odds (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game identification
    external_game_id VARCHAR(255) NOT NULL,
    
    -- Sportsbook identification
    sportsbook_key VARCHAR(50),
    
    -- Raw odds data (JSONB for efficient queries)
    raw_odds JSONB NOT NULL,
    
    -- Timestamps
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    
    -- Index for efficient lookups
    CONSTRAINT valid_external_game_id CHECK (external_game_id != ''),
    CONSTRAINT valid_raw_odds CHECK (raw_odds IS NOT NULL)
);

-- ================================
-- Create missing raw_data.action_network_games table
-- ================================
CREATE TABLE IF NOT EXISTS raw_data.action_network_games (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game identification (must be unique)
    external_game_id VARCHAR(255) UNIQUE NOT NULL,
    
    -- Raw game data from Action Network API
    raw_game_data JSONB NOT NULL,
    
    -- Timestamps
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    
    -- Constraints
    CONSTRAINT valid_external_game_id_games CHECK (external_game_id != ''),
    CONSTRAINT valid_raw_game_data CHECK (raw_game_data IS NOT NULL)
);

-- ================================
-- Create Performance Indexes
-- ================================

-- Indexes for raw_data.action_network_odds
CREATE INDEX IF NOT EXISTS idx_action_network_odds_external_game_id 
    ON raw_data.action_network_odds(external_game_id);

CREATE INDEX IF NOT EXISTS idx_action_network_odds_sportsbook_key 
    ON raw_data.action_network_odds(sportsbook_key);

CREATE INDEX IF NOT EXISTS idx_action_network_odds_collected_at 
    ON raw_data.action_network_odds(collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_action_network_odds_processed_at 
    ON raw_data.action_network_odds(processed_at) WHERE processed_at IS NULL;

-- Combined index for common query patterns
CREATE INDEX IF NOT EXISTS idx_action_network_odds_game_sportsbook 
    ON raw_data.action_network_odds(external_game_id, sportsbook_key);

-- Indexes for raw_data.action_network_games
CREATE INDEX IF NOT EXISTS idx_action_network_games_external_game_id 
    ON raw_data.action_network_games(external_game_id);

CREATE INDEX IF NOT EXISTS idx_action_network_games_collected_at 
    ON raw_data.action_network_games(collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_action_network_games_processed_at 
    ON raw_data.action_network_games(processed_at) WHERE processed_at IS NULL;

-- ================================
-- Run migration 004 staging tables (if not exist)
-- ================================

-- Ensure staging schema exists
CREATE SCHEMA IF NOT EXISTS staging;

-- Create staging.action_network_games (from migration 004)
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

-- Create staging.action_network_odds_historical (from migration 004)
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
-- Staging Table Indexes (from migration 004)
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
-- Add Table Comments
-- ================================

COMMENT ON TABLE raw_data.action_network_odds IS 
'Raw odds data from Action Network API. Contains unprocessed JSONB odds data for each game and sportsbook.';

COMMENT ON TABLE raw_data.action_network_games IS
'Raw game data from Action Network API. Contains unprocessed JSONB game information.';

COMMENT ON COLUMN raw_data.action_network_odds.processed_at IS 
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON COLUMN raw_data.action_network_games.processed_at IS
'Timestamp when record was processed by pipeline. NULL indicates unprocessed record.';

COMMENT ON TABLE staging.action_network_games IS 'Unified games table with MLB Stats API integration for cross-system compatibility';
COMMENT ON TABLE staging.action_network_odds_historical IS 'Complete temporal odds data combining LONG structure with historical timestamps for sophisticated betting analysis';

COMMENT ON COLUMN staging.action_network_odds_historical.updated_at IS 'Exact timestamp when odds changed, extracted from JSON history.updated_at';
COMMENT ON COLUMN staging.action_network_odds_historical.is_current_odds IS 'TRUE if this represents the most recent odds for this market/side/sportsbook';
COMMENT ON COLUMN staging.action_network_odds_historical.line_status IS 'Line status from JSON: opener, normal, suspended, etc.';

-- ================================
-- Summary
-- ================================

-- This migration fixes the Docker PostgreSQL migration by:
-- 1. Creating missing raw_data.action_network_odds table with proper schema
-- 2. Creating missing raw_data.action_network_games table with proper schema  
-- 3. Ensuring staging tables from migration 004 exist
-- 4. Adding proper indexes for performance
-- 5. Adding documentation and constraints

-- Tables created:
-- - raw_data.action_network_odds (for _store_raw_odds_data method)
-- - raw_data.action_network_games (for game mapping queries)
-- - staging.action_network_games (unified game data)
-- - staging.action_network_odds_historical (temporal odds analysis)

-- This should resolve the UndefinedTableError: relation "raw_data.action_network_odds" does not exist