-- =============================================================================
-- CONSOLIDATED MIGRATION: Raw Data Tables
-- =============================================================================
-- Purpose: Consolidated creation of all raw_data schema tables
-- Replaces: 007, 009, 020_vsin, 031 (Action Network tables)
-- Date: 2025-08-12 (Consolidation)
-- =============================================================================

-- Ensure raw_data schema exists
CREATE SCHEMA IF NOT EXISTS raw_data;

-- =============================================================================
-- Action Network Raw Tables
-- =============================================================================

-- Action Network Odds Data
CREATE TABLE IF NOT EXISTS raw_data.action_network_odds (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) NOT NULL,
    sportsbook_key VARCHAR(100),
    raw_odds JSONB NOT NULL,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    CONSTRAINT unique_action_network_odds UNIQUE (external_game_id, sportsbook_key, collected_at),
    INDEX idx_action_network_odds_game_id (external_game_id),
    INDEX idx_action_network_odds_collected_at (collected_at),
    INDEX idx_action_network_odds_processed_at (processed_at),
    INDEX idx_action_network_odds_sportsbook (sportsbook_key)
);

-- Action Network Games Data  
CREATE TABLE IF NOT EXISTS raw_data.action_network_games (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE NOT NULL,
    raw_game_data JSONB NOT NULL,
    game_date DATE,
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    game_status VARCHAR(50),
    start_time TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_action_network_games_game_date (game_date),
    INDEX idx_action_network_games_teams (home_team, away_team),
    INDEX idx_action_network_games_status (game_status),
    INDEX idx_action_network_games_start_time (start_time),
    INDEX idx_action_network_games_processed_at (processed_at)
);

-- Action Network Historical Data
CREATE TABLE IF NOT EXISTS raw_data.action_network_history (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) NOT NULL,
    raw_history JSONB NOT NULL,
    endpoint_url TEXT,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_action_network_history_game_id (external_game_id),
    INDEX idx_action_network_history_collected_at (collected_at),
    INDEX idx_action_network_history_processed_at (processed_at)
);

-- =============================================================================
-- VSIN Raw Tables
-- =============================================================================

-- VSIN Sharp Action Data
CREATE TABLE IF NOT EXISTS raw_data.vsin (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255),
    sport VARCHAR(50) DEFAULT 'MLB',
    sportsbook VARCHAR(100),
    betting_data JSONB,
    sharp_action_detected BOOLEAN DEFAULT FALSE,
    data_quality_score NUMERIC(3,2) DEFAULT 1.0,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_vsin_external_game_id (external_game_id),
    INDEX idx_vsin_sport (sport),
    INDEX idx_vsin_sportsbook (sportsbook),
    INDEX idx_vsin_sharp_action (sharp_action_detected),
    INDEX idx_vsin_collected_at (collected_at),
    INDEX idx_vsin_processed_at (processed_at)
);

-- =============================================================================
-- SBD (SportsBettingDime) Raw Tables
-- =============================================================================

-- SBD Betting Splits
CREATE TABLE IF NOT EXISTS raw_data.sbd_betting_splits (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255),
    sportsbook VARCHAR(100),
    market_type VARCHAR(50), -- moneyline, spread, total
    bet_percentage NUMERIC(5,2),
    money_percentage NUMERIC(5,2),
    team_side VARCHAR(20), -- home, away, over, under
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_sbd_splits_game_id (external_game_id),
    INDEX idx_sbd_splits_market_type (market_type),
    INDEX idx_sbd_splits_team_side (team_side),
    INDEX idx_sbd_splits_collected_at (collected_at),
    INDEX idx_sbd_splits_processed_at (processed_at)
);

-- =============================================================================
-- MLB Stats API Raw Tables
-- =============================================================================

-- MLB Game Outcomes
CREATE TABLE IF NOT EXISTS raw_data.mlb_game_outcomes (
    id BIGSERIAL PRIMARY KEY,
    mlb_stats_api_game_id VARCHAR(50) UNIQUE NOT NULL,
    raw_response JSONB NOT NULL,
    game_status VARCHAR(50),
    home_team_name VARCHAR(100),
    away_team_name VARCHAR(100),
    home_score INTEGER,
    away_score INTEGER,
    game_date DATE,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance  
    INDEX idx_mlb_outcomes_game_date (game_date),
    INDEX idx_mlb_outcomes_game_status (game_status),
    INDEX idx_mlb_outcomes_teams (home_team_name, away_team_name),
    INDEX idx_mlb_outcomes_collected_at (collected_at),
    INDEX idx_mlb_outcomes_processed_at (processed_at)
);

-- MLB Stats API Games (general)
CREATE TABLE IF NOT EXISTS raw_data.mlb_stats_api_games (
    id BIGSERIAL PRIMARY KEY,
    mlb_stats_api_game_id VARCHAR(50) UNIQUE NOT NULL,
    raw_game_data JSONB NOT NULL,
    season INTEGER,
    game_type VARCHAR(10), -- R (regular), P (playoffs), etc.
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_team_name VARCHAR(100),
    away_team_name VARCHAR(100),
    game_date DATE,
    start_time TIMESTAMPTZ,
    venue_name VARCHAR(200),
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_mlb_api_games_season (season),
    INDEX idx_mlb_api_games_game_type (game_type),
    INDEX idx_mlb_api_games_date (game_date),
    INDEX idx_mlb_api_games_teams (home_team_id, away_team_id),
    INDEX idx_mlb_api_games_start_time (start_time),
    INDEX idx_mlb_api_games_processed_at (processed_at)
);

-- MLB Stats API General
CREATE TABLE IF NOT EXISTS raw_data.mlb_stats_api (
    id BIGSERIAL PRIMARY KEY,
    endpoint VARCHAR(200) NOT NULL,
    request_params JSONB,
    raw_response JSONB NOT NULL,
    response_status INTEGER DEFAULT 200,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    
    -- Indexes for performance
    INDEX idx_mlb_stats_api_endpoint (endpoint),
    INDEX idx_mlb_stats_api_collected_at (collected_at),
    INDEX idx_mlb_stats_api_status (response_status),
    INDEX idx_mlb_stats_api_processed_at (processed_at)
);

-- =============================================================================
-- Comments and Documentation
-- =============================================================================

COMMENT ON SCHEMA raw_data IS 'Raw data storage for all external data sources (Action Network, VSIN, SBD, MLB Stats API)';

COMMENT ON TABLE raw_data.action_network_odds IS 'Raw betting odds data from Action Network API';
COMMENT ON TABLE raw_data.action_network_games IS 'Raw game information from Action Network API';
COMMENT ON TABLE raw_data.action_network_history IS 'Raw historical line movement data from Action Network API';

COMMENT ON TABLE raw_data.vsin IS 'Raw sharp action analysis data from VSIN';

COMMENT ON TABLE raw_data.sbd_betting_splits IS 'Raw betting split percentages from SportsBettingDime';

COMMENT ON TABLE raw_data.mlb_game_outcomes IS 'Raw game outcome data from MLB Stats API';
COMMENT ON TABLE raw_data.mlb_stats_api_games IS 'Raw game information from MLB Stats API';
COMMENT ON TABLE raw_data.mlb_stats_api IS 'General raw responses from MLB Stats API';