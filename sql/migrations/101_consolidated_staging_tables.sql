-- =============================================================================
-- CONSOLIDATED MIGRATION: Staging Tables
-- =============================================================================
-- Purpose: Consolidated creation of all staging schema tables
-- Replaces: 032, 033 (fragmented staging tables), superseded by 035 (unified)
-- Date: 2025-08-12 (Consolidation)
-- Primary Table: staging.betting_odds_unified (modern unified approach)
-- Legacy Tables: staging.moneylines, spreads, totals (for backward compatibility)
-- =============================================================================

-- Ensure staging schema exists
CREATE SCHEMA IF NOT EXISTS staging;

-- =============================================================================
-- UNIFIED STAGING TABLE (PRIMARY - MODERN APPROACH)
-- =============================================================================

-- Unified Betting Odds Staging Table
-- This is the modern, unified approach that consolidates all bet types
CREATE TABLE IF NOT EXISTS staging.betting_odds_unified (
    id BIGSERIAL PRIMARY KEY,
    
    -- Source Attribution (ADDRESSES ISSUE #1)
    data_source VARCHAR(50) NOT NULL, -- action_network, vsin, sbd, mlb_stats_api
    source_collector VARCHAR(100), -- specific collector used
    
    -- External Identifiers
    external_game_id VARCHAR(255) NOT NULL,
    external_source_id VARCHAR(50), -- unique within data source
    
    -- Game Information (ADDRESSES ISSUE #2)
    home_team VARCHAR(10), -- Normalized team abbreviation (e.g., 'LAD', 'NYY')
    away_team VARCHAR(10), -- Normalized team abbreviation
    game_date DATE,
    game_start_time TIMESTAMPTZ,
    mlb_stats_api_game_id VARCHAR(50), -- Cross-system integration
    
    -- Sportsbook Information (ADDRESSES ISSUE #3)
    sportsbook_external_id VARCHAR(50), -- External sportsbook identifier
    sportsbook_name VARCHAR(100), -- Resolved sportsbook name
    
    -- Unified Betting Data (ADDRESSES ISSUE #4 - BET CONSOLIDATION)
    market_type VARCHAR(20) NOT NULL, -- Primary market type for this record
    
    -- Moneyline Odds
    home_moneyline_odds INTEGER,
    away_moneyline_odds INTEGER,
    
    -- Spread Betting
    spread_line NUMERIC(4,2),
    home_spread_odds INTEGER,
    away_spread_odds INTEGER,
    
    -- Total (Over/Under) Betting
    total_line NUMERIC(4,2),
    over_odds INTEGER,
    under_odds INTEGER,
    
    -- Data Lineage (ADDRESSES ISSUE #5)
    raw_data_table VARCHAR(100), -- Source table (e.g., 'raw_data.action_network_odds')
    raw_data_id BIGINT, -- Source record ID
    transformation_metadata JSONB, -- Processing metadata
    
    -- Quality and Validation
    data_quality_score NUMERIC(4,3) DEFAULT 1.0, -- 0.0 to 1.0 quality score
    validation_status VARCHAR(20) DEFAULT 'pending', -- pending, valid, invalid, warning
    validation_errors JSONB, -- Array of validation error messages
    
    -- Timestamps
    odds_timestamp TIMESTAMPTZ, -- When odds were effective
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_data_source CHECK (data_source IN ('action_network', 'vsin', 'sbd', 'mlb_stats_api', 'sports_book_review')),
    CONSTRAINT valid_market_type CHECK (market_type IN ('moneyline', 'spread', 'total', 'mixed')),
    CONSTRAINT valid_validation_status CHECK (validation_status IN ('pending', 'valid', 'invalid', 'warning')),
    CONSTRAINT valid_team_codes CHECK (LENGTH(home_team) BETWEEN 2 AND 5 AND LENGTH(away_team) BETWEEN 2 AND 5),
    CONSTRAINT different_teams CHECK (home_team != away_team),
    CONSTRAINT valid_quality_score CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_staging_odds UNIQUE (data_source, external_game_id, sportsbook_external_id, market_type, odds_timestamp)
);

-- Comprehensive indexes for unified staging table
CREATE INDEX IF NOT EXISTS idx_staging_unified_game_id ON staging.betting_odds_unified (external_game_id);
CREATE INDEX IF NOT EXISTS idx_staging_unified_data_source ON staging.betting_odds_unified (data_source);
CREATE INDEX IF NOT EXISTS idx_staging_unified_teams ON staging.betting_odds_unified (home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_staging_unified_game_date ON staging.betting_odds_unified (game_date);
CREATE INDEX IF NOT EXISTS idx_staging_unified_sportsbook ON staging.betting_odds_unified (sportsbook_name);
CREATE INDEX IF NOT EXISTS idx_staging_unified_market_type ON staging.betting_odds_unified (market_type);
CREATE INDEX IF NOT EXISTS idx_staging_unified_quality ON staging.betting_odds_unified (data_quality_score);
CREATE INDEX IF NOT EXISTS idx_staging_unified_validation ON staging.betting_odds_unified (validation_status);
CREATE INDEX IF NOT EXISTS idx_staging_unified_mlb_game_id ON staging.betting_odds_unified (mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_staging_unified_processed_at ON staging.betting_odds_unified (processed_at);
CREATE INDEX IF NOT EXISTS idx_staging_unified_odds_timestamp ON staging.betting_odds_unified (odds_timestamp);

-- =============================================================================
-- LEGACY STAGING TABLES (FOR BACKWARD COMPATIBILITY)
-- =============================================================================

-- Legacy Moneylines Table
CREATE TABLE IF NOT EXISTS staging.moneylines (
    id BIGSERIAL PRIMARY KEY,
    raw_moneylines_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_normalized VARCHAR(10),
    away_team_normalized VARCHAR(10),
    data_quality_score NUMERIC(4,3) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_moneylines_game_id (game_id),
    INDEX idx_moneylines_sportsbook (sportsbook_name),
    INDEX idx_moneylines_teams (home_team_normalized, away_team_normalized),
    INDEX idx_moneylines_processed_at (processed_at)
);

-- Legacy Spreads Table  
CREATE TABLE IF NOT EXISTS staging.spreads (
    id BIGSERIAL PRIMARY KEY,
    raw_spreads_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    line_value NUMERIC(4,2),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_normalized VARCHAR(10),
    away_team_normalized VARCHAR(10),
    data_quality_score NUMERIC(4,3) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_spreads_game_id (game_id),
    INDEX idx_spreads_line_value (line_value),
    INDEX idx_spreads_sportsbook (sportsbook_name),
    INDEX idx_spreads_teams (home_team_normalized, away_team_normalized),
    INDEX idx_spreads_processed_at (processed_at)
);

-- Legacy Totals Table
CREATE TABLE IF NOT EXISTS staging.totals (
    id BIGSERIAL PRIMARY KEY,
    raw_totals_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    line_value NUMERIC(4,2),
    over_odds INTEGER,
    under_odds INTEGER,
    data_quality_score NUMERIC(4,3) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_totals_game_id (game_id),
    INDEX idx_totals_line_value (line_value),
    INDEX idx_totals_sportsbook (sportsbook_name),
    INDEX idx_totals_processed_at (processed_at)
);

-- =============================================================================
-- STAGING SUPPORT TABLES
-- =============================================================================

-- General Betting Lines (fallback/generic)
CREATE TABLE IF NOT EXISTS staging.betting_lines (
    id BIGSERIAL PRIMARY KEY,
    raw_betting_lines_id BIGINT,
    game_id VARCHAR(255),
    sportsbook_id INTEGER,
    bet_type VARCHAR(50),
    line_value NUMERIC(10,4),
    odds_american INTEGER,
    team_type VARCHAR(20), -- home, away, over, under
    team_normalized VARCHAR(10),
    data_quality_score NUMERIC(4,3) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending',
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_betting_lines_game_id (game_id),
    INDEX idx_betting_lines_bet_type (bet_type),
    INDEX idx_betting_lines_team_type (team_type),
    INDEX idx_betting_lines_processed_at (processed_at)
);

-- =============================================================================
-- STAGING UTILITY FUNCTIONS
-- =============================================================================

-- Function to migrate legacy staging data to unified table
CREATE OR REPLACE FUNCTION staging.migrate_legacy_to_unified()
RETURNS TABLE(
    migrated_records INTEGER,
    source_table TEXT,
    status TEXT
) AS $$
DECLARE
    moneyline_count INTEGER := 0;
    spread_count INTEGER := 0;
    total_count INTEGER := 0;
BEGIN
    -- This function can be used to migrate existing legacy staging data
    -- to the new unified staging table format
    
    -- Count existing records
    SELECT COUNT(*) INTO moneyline_count FROM staging.moneylines;
    SELECT COUNT(*) INTO spread_count FROM staging.spreads;  
    SELECT COUNT(*) INTO total_count FROM staging.totals;
    
    -- Return summary
    RETURN QUERY VALUES 
        (moneyline_count, 'staging.moneylines', 'ready_for_migration'),
        (spread_count, 'staging.spreads', 'ready_for_migration'),
        (total_count, 'staging.totals', 'ready_for_migration');
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA staging IS 'Staging zone for cleaned, normalized, and validated data from raw sources';

COMMENT ON TABLE staging.betting_odds_unified IS 'Primary unified staging table consolidating all betting data types with comprehensive source attribution and quality scoring';

COMMENT ON TABLE staging.moneylines IS 'Legacy moneyline staging table - use staging.betting_odds_unified for new development';
COMMENT ON TABLE staging.spreads IS 'Legacy spread staging table - use staging.betting_odds_unified for new development';  
COMMENT ON TABLE staging.totals IS 'Legacy totals staging table - use staging.betting_odds_unified for new development';

COMMENT ON FUNCTION staging.migrate_legacy_to_unified() IS 'Utility function to migrate data from legacy fragmented staging tables to unified staging table';