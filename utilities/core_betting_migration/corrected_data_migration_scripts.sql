-- =========================================================================
-- Core Betting Schema Decommission - CORRECTED Data Migration Scripts
-- =========================================================================
-- Generated: 2025-01-25 (Corrected based on actual schema analysis)
-- Purpose: Migrate all data from core_betting schema to curated schema
-- Requirements: PostgreSQL 12+, operational schema for tracking
-- 
-- IMPORTANT: This is the corrected version based on actual schema analysis
-- =========================================================================

-- First, rollback any partial migration
DELETE FROM operational.core_betting_migration_log WHERE phase != 'MIGRATION_START';
DELETE FROM operational.pre_migration_counts;
DELETE FROM operational.post_migration_counts;

-- Drop any partially created curated tables
DROP TABLE IF EXISTS curated.betting_lines_unified CASCADE;
DROP TABLE IF EXISTS curated.game_outcomes CASCADE;
DROP TABLE IF EXISTS curated.games_complete CASCADE;
DROP TABLE IF EXISTS curated.sportsbook_mappings CASCADE;
DROP TABLE IF EXISTS curated.data_sources CASCADE;
DROP TABLE IF EXISTS curated.teams_master CASCADE;
DROP TABLE IF EXISTS curated.sportsbooks CASCADE;

-- -------------------------------------------------------------------------
-- PHASE 1: PRE-MIGRATION SETUP AND VALIDATION (CORRECTED)
-- -------------------------------------------------------------------------

-- Log migration start
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details)
VALUES ('PHASE_1', 'CORRECTED_MIGRATION_START', 'started', '{"description": "Corrected core betting schema decommission migration"}');

-- Record current row counts for validation (CORRECTED TABLES)
INSERT INTO operational.pre_migration_counts (table_name, record_count)
SELECT 'curated.games_complete', COUNT(*) FROM curated.games_complete
UNION ALL
SELECT 'curated.game_outcomes', COUNT(*) FROM curated.game_outcomes
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
UNION ALL
SELECT 'curated.sportsbooks', COUNT(*) FROM curated.sportsbooks
UNION ALL
SELECT 'curated.teams_master', COUNT(*) FROM curated.teams_master
UNION ALL
SELECT 'curated.sportsbook_mappings', COUNT(*) FROM curated.sportsbook_mappings;

-- -------------------------------------------------------------------------
-- PHASE 2: CREATE ENHANCED CURATED SCHEMA TABLES (CORRECTED)
-- -------------------------------------------------------------------------

-- Enhanced sportsbooks table (migrate first - referenced by others)
CREATE TABLE curated.sportsbooks (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100),
    abbreviation VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    supports_live_betting BOOLEAN DEFAULT FALSE,
    -- Enhanced metadata
    reliability_score DECIMAL(3,2) DEFAULT 1.0,
    website_url VARCHAR(200),
    api_endpoint VARCHAR(200),
    rate_limit_per_minute INTEGER DEFAULT 60,
    timezone VARCHAR(50) DEFAULT 'America/New_York',
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced teams master table
CREATE TABLE curated.teams_master (
    id INTEGER PRIMARY KEY,
    team_id INTEGER NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(50) NOT NULL,
    short_name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    abbr VARCHAR(5) NOT NULL,
    logo VARCHAR(500),
    primary_color CHAR(6),
    secondary_color CHAR(6),
    conference_type VARCHAR(2) CHECK (conference_type IN ('AL', 'NL')),
    division_type VARCHAR(10) CHECK (division_type IN ('EAST', 'CENTRAL', 'WEST')),
    url_slug VARCHAR(100),
    -- Enhanced fields
    action_network_id INTEGER,
    action_network_data JSONB,
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced games_complete table 
CREATE TABLE curated.games_complete (
    id INTEGER PRIMARY KEY,
    sportsbookreview_game_id VARCHAR(100) UNIQUE,
    mlb_stats_api_game_id VARCHAR(20),
    action_network_game_id INTEGER,
    vsin_game_id VARCHAR(50),
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    game_status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(5),
    venue_name VARCHAR(200),
    venue_id INTEGER,
    season INTEGER,
    season_type VARCHAR(20) DEFAULT 'regular',
    game_type VARCHAR(20) DEFAULT 'regular',
    weather_condition VARCHAR(20),
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction VARCHAR(10),
    humidity INTEGER,
    data_quality VARCHAR(20) DEFAULT 'HIGH',
    mlb_correlation_confidence NUMERIC(5,4),
    has_mlb_enrichment BOOLEAN DEFAULT FALSE,
    -- Enhanced fields
    total_score INTEGER GENERATED ALWAYS AS (COALESCE(home_score, 0) + COALESCE(away_score, 0)) STORED,
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    -- Temporal tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced game_outcomes table
CREATE TABLE curated.game_outcomes (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.games_complete(id),
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    home_win BOOLEAN NOT NULL,
    over BOOLEAN NOT NULL,
    home_cover_spread BOOLEAN,
    total_line DOUBLE PRECISION,
    home_spread_line DOUBLE PRECISION,
    game_date TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Enhanced outcome data
    total_score INTEGER GENERATED ALWAYS AS (home_score + away_score) STORED,
    margin_of_victory INTEGER GENERATED ALWAYS AS (ABS(home_score - away_score)) STORED,
    winning_team VARCHAR(5) GENERATED ALWAYS AS (
        CASE WHEN home_score > away_score THEN home_team
             WHEN away_score > home_score THEN away_team
             ELSE NULL END
    ) STORED,
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    -- Temporal tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(game_id)
);

-- Unified betting lines table (consolidates moneyline, spreads, totals)
CREATE TABLE curated.betting_lines_unified (
    id SERIAL PRIMARY KEY,
    external_line_id VARCHAR(100),
    game_id INTEGER NOT NULL REFERENCES curated.games_complete(id),
    sportsbook_id INTEGER NOT NULL REFERENCES curated.sportsbooks(id),
    market_type VARCHAR(20) NOT NULL, -- 'moneyline', 'spread', 'totals'
    line_type VARCHAR(20) NOT NULL,   -- 'opening', 'current', 'closing'
    
    -- Moneyline fields
    home_moneyline INTEGER,
    away_moneyline INTEGER,
    
    -- Spread fields  
    spread_home DECIMAL(4,1),
    spread_away DECIMAL(4,1),
    spread_home_odds INTEGER,
    spread_away_odds INTEGER,
    
    -- Totals fields
    total_line DECIMAL(4,1),
    over_odds INTEGER,
    under_odds INTEGER,
    
    -- Enhanced tracking
    line_movement_id UUID,
    previous_line_id INTEGER REFERENCES curated.betting_lines_unified(id),
    movement_amount DECIMAL(4,1),
    movement_direction VARCHAR(10), -- 'up', 'down', 'stable'
    
    -- Metadata
    recorded_at TIMESTAMP NOT NULL,
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(external_line_id, sportsbook_id, market_type, line_type),
    CHECK (market_type IN ('moneyline', 'spread', 'totals')),
    CHECK (line_type IN ('opening', 'current', 'closing'))
);

-- Enhanced sportsbook mappings table
CREATE TABLE curated.sportsbook_mappings (
    id SERIAL PRIMARY KEY,
    sportsbook_id INTEGER NOT NULL REFERENCES curated.sportsbooks(id),
    external_source VARCHAR(50) NOT NULL,
    external_sportsbook_id VARCHAR(100) NOT NULL,
    external_sportsbook_name VARCHAR(100),
    -- Enhanced confidence tracking
    mapping_confidence DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'active',
    last_validated TIMESTAMP,
    validation_method VARCHAR(50),
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(sportsbook_id, external_source, external_sportsbook_id)
);

-- Enhanced data sources table (if exists)
CREATE TABLE curated.data_sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL DEFAULT 'api',
    endpoint_url VARCHAR(500),
    -- Enhanced reliability tracking
    reliability_score DECIMAL(3,2) DEFAULT 1.0,
    avg_response_time_ms INTEGER,
    success_rate DECIMAL(5,4) DEFAULT 1.0,
    last_successful_collection TIMESTAMP,
    total_collections INTEGER DEFAULT 0,
    failed_collections INTEGER DEFAULT 0,
    -- Configuration
    rate_limit_per_minute INTEGER DEFAULT 60,
    timeout_seconds INTEGER DEFAULT 30,
    retry_attempts INTEGER DEFAULT 3,
    is_active BOOLEAN DEFAULT TRUE,
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_games_complete_date ON curated.games_complete(game_date);
CREATE INDEX idx_games_complete_teams ON curated.games_complete(home_team, away_team);
CREATE INDEX idx_games_complete_mlb_api ON curated.games_complete(mlb_stats_api_game_id);
CREATE INDEX idx_games_complete_sbr ON curated.games_complete(sportsbookreview_game_id);

CREATE INDEX idx_game_outcomes_game_id ON curated.game_outcomes(game_id);
CREATE INDEX idx_game_outcomes_date ON curated.game_outcomes(game_date);

CREATE INDEX idx_betting_lines_game_id ON curated.betting_lines_unified(game_id);
CREATE INDEX idx_betting_lines_sportsbook ON curated.betting_lines_unified(sportsbook_id);
CREATE INDEX idx_betting_lines_market_type ON curated.betting_lines_unified(market_type);
CREATE INDEX idx_betting_lines_recorded_at ON curated.betting_lines_unified(recorded_at);
CREATE INDEX idx_betting_lines_composite ON curated.betting_lines_unified(game_id, sportsbook_id, market_type);

CREATE INDEX idx_sportsbook_mappings_sportsbook ON curated.sportsbook_mappings(sportsbook_id);
CREATE INDEX idx_sportsbook_mappings_external ON curated.sportsbook_mappings(external_source, external_sportsbook_id);

-- Log phase completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, end_time)
VALUES ('PHASE_2', 'CREATE_CURATED_TABLES_CORRECTED', 'completed', CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 3: MIGRATE UNIQUE DATA (MASTER DATA) - CORRECTED
-- -------------------------------------------------------------------------

-- Migrate sportsbooks (foundational data) - CORRECTED FIELDS
INSERT INTO curated.sportsbooks (
    id, name, display_name, abbreviation, is_active, supports_live_betting,
    reliability_score, source_system, created_at, updated_at, migrated_at
)
SELECT 
    id, name, display_name, abbreviation, is_active, supports_live_betting,
    1.0 as reliability_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.sportsbooks
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    display_name = EXCLUDED.display_name,
    abbreviation = EXCLUDED.abbreviation,
    is_active = EXCLUDED.is_active,
    supports_live_betting = EXCLUDED.supports_live_betting,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate teams (foundational data) - CORRECTED FIELDS
INSERT INTO curated.teams_master (
    id, team_id, full_name, display_name, short_name, location, abbr,
    logo, primary_color, secondary_color, conference_type, division_type, url_slug,
    action_network_id, action_network_data, source_system, is_active, 
    created_at, updated_at, migrated_at
)
SELECT 
    id, team_id, full_name, display_name, short_name, location, abbr,
    logo, primary_color, secondary_color, conference_type, division_type, url_slug,
    action_network_id, action_network_data,
    'core_betting_migration' as source_system,
    TRUE as is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.teams_master
ON CONFLICT (id) DO UPDATE SET
    team_id = EXCLUDED.team_id,
    full_name = EXCLUDED.full_name,
    display_name = EXCLUDED.display_name,
    short_name = EXCLUDED.short_name,
    location = EXCLUDED.location,
    abbr = EXCLUDED.abbr,
    logo = EXCLUDED.logo,
    primary_color = EXCLUDED.primary_color,
    secondary_color = EXCLUDED.secondary_color,
    conference_type = EXCLUDED.conference_type,
    division_type = EXCLUDED.division_type,
    url_slug = EXCLUDED.url_slug,
    action_network_id = EXCLUDED.action_network_id,
    action_network_data = EXCLUDED.action_network_data,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate sportsbook external mappings (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core_betting' AND table_name = 'sportsbook_external_mappings') THEN
        INSERT INTO curated.sportsbook_mappings (
            sportsbook_id, external_source, external_sportsbook_id, external_sportsbook_name,
            mapping_confidence, validation_status, source_system, created_at, updated_at, migrated_at
        )
        SELECT 
            sportsbook_id, external_source, external_sportsbook_id, external_sportsbook_name,
            1.0 as mapping_confidence,
            'active' as validation_status,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.sportsbook_mappings
        ON CONFLICT (sportsbook_id, external_source, external_sportsbook_id) DO UPDATE SET
            external_sportsbook_name = EXCLUDED.external_sportsbook_name,
            mapping_confidence = EXCLUDED.mapping_confidence,
            updated_at = CURRENT_TIMESTAMP,
            migrated_at = CURRENT_TIMESTAMP;
        
        RAISE NOTICE 'Migrated sportsbook external mappings';
    ELSE
        RAISE NOTICE 'sportsbook_external_mappings table does not exist, skipping';
    END IF;
END $$;

-- Log unique data migration
INSERT INTO operational.core_betting_migration_log (phase, operation, status, records_processed, end_time)
VALUES ('PHASE_3', 'MIGRATE_UNIQUE_DATA_CORRECTED', 'completed', 
    (SELECT COUNT(*) FROM curated.sportsbooks WHERE source_system = 'core_betting_migration') +
    (SELECT COUNT(*) FROM curated.teams_master WHERE source_system = 'core_betting_migration'),
    CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 4: MIGRATE PRIMARY DATA - CORRECTED
-- -------------------------------------------------------------------------

-- Migrate games (using actual field names)
INSERT INTO curated.games_complete (
    id, sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id, vsin_game_id,
    home_team, away_team, game_date, game_datetime, game_status, home_score, away_score, winning_team,
    venue_name, venue_id, season, season_type, game_type, weather_condition, temperature,
    wind_speed, wind_direction, humidity, data_quality, mlb_correlation_confidence, has_mlb_enrichment,
    data_quality_score, source_system, created_at, updated_at, migrated_at
)
SELECT 
    id, sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id, vsin_game_id,
    home_team, away_team, game_date, game_datetime, game_status, home_score, away_score, winning_team,
    venue_name, venue_id, season, season_type, game_type, weather_condition, temperature,
    wind_speed, wind_direction, humidity, data_quality, mlb_correlation_confidence, has_mlb_enrichment,
    1.0 as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.games_complete
ON CONFLICT (id) DO UPDATE SET
    sportsbookreview_game_id = EXCLUDED.sportsbookreview_game_id,
    mlb_stats_api_game_id = EXCLUDED.mlb_stats_api_game_id,
    action_network_game_id = EXCLUDED.action_network_game_id,
    vsin_game_id = EXCLUDED.vsin_game_id,
    home_team = EXCLUDED.home_team,
    away_team = EXCLUDED.away_team,
    game_date = EXCLUDED.game_date,
    game_datetime = EXCLUDED.game_datetime,
    game_status = EXCLUDED.game_status,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    winning_team = EXCLUDED.winning_team,
    venue_name = EXCLUDED.venue_name,
    venue_id = EXCLUDED.venue_id,
    season = EXCLUDED.season,
    season_type = EXCLUDED.season_type,
    game_type = EXCLUDED.game_type,
    weather_condition = EXCLUDED.weather_condition,
    temperature = EXCLUDED.temperature,
    wind_speed = EXCLUDED.wind_speed,
    wind_direction = EXCLUDED.wind_direction,
    humidity = EXCLUDED.humidity,
    data_quality = EXCLUDED.data_quality,
    mlb_correlation_confidence = EXCLUDED.mlb_correlation_confidence,
    has_mlb_enrichment = EXCLUDED.has_mlb_enrichment,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate game outcomes (using actual field names)
INSERT INTO curated.game_outcomes (
    game_id, home_team, away_team, home_score, away_score, home_win, over,
    home_cover_spread, total_line, home_spread_line, game_date,
    confidence_score, source_system, created_at, updated_at, migrated_at
)
SELECT 
    game_id, home_team, away_team, home_score, away_score, home_win, over,
    home_cover_spread, total_line, home_spread_line, game_date,
    1.0 as confidence_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.game_outcomes
WHERE game_id IN (SELECT id FROM curated.games_complete)
ON CONFLICT (game_id) DO UPDATE SET
    home_team = EXCLUDED.home_team,
    away_team = EXCLUDED.away_team,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    home_win = EXCLUDED.home_win,
    over = EXCLUDED.over,
    home_cover_spread = EXCLUDED.home_cover_spread,
    total_line = EXCLUDED.total_line,
    home_spread_line = EXCLUDED.home_spread_line,
    game_date = EXCLUDED.game_date,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Log primary data migration
INSERT INTO operational.core_betting_migration_log (phase, operation, status, records_processed, end_time)
VALUES ('PHASE_4', 'MIGRATE_PRIMARY_DATA_CORRECTED', 'completed', 
    (SELECT COUNT(*) FROM curated.games_complete WHERE source_system = 'core_betting_migration') +
    (SELECT COUNT(*) FROM curated.game_outcomes WHERE source_system = 'core_betting_migration'),
    CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 5: CONSOLIDATE BETTING LINES (COMPLEX MIGRATION) - CORRECTED
-- -------------------------------------------------------------------------

-- Check actual betting lines table structures first
DO $$
DECLARE
    moneyline_count INTEGER := 0;
    spread_count INTEGER := 0;
    totals_count INTEGER := 0;
BEGIN
    -- Check moneyline table structure and migrate
    SELECT COUNT(*) INTO moneyline_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline';
    
    IF moneyline_count > 0 THEN
        -- Get a sample record to understand structure
        INSERT INTO curated.betting_lines_unified (
            external_line_id, game_id, sportsbook_id, market_type, line_type,
            home_moneyline, away_moneyline, recorded_at, data_quality_score,
            source_system, created_at, migrated_at
        )
        SELECT 
            CONCAT('ml_', id) as external_line_id,
            game_id, sportsbook_id, 'moneyline' as market_type, 
            COALESCE(line_type, 'current') as line_type,
            home_odds as home_moneyline, away_odds as away_moneyline,
            COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
            1.0 as data_quality_score,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
        WHERE game_id IN (SELECT id FROM curated.games_complete)
          AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
        ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;
        
        RAISE NOTICE 'Migrated % moneyline records', moneyline_count;
    END IF;

    -- Check spread table structure and migrate
    SELECT COUNT(*) INTO spread_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread';
    
    IF spread_count > 0 THEN
        INSERT INTO curated.betting_lines_unified (
            external_line_id, game_id, sportsbook_id, market_type, line_type,
            spread_home, spread_away, spread_home_odds, spread_away_odds,
            recorded_at, data_quality_score, source_system, created_at, migrated_at
        )
        SELECT 
            CONCAT('sp_', id) as external_line_id,
            game_id, sportsbook_id, 'spread' as market_type,
            COALESCE(line_type, 'current') as line_type,
            home_spread as spread_home, 
            away_spread as spread_away,
            home_odds as spread_home_odds, 
            away_odds as spread_away_odds,
            COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
            1.0 as data_quality_score,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'
        WHERE game_id IN (SELECT id FROM curated.games_complete)
          AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
        ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;
        
        RAISE NOTICE 'Migrated % spread records', spread_count;
    END IF;

    -- Check totals table structure and migrate
    SELECT COUNT(*) INTO totals_count FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals';
    
    IF totals_count > 0 THEN
        INSERT INTO curated.betting_lines_unified (
            external_line_id, game_id, sportsbook_id, market_type, line_type,
            total_line, over_odds, under_odds, recorded_at, data_quality_score,
            source_system, created_at, migrated_at
        )
        SELECT 
            CONCAT('tot_', id) as external_line_id,
            game_id, sportsbook_id, 'totals' as market_type,
            COALESCE(line_type, 'current') as line_type,
            total_line, over_odds, under_odds,
            COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
            1.0 as data_quality_score,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
        WHERE game_id IN (SELECT id FROM curated.games_complete)
          AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
        ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;
        
        RAISE NOTICE 'Migrated % totals records', totals_count;
    END IF;

    RAISE NOTICE 'Total betting lines consolidated: %', moneyline_count + spread_count + totals_count;
END $$;

-- Log betting lines consolidation
INSERT INTO operational.core_betting_migration_log (phase, operation, status, records_processed, end_time)
VALUES ('PHASE_5', 'CONSOLIDATE_BETTING_LINES_CORRECTED', 'completed', 
    (SELECT COUNT(*) FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'),
    CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 6: COMPREHENSIVE VALIDATION - CORRECTED
-- -------------------------------------------------------------------------

-- Record post-migration row counts
INSERT INTO operational.post_migration_counts (table_name, record_count)
SELECT 'curated.games_complete', COUNT(*) FROM curated.games_complete WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.game_outcomes', COUNT(*) FROM curated.game_outcomes WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.betting_lines_unified', COUNT(*) FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.sportsbooks', COUNT(*) FROM curated.sportsbooks WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.teams_master', COUNT(*) FROM curated.teams_master WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.sportsbook_mappings', COUNT(*) FROM curated.sportsbook_mappings WHERE source_system = 'core_betting_migration';

-- Create corrected validation view
CREATE OR REPLACE VIEW operational.v_core_betting_migration_validation_corrected AS
WITH pre_counts AS (
    SELECT 
        CASE 
            WHEN table_name LIKE '%betting_lines_%' THEN 'curated.betting_lines_unified'
            WHEN table_name = 'curated.games_complete' THEN 'curated.games_complete'
            WHEN table_name = 'curated.game_outcomes' THEN 'curated.game_outcomes'
            WHEN table_name = 'curated.sportsbooks' THEN 'curated.sportsbooks'
            WHEN table_name = 'curated.teams_master' THEN 'curated.teams_master'
            WHEN table_name = 'curated.sportsbook_mappings' THEN 'curated.sportsbook_mappings'
        END as target_table,
        SUM(record_count) as pre_migration_count
    FROM operational.pre_migration_counts
    WHERE table_name NOT LIKE '%supplementary%'
    GROUP BY 1
),
post_counts AS (
    SELECT 
        table_name as target_table,
        record_count as post_migration_count
    FROM operational.post_migration_counts
)
SELECT 
    COALESCE(pre.target_table, post.target_table) as table_name,
    COALESCE(pre.pre_migration_count, 0) as pre_migration_count,
    COALESCE(post.post_migration_count, 0) as post_migration_count,
    COALESCE(post.post_migration_count, 0) - COALESCE(pre.pre_migration_count, 0) as count_difference,
    CASE 
        WHEN COALESCE(pre.pre_migration_count, 0) = COALESCE(post.post_migration_count, 0) THEN '✅ MATCH'
        WHEN COALESCE(post.post_migration_count, 0) > COALESCE(pre.pre_migration_count, 0) THEN '⚠️ MORE_RECORDS'
        WHEN COALESCE(post.post_migration_count, 0) = 0 AND COALESCE(pre.pre_migration_count, 0) > 0 THEN '❌ MISSING_RECORDS'
        ELSE '⚠️ PARTIAL_MIGRATION'
    END as validation_status
FROM pre_counts pre
FULL OUTER JOIN post_counts post ON pre.target_table = post.target_table
ORDER BY table_name;

-- Log validation completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, end_time)
VALUES ('PHASE_6', 'COMPREHENSIVE_VALIDATION_CORRECTED', 'completed', CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 7: FINAL MIGRATION STATUS - CORRECTED
-- -------------------------------------------------------------------------

-- Generate final migration summary
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details, end_time)
VALUES ('PHASE_7', 'CORRECTED_MIGRATION_COMPLETE', 'completed', 
    jsonb_build_object(
        'total_phases', 7,
        'migration_completed_at', CURRENT_TIMESTAMP,
        'validation_view', 'operational.v_core_betting_migration_validation_corrected',
        'status_view', 'operational.v_core_betting_migration_status',
        'next_steps', ARRAY[
            'Review corrected validation results',
            'Test application functionality', 
            'Execute code refactoring',
            'Update external FK constraints (manual step)',
            'Drop core_betting schema (only after validation)'
        ]
    ),
    CURRENT_TIMESTAMP);

-- Display final results
SELECT 'CORRECTED CORE BETTING MIGRATION COMPLETED' as status;
SELECT * FROM operational.v_core_betting_migration_validation_corrected;