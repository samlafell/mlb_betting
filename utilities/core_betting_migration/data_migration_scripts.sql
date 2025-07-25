-- =========================================================================
-- Core Betting Schema Decommission - Complete Data Migration Scripts
-- =========================================================================
-- Generated: 2025-01-25
-- Purpose: Migrate all data from core_betting schema to curated schema
-- Requirements: PostgreSQL 12+, operational schema for tracking
-- 
-- IMPORTANT: Review all scripts before execution!
-- This migration is designed to be run in phases with validation checkpoints.
-- =========================================================================

-- -------------------------------------------------------------------------
-- PHASE 1: PRE-MIGRATION SETUP AND VALIDATION
-- -------------------------------------------------------------------------

-- Create migration tracking table
CREATE SCHEMA IF NOT EXISTS operational;

CREATE TABLE IF NOT EXISTS operational.core_betting_migration_log (
    id SERIAL PRIMARY KEY,
    phase VARCHAR(50) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'started',
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_expected INTEGER DEFAULT 0,
    error_message TEXT,
    details JSONB
);

-- Log migration start
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details)
VALUES ('PHASE_1', 'MIGRATION_START', 'started', '{"description": "Core betting schema decommission migration"}');

-- Create pre-migration backup tracking
CREATE TABLE IF NOT EXISTS operational.pre_migration_counts (
    table_name VARCHAR(100) PRIMARY KEY,
    record_count INTEGER NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Record current row counts for validation
INSERT INTO operational.pre_migration_counts (table_name, record_count)
SELECT 'curated.games_complete', COUNT(*) FROM curated.games_complete
UNION ALL
SELECT 'curated.game_outcomes', COUNT(*) FROM curated.game_outcomes
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'
UNION ALL
SELECT 'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'', COUNT(*) FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
UNION ALL
SELECT 'curated.sportsbooks', COUNT(*) FROM curated.sportsbooks
UNION ALL
SELECT 'curated.teams_master', COUNT(*) FROM curated.teams_master
UNION ALL
SELECT 'curated.sportsbook_mappings', COUNT(*) FROM curated.sportsbook_mappings
UNION ALL
SELECT 'curated.data_sources', COUNT(*) FROM curated.data_sources
UNION ALL
SELECT 'curated.games_complete', COUNT(*) FROM curated.games_complete;

-- Validate schema accessibility
SELECT 'core_betting' as schema_name, COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'core_betting';

-- -------------------------------------------------------------------------
-- PHASE 2: CREATE ENHANCED CURATED SCHEMA TABLES
-- -------------------------------------------------------------------------

-- Enhanced games_complete table (merges games + supplementary_games)
CREATE TABLE IF NOT EXISTS curated.games_complete (
    id INTEGER PRIMARY KEY,
    external_game_id VARCHAR(100) UNIQUE,
    mlb_api_game_id INTEGER,
    game_date DATE NOT NULL,
    game_time TIME,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    game_type VARCHAR(20) DEFAULT 'regular',
    game_status VARCHAR(20) DEFAULT 'scheduled',
    venue VARCHAR(100),
    weather_conditions JSONB,
    -- Enhanced fields
    is_postponed BOOLEAN DEFAULT FALSE,
    postponed_reason TEXT,
    original_date DATE,
    doubleheader_flag VARCHAR(10),
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    -- Temporal tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced game_outcomes table
CREATE TABLE IF NOT EXISTS curated.game_outcomes (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.games_complete(id),
    home_team_score INTEGER,
    away_team_score INTEGER,
    total_score INTEGER GENERATED ALWAYS AS (COALESCE(home_team_score, 0) + COALESCE(away_team_score, 0)) STORED,
    outcome_type VARCHAR(20) DEFAULT 'final',
    innings_played INTEGER DEFAULT 9,
    -- Enhanced outcome data
    winning_team VARCHAR(10),
    margin_of_victory INTEGER,
    total_over_under VARCHAR(10), -- 'over', 'under', 'push'
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    -- Temporal tracking
    recorded_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(game_id, outcome_type)
);

-- Unified betting lines table (consolidates moneyline, spreads, totals)
CREATE TABLE IF NOT EXISTS curated.betting_lines_unified (
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

-- Enhanced sportsbooks table
CREATE TABLE IF NOT EXISTS curated.sportsbooks (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    short_name VARCHAR(20),
    external_source_id VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    reliability_score DECIMAL(3,2) DEFAULT 1.0,
    -- Enhanced metadata
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
CREATE TABLE IF NOT EXISTS curated.teams_master (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(10) UNIQUE NOT NULL,
    city VARCHAR(100),
    league VARCHAR(10) DEFAULT 'MLB',
    division VARCHAR(10),
    -- External ID support
    mlb_api_team_id INTEGER UNIQUE,
    external_team_ids JSONB,
    -- Enhanced metadata
    stadium_name VARCHAR(100),
    timezone VARCHAR(50),
    primary_color VARCHAR(7),
    secondary_color VARCHAR(7),
    -- Source tracking
    source_system VARCHAR(50) DEFAULT 'core_betting_migration',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced sportsbook mappings table
CREATE TABLE IF NOT EXISTS curated.sportsbook_mappings (
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

-- Enhanced data sources table
CREATE TABLE IF NOT EXISTS curated.data_sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL, -- 'api', 'scraper', 'manual'
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
CREATE INDEX IF NOT EXISTS idx_games_complete_date ON curated.games_complete(game_date);
CREATE INDEX IF NOT EXISTS idx_games_complete_teams ON curated.games_complete(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_games_complete_external ON curated.games_complete(external_game_id);
CREATE INDEX IF NOT EXISTS idx_games_complete_mlb_api ON curated.games_complete(mlb_api_game_id);

CREATE INDEX IF NOT EXISTS idx_game_outcomes_game_id ON curated.game_outcomes(game_id);
CREATE INDEX IF NOT EXISTS idx_game_outcomes_type ON curated.game_outcomes(outcome_type);

CREATE INDEX IF NOT EXISTS idx_betting_lines_game_id ON curated.betting_lines_unified(game_id);
CREATE INDEX IF NOT EXISTS idx_betting_lines_sportsbook ON curated.betting_lines_unified(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_betting_lines_market_type ON curated.betting_lines_unified(market_type);
CREATE INDEX IF NOT EXISTS idx_betting_lines_recorded_at ON curated.betting_lines_unified(recorded_at);
CREATE INDEX IF NOT EXISTS idx_betting_lines_composite ON curated.betting_lines_unified(game_id, sportsbook_id, market_type);

CREATE INDEX IF NOT EXISTS idx_sportsbook_mappings_sportsbook ON curated.sportsbook_mappings(sportsbook_id);
CREATE INDEX IF NOT EXISTS idx_sportsbook_mappings_external ON curated.sportsbook_mappings(external_source, external_sportsbook_id);

-- Log phase completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, end_time)
VALUES ('PHASE_2', 'CREATE_CURATED_TABLES', 'completed', CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 3: MIGRATE UNIQUE DATA (MASTER DATA)
-- -------------------------------------------------------------------------

-- Migrate sportsbooks (foundational data)
INSERT INTO curated.sportsbooks (
    id, name, short_name, external_source_id, is_active,
    reliability_score, source_system, created_at, updated_at, migrated_at
)
SELECT 
    id, name, short_name, external_source_id, is_active,
    1.0 as reliability_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.sportsbooks
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    short_name = EXCLUDED.short_name,
    external_source_id = EXCLUDED.external_source_id,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate teams (foundational data)
INSERT INTO curated.teams_master (
    id, name, abbreviation, city, league, division,
    source_system, is_active, created_at, updated_at, migrated_at
)
SELECT 
    id, name, abbreviation, city, 
    COALESCE(league, 'MLB') as league,
    division,
    'core_betting_migration' as source_system,
    TRUE as is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.teams_master
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    abbreviation = EXCLUDED.abbreviation,
    city = EXCLUDED.city,
    league = EXCLUDED.league,
    division = EXCLUDED.division,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate sportsbook external mappings
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

-- Migrate data source metadata
INSERT INTO curated.data_sources (
    source_name, source_type, endpoint_url, reliability_score,
    is_active, source_system, created_at, updated_at, migrated_at
)
SELECT 
    source_name, 
    COALESCE(source_type, 'api') as source_type,
    endpoint_url,
    COALESCE(reliability_score, 1.0) as reliability_score,
    COALESCE(is_active, TRUE) as is_active,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.data_sources
ON CONFLICT (source_name) DO UPDATE SET
    source_type = EXCLUDED.source_type,
    endpoint_url = EXCLUDED.endpoint_url,
    reliability_score = EXCLUDED.reliability_score,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Log unique data migration
INSERT INTO operational.core_betting_migration_log (phase, operation, status, records_processed, end_time)
VALUES ('PHASE_3', 'MIGRATE_UNIQUE_DATA', 'completed', 
    (SELECT SUM(record_count) FROM operational.pre_migration_counts 
     WHERE table_name IN ('curated.sportsbooks', 'curated.teams_master', 
                          'curated.sportsbook_mappings', 'curated.data_sources')),
    CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 4: MIGRATE PRIMARY DATA WITH CONSOLIDATION
-- -------------------------------------------------------------------------

-- Migrate games (consolidating with supplementary_games)
WITH games_consolidated AS (
    -- Main games
    SELECT 
        id, external_game_id, mlb_api_game_id, game_date, game_time,
        home_team, away_team, season, game_type, game_status, venue,
        weather_conditions, is_postponed, postponed_reason, original_date,
        doubleheader_flag, created_at, updated_at
    FROM curated.games_complete
    
    UNION ALL
    
    -- Supplementary games (with ID offset to avoid conflicts)
    SELECT 
        id + 100000 as id,  -- Offset to avoid ID conflicts
        external_game_id, mlb_api_game_id, game_date, game_time,
        home_team, away_team, season, game_type, game_status, venue,
        weather_conditions, is_postponed, postponed_reason, original_date,
        doubleheader_flag, created_at, updated_at
    FROM curated.games_complete
    WHERE NOT EXISTS (
        SELECT 1 FROM curated.games_complete g 
        WHERE g.external_game_id = curated.games_complete.external_game_id
    )
)
INSERT INTO curated.games_complete (
    id, external_game_id, mlb_api_game_id, game_date, game_time,
    home_team, away_team, season, game_type, game_status, venue,
    weather_conditions, is_postponed, postponed_reason, original_date,
    doubleheader_flag, data_quality_score, source_system, 
    created_at, updated_at, migrated_at
)
SELECT 
    id, external_game_id, mlb_api_game_id, game_date, game_time,
    home_team, away_team, season, game_type, game_status, venue,
    weather_conditions, is_postponed, postponed_reason, original_date,
    doubleheader_flag,
    1.0 as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM games_consolidated
ON CONFLICT (id) DO UPDATE SET
    external_game_id = EXCLUDED.external_game_id,
    mlb_api_game_id = EXCLUDED.mlb_api_game_id,
    game_date = EXCLUDED.game_date,
    game_time = EXCLUDED.game_time,
    home_team = EXCLUDED.home_team,
    away_team = EXCLUDED.away_team,
    season = EXCLUDED.season,
    game_type = EXCLUDED.game_type,
    game_status = EXCLUDED.game_status,
    venue = EXCLUDED.venue,
    weather_conditions = EXCLUDED.weather_conditions,
    is_postponed = EXCLUDED.is_postponed,
    postponed_reason = EXCLUDED.postponed_reason,
    original_date = EXCLUDED.original_date,
    doubleheader_flag = EXCLUDED.doubleheader_flag,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate game outcomes
INSERT INTO curated.game_outcomes (
    game_id, home_team_score, away_team_score, outcome_type, innings_played,
    winning_team, margin_of_victory, recorded_at, confidence_score,
    source_system, created_at, updated_at, migrated_at
)
SELECT 
    game_id, home_team_score, away_team_score, 
    COALESCE(outcome_type, 'final') as outcome_type,
    COALESCE(innings_played, 9) as innings_played,
    CASE 
        WHEN home_team_score > away_team_score THEN 'home'
        WHEN away_team_score > home_team_score THEN 'away'
        ELSE 'tie'
    END as winning_team,
    ABS(COALESCE(home_team_score, 0) - COALESCE(away_team_score, 0)) as margin_of_victory,
    COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    1.0 as confidence_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as updated_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.game_outcomes
WHERE game_id IN (SELECT id FROM curated.games_complete)
ON CONFLICT (game_id, outcome_type) DO UPDATE SET
    home_team_score = EXCLUDED.home_team_score,
    away_team_score = EXCLUDED.away_team_score,
    innings_played = EXCLUDED.innings_played,
    winning_team = EXCLUDED.winning_team,
    margin_of_victory = EXCLUDED.margin_of_victory,
    recorded_at = EXCLUDED.recorded_at,
    updated_at = CURRENT_TIMESTAMP,
    migrated_at = CURRENT_TIMESTAMP;

-- -------------------------------------------------------------------------
-- PHASE 5: CONSOLIDATE BETTING LINES (COMPLEX MIGRATION)
-- -------------------------------------------------------------------------

-- Migrate moneyline betting lines
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    home_moneyline, away_moneyline, recorded_at, data_quality_score,
    source_system, created_at, migrated_at
)
SELECT 
    CONCAT('ml_', id) as external_line_id,
    game_id, sportsbook_id, 'moneyline' as market_type, line_type,
    home_odds as home_moneyline, away_odds as away_moneyline,
    COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    1.0 as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
WHERE game_id IN (SELECT id FROM curated.games_complete)
  AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO UPDATE SET
    home_moneyline = EXCLUDED.home_moneyline,
    away_moneyline = EXCLUDED.away_moneyline,
    recorded_at = EXCLUDED.recorded_at,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate spread betting lines
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    spread_home, spread_away, spread_home_odds, spread_away_odds,
    recorded_at, data_quality_score, source_system, created_at, migrated_at
)
SELECT 
    CONCAT('sp_', id) as external_line_id,
    game_id, sportsbook_id, 'spread' as market_type, line_type,
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
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO UPDATE SET
    spread_home = EXCLUDED.spread_home,
    spread_away = EXCLUDED.spread_away,
    spread_home_odds = EXCLUDED.spread_home_odds,
    spread_away_odds = EXCLUDED.spread_away_odds,
    recorded_at = EXCLUDED.recorded_at,
    migrated_at = CURRENT_TIMESTAMP;

-- Migrate totals betting lines
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    total_line, over_odds, under_odds, recorded_at, data_quality_score,
    source_system, created_at, migrated_at
)
SELECT 
    CONCAT('tot_', id) as external_line_id,
    game_id, sportsbook_id, 'totals' as market_type, line_type,
    total_line, over_odds, under_odds,
    COALESCE(recorded_at, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    1.0 as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
WHERE game_id IN (SELECT id FROM curated.games_complete)
  AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO UPDATE SET
    total_line = EXCLUDED.total_line,
    over_odds = EXCLUDED.over_odds,
    under_odds = EXCLUDED.under_odds,
    recorded_at = EXCLUDED.recorded_at,
    migrated_at = CURRENT_TIMESTAMP;

-- Log betting lines consolidation
INSERT INTO operational.core_betting_migration_log (phase, operation, status, records_processed, end_time)
VALUES ('PHASE_5', 'CONSOLIDATE_BETTING_LINES', 'completed', 
    (SELECT COUNT(*) FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'),
    CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 6: UPDATE EXTERNAL FOREIGN KEY CONSTRAINTS
-- -------------------------------------------------------------------------

-- Update analytics schema FK constraints to point to curated
-- Note: These statements may need to be run individually with careful coordination

-- Analytics schema updates
ALTER TABLE analytics.betting_recommendations 
DROP CONSTRAINT IF EXISTS betting_recommendations_game_id_fkey,
ADD CONSTRAINT betting_recommendations_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

ALTER TABLE analytics.confidence_scores 
DROP CONSTRAINT IF EXISTS confidence_scores_game_id_fkey,
ADD CONSTRAINT confidence_scores_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

ALTER TABLE analytics.cross_market_analysis 
DROP CONSTRAINT IF EXISTS cross_market_analysis_game_id_fkey,
ADD CONSTRAINT cross_market_analysis_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

ALTER TABLE analytics.strategy_signals 
DROP CONSTRAINT IF EXISTS strategy_signals_game_id_fkey,
ADD CONSTRAINT strategy_signals_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Curated schema updates (for tables that reference sportsbooks)
ALTER TABLE curated.arbitrage_opportunities 
DROP CONSTRAINT IF EXISTS arbitrage_opportunities_sportsbook1_id_fkey,
DROP CONSTRAINT IF EXISTS arbitrage_opportunities_sportsbook2_id_fkey,
ADD CONSTRAINT arbitrage_opportunities_sportsbook1_id_fkey 
FOREIGN KEY (sportsbook1_id) REFERENCES curated.sportsbooks(id),
ADD CONSTRAINT arbitrage_opportunities_sportsbook2_id_fkey 
FOREIGN KEY (sportsbook2_id) REFERENCES curated.sportsbooks(id);

ALTER TABLE curated.rlm_opportunities 
DROP CONSTRAINT IF EXISTS rlm_opportunities_sportsbook_id_fkey,
ADD CONSTRAINT rlm_opportunities_sportsbook_id_fkey 
FOREIGN KEY (sportsbook_id) REFERENCES curated.sportsbooks(id);

ALTER TABLE curated.steam_moves 
DROP CONSTRAINT IF EXISTS steam_moves_game_id_fkey,
ADD CONSTRAINT steam_moves_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Staging schema updates
ALTER TABLE staging.betting_splits 
DROP CONSTRAINT IF EXISTS betting_splits_game_id_fkey,
ADD CONSTRAINT betting_splits_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Log FK constraint updates
INSERT INTO operational.core_betting_migration_log (phase, operation, status, end_time)
VALUES ('PHASE_6', 'UPDATE_FOREIGN_KEY_CONSTRAINTS', 'completed', CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 7: COMPREHENSIVE VALIDATION
-- -------------------------------------------------------------------------

-- Create post-migration validation counts
CREATE TABLE IF NOT EXISTS operational.post_migration_counts (
    table_name VARCHAR(100) PRIMARY KEY,
    record_count INTEGER NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
SELECT 'curated.sportsbook_mappings', COUNT(*) FROM curated.sportsbook_mappings WHERE source_system = 'core_betting_migration'
UNION ALL
SELECT 'curated.data_sources', COUNT(*) FROM curated.data_sources WHERE source_system = 'core_betting_migration';

-- Validation query to compare record counts
CREATE OR REPLACE VIEW operational.v_core_betting_migration_validation AS
WITH pre_counts AS (
    SELECT 
        CASE 
            WHEN table_name LIKE '%betting_lines_%' THEN 'curated.betting_lines_unified'
            WHEN table_name = 'curated.games_complete' THEN 'curated.games_complete'
            WHEN table_name = 'curated.games_complete' THEN 'curated.games_complete'
            WHEN table_name = 'curated.game_outcomes' THEN 'curated.game_outcomes'
            WHEN table_name = 'curated.sportsbooks' THEN 'curated.sportsbooks'
            WHEN table_name = 'curated.teams_master' THEN 'curated.teams_master'
            WHEN table_name = 'curated.sportsbook_mappings' THEN 'curated.sportsbook_mappings'
            WHEN table_name = 'curated.data_sources' THEN 'curated.data_sources'
        END as target_table,
        SUM(record_count) as pre_migration_count
    FROM operational.pre_migration_counts
    WHERE table_name NOT IN ('curated.games_complete') -- Will be merged with games
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
        ELSE '❌ MISSING_RECORDS'
    END as validation_status
FROM pre_counts pre
FULL OUTER JOIN post_counts post ON pre.target_table = post.target_table
ORDER BY table_name;

-- Create migration status view
CREATE OR REPLACE VIEW operational.v_core_betting_migration_status AS
SELECT 
    phase,
    operation,
    status,
    records_processed,
    records_expected,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time)) as duration_seconds,
    error_message
FROM operational.core_betting_migration_log
ORDER BY id DESC;

-- Log validation completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, end_time)
VALUES ('PHASE_7', 'COMPREHENSIVE_VALIDATION', 'completed', CURRENT_TIMESTAMP);

-- -------------------------------------------------------------------------
-- PHASE 8: FINAL MIGRATION STATUS
-- -------------------------------------------------------------------------

-- Generate final migration summary
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details, end_time)
VALUES ('PHASE_8', 'MIGRATION_COMPLETE', 'completed', 
    jsonb_build_object(
        'total_phases', 8,
        'migration_completed_at', CURRENT_TIMESTAMP,
        'validation_view', 'operational.v_core_betting_migration_validation',
        'status_view', 'operational.v_core_betting_migration_status',
        'next_steps', ARRAY[
            'Review validation results',
            'Test application functionality', 
            'Execute code refactoring',
            'Drop core_betting schema (only after validation)'
        ]
    ),
    CURRENT_TIMESTAMP);

-- Display final results
SELECT 'CORE BETTING MIGRATION COMPLETED' as status;
SELECT * FROM operational.v_core_betting_migration_validation;

-- =========================================================================
-- CLEANUP PHASE (EXECUTE ONLY AFTER COMPLETE VALIDATION)
-- =========================================================================

/*
-- DANGER: Only execute this after complete validation and testing!
-- 
-- DROP SCHEMA core_betting CASCADE;
--
-- Remember to:
-- 1. Verify all validation checks pass
-- 2. Test all application functionality
-- 3. Complete code refactoring
-- 4. Get approval from stakeholders
-- 5. Create final backup before dropping schema
*/

-- =========================================================================
-- END OF MIGRATION SCRIPTS
-- =========================================================================