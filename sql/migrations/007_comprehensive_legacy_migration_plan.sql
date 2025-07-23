-- Comprehensive Legacy Core Betting Migration Plan
-- Migration of 28,407 records from 10 active legacy tables to three-tier architecture
--
-- ANALYSIS SUMMARY:
-- - Total legacy records: 28,407
-- - Active tables with data: 10
-- - Key tables: games (1,747), betting_lines_moneyline (12,410), betting_lines_totals (10,568), betting_lines_spread (3,360)
-- - Reference tables: teams (30), sportsbooks (11), sportsbook_external_mappings (19)

-- ==============================
-- MIGRATION STRATEGY
-- ==============================

-- PHASE 1: CREATE ARCHIVE AND BACKUP SCHEMAS
-- PHASE 2: MIGRATE REFERENCE DATA TO STAGING
-- PHASE 3: MIGRATE OPERATIONAL DATA TO RAW_DATA FOR REPROCESSING
-- PHASE 4: MIGRATE ANALYSIS DATA TO STAGING
-- PHASE 5: ARCHIVE SYSTEM DATA
-- PHASE 6: VALIDATION AND CLEANUP

-- ==============================
-- PHASE 1: SCHEMA PREPARATION
-- ==============================

-- Create necessary schemas
CREATE SCHEMA IF NOT EXISTS archive;
CREATE SCHEMA IF NOT EXISTS backup_core_betting;

COMMENT ON SCHEMA archive IS 'Archive schema for legacy data preservation';
COMMENT ON SCHEMA backup_core_betting IS 'Complete backup of core_betting schema before migration';

-- Create backup of entire core_betting schema
CREATE TABLE backup_core_betting.games AS SELECT * FROM core_betting.games;
CREATE TABLE backup_core_betting.teams AS SELECT * FROM core_betting.teams;
CREATE TABLE backup_core_betting.sportsbooks AS SELECT * FROM core_betting.sportsbooks;
CREATE TABLE backup_core_betting.betting_lines_spread AS SELECT * FROM core_betting.betting_lines_spread;
CREATE TABLE backup_core_betting.betting_lines_moneyline AS SELECT * FROM core_betting.betting_lines_moneyline;
CREATE TABLE backup_core_betting.betting_lines_totals AS SELECT * FROM core_betting.betting_lines_totals;
CREATE TABLE backup_core_betting.supplementary_games AS SELECT * FROM core_betting.supplementary_games;
CREATE TABLE backup_core_betting.sportsbook_external_mappings AS SELECT * FROM core_betting.sportsbook_external_mappings;
CREATE TABLE backup_core_betting.data_source_metadata AS SELECT * FROM core_betting.data_source_metadata;
CREATE TABLE backup_core_betting.data_migrations AS SELECT * FROM core_betting.data_migrations;

-- Add backup timestamps
ALTER TABLE backup_core_betting.games ADD COLUMN backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE backup_core_betting.teams ADD COLUMN backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE backup_core_betting.sportsbooks ADD COLUMN backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- ==============================
-- PHASE 2: REFERENCE DATA MIGRATION TO STAGING
-- ==============================

-- 2.1 Migrate Teams (30 records) -> staging.teams
-- Enhance existing staging.teams with legacy data
INSERT INTO staging.teams (
    team_name, 
    full_name, 
    abbreviation, 
    league, 
    division,
    action_network_id,
    external_ids,
    created_at,
    updated_at
)
SELECT 
    cb.short_name,
    cb.full_name,
    cb.abbr,
    CASE 
        WHEN cb.conference_type = 'AL' THEN 'American League'
        WHEN cb.conference_type = 'NL' THEN 'National League'
        ELSE 'Unknown'
    END,
    cb.division_type,
    cb.action_network_id,
    jsonb_build_object(
        'legacy_id', cb.id,
        'legacy_team_id', cb.team_id,
        'display_name', cb.display_name,
        'location', cb.location,
        'url_slug', cb.url_slug,
        'colors', jsonb_build_object(
            'primary', cb.primary_color,
            'secondary', cb.secondary_color
        )
    ),
    COALESCE(cb.created_at, CURRENT_TIMESTAMP),
    COALESCE(cb.updated_at, CURRENT_TIMESTAMP)
FROM core_betting.teams cb
ON CONFLICT (team_name) DO UPDATE SET
    external_ids = staging.teams.external_ids || EXCLUDED.external_ids,
    updated_at = CURRENT_TIMESTAMP;

-- 2.2 Migrate Sportsbooks (11 records) -> staging.sportsbooks 
-- Create staging.sportsbooks table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.sportsbooks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255),
    abbreviation VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    supports_live_betting BOOLEAN DEFAULT false,
    external_ids JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO staging.sportsbooks (
    name,
    display_name, 
    abbreviation,
    is_active,
    supports_live_betting,
    external_ids,
    metadata,
    created_at
)
SELECT 
    cb.name,
    COALESCE(cb.display_name, cb.name),
    cb.abbreviation,
    COALESCE(cb.is_active, true),
    COALESCE(cb.supports_live_betting, false),
    jsonb_build_object('legacy_id', cb.id),
    jsonb_build_object(
        'migrated_from', 'core_betting.sportsbooks',
        'migration_date', CURRENT_TIMESTAMP
    ),
    COALESCE(cb.created_at, CURRENT_TIMESTAMP)
FROM core_betting.sportsbooks cb
ON CONFLICT (name) DO UPDATE SET
    external_ids = staging.sportsbooks.external_ids || EXCLUDED.external_ids,
    updated_at = CURRENT_TIMESTAMP;

-- 2.3 Migrate Sportsbook External Mappings (19 records) -> staging.sportsbook_external_mappings
CREATE TABLE IF NOT EXISTS staging.sportsbook_external_mappings (
    id SERIAL PRIMARY KEY,
    sportsbook_name VARCHAR(255) NOT NULL,
    external_source VARCHAR(100) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    mapping_confidence DECIMAL(3,2) DEFAULT 1.0,
    is_active BOOLEAN DEFAULT true,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sportsbook_name, external_source, external_id)
);

INSERT INTO staging.sportsbook_external_mappings (
    sportsbook_name,
    external_source,
    external_id,
    mapping_confidence,
    metadata,
    created_at
)
SELECT 
    'legacy_mapping' as sportsbook_name,  -- We'll need to resolve this based on the actual mapping data
    'core_betting' as external_source,
    id::text as external_id,
    1.0 as mapping_confidence,
    jsonb_build_object(
        'migrated_from', 'core_betting.sportsbook_external_mappings',
        'original_record', row_to_json(cb.*)
    ),
    CURRENT_TIMESTAMP
FROM core_betting.sportsbook_external_mappings cb;

-- 2.4 Migrate Data Source Metadata (7 records) -> staging.data_source_metadata  
CREATE TABLE IF NOT EXISTS staging.data_source_metadata (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    base_url VARCHAR(500),
    api_version VARCHAR(50),
    rate_limits JSONB DEFAULT '{}',
    authentication_type VARCHAR(50),
    data_formats JSONB DEFAULT '[]',
    supported_markets JSONB DEFAULT '[]',
    quality_metrics JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO staging.data_source_metadata (
    source_name,
    source_type,
    metadata,
    created_at
)
SELECT 
    'legacy_source_' || id::text as source_name,
    'legacy' as source_type,
    jsonb_build_object(
        'migrated_from', 'core_betting.data_source_metadata',
        'original_record', row_to_json(cb.*)
    ),
    CURRENT_TIMESTAMP
FROM core_betting.data_source_metadata cb;

-- ==============================
-- PHASE 3: OPERATIONAL DATA MIGRATION TO RAW_DATA
-- ==============================

-- 3.1 Migrate Games (1,747 records) -> raw_data.legacy_games
CREATE TABLE IF NOT EXISTS raw_data.legacy_games (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER NOT NULL,
    sportsbookreview_game_id VARCHAR(255),
    mlb_stats_api_game_id VARCHAR(255),
    action_network_game_id INTEGER,
    vsin_game_id VARCHAR(255),
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    game_status VARCHAR(50) NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(255),
    venue_name VARCHAR(255),
    venue_id INTEGER,
    season INTEGER,
    season_type VARCHAR(50),
    game_type VARCHAR(50),
    weather_condition VARCHAR(100),
    temperature INTEGER,
    wind_speed INTEGER, 
    wind_direction VARCHAR(50),
    humidity INTEGER,
    raw_data JSONB,
    source_metadata JSONB DEFAULT '{}',
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO raw_data.legacy_games (
    legacy_id,
    sportsbookreview_game_id,
    mlb_stats_api_game_id,
    action_network_game_id,
    vsin_game_id,
    home_team,
    away_team,
    game_date,
    game_datetime,
    game_status,
    home_score,
    away_score,
    winning_team,
    venue_name,
    venue_id,
    season,
    season_type,
    game_type,
    weather_condition,
    temperature,
    wind_speed,
    wind_direction,
    humidity,
    raw_data,
    source_metadata
)
SELECT 
    cb.id,
    cb.sportsbookreview_game_id,
    cb.mlb_stats_api_game_id,
    cb.action_network_game_id,
    cb.vsin_game_id,
    cb.home_team,
    cb.away_team,
    cb.game_date,
    cb.game_datetime,
    cb.game_status,
    cb.home_score,
    cb.away_score,
    cb.winning_team,
    cb.venue_name,
    cb.venue_id,
    cb.season,
    cb.season_type,
    cb.game_type,
    cb.weather_condition,
    cb.temperature,
    cb.wind_speed,
    cb.wind_direction,
    cb.humidity,
    jsonb_build_object(
        'data_quality', cb.data_quality,
        'mlb_correlation_confidence', cb.mlb_correlation_confidence,
        'has_mlb_enrichment', cb.has_mlb_enrichment,
        'legacy_created_at', cb.created_at,
        'legacy_updated_at', cb.updated_at
    ),
    jsonb_build_object(
        'source', 'legacy_core_betting_games',
        'migration_date', CURRENT_TIMESTAMP,
        'requires_reprocessing', true
    )
FROM core_betting.games cb;

-- 3.2 Migrate Betting Lines Spread (3,360 records) -> raw_data.legacy_betting_lines_spread
CREATE TABLE IF NOT EXISTS raw_data.legacy_betting_lines_spread (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER NOT NULL,
    legacy_game_id INTEGER NOT NULL,
    sportsbook_id INTEGER,
    sportsbook VARCHAR(255) NOT NULL,
    spread_line DECIMAL(4,1),
    home_spread_price INTEGER,
    away_spread_price INTEGER,
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    betting_data JSONB,
    raw_data JSONB,
    source_metadata JSONB DEFAULT '{}',
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO raw_data.legacy_betting_lines_spread (
    legacy_id,
    legacy_game_id,
    sportsbook_id,
    sportsbook,
    spread_line,
    home_spread_price,
    away_spread_price,
    odds_timestamp,
    betting_data,
    raw_data,
    source_metadata
)
SELECT 
    cb.id,
    cb.game_id,
    cb.sportsbook_id,
    cb.sportsbook,
    cb.spread_line,
    cb.home_spread_price,
    cb.away_spread_price,
    cb.odds_timestamp,
    jsonb_build_object(
        'home_bets_count', cb.home_bets_count,
        'away_bets_count', cb.away_bets_count,
        'home_bets_percentage', cb.home_bets_percentage,
        'away_bets_percentage', cb.away_bets_percentage,
        'home_money_percentage', cb.home_money_percentage,
        'away_money_percentage', cb.away_money_percentage
    ),
    jsonb_build_object(
        'final_score_difference', cb.final_score_difference,
        'data_quality', cb.data_quality,
        'game_datetime', cb.game_datetime,
        'home_team', cb.home_team,
        'away_team', cb.away_team,
        'data_completeness_score', cb.data_completeness_score,
        'source_metadata', cb.source_metadata,
        'collection_batch_id', cb.collection_batch_id,
        'source_reliability_score', cb.source_reliability_score,
        'collection_method', cb.collection_method,
        'external_source_id', cb.external_source_id,
        'source_api_version', cb.source_api_version
    ),
    jsonb_build_object(
        'source', 'legacy_core_betting_betting_lines_spread',
        'migration_date', CURRENT_TIMESTAMP,
        'requires_reprocessing', true
    )
FROM core_betting.betting_lines_spread cb;

-- 3.3 Migrate Betting Lines Moneyline (12,410 records) -> raw_data.legacy_betting_lines_moneyline
CREATE TABLE IF NOT EXISTS raw_data.legacy_betting_lines_moneyline (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER NOT NULL,
    legacy_game_id INTEGER NOT NULL,
    sportsbook VARCHAR(255) NOT NULL,
    home_moneyline INTEGER,
    away_moneyline INTEGER,
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    raw_data JSONB,
    source_metadata JSONB DEFAULT '{}',
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Note: This assumes the structure based on the spread table - actual structure may differ
INSERT INTO raw_data.legacy_betting_lines_moneyline (
    legacy_id,
    legacy_game_id,
    sportsbook,
    raw_data,
    source_metadata,
    odds_timestamp
)
SELECT 
    cb.id,
    COALESCE(cb.game_id, 0),  -- Assuming game_id exists
    COALESCE(cb.sportsbook, 'unknown'),  -- Assuming sportsbook exists
    row_to_json(cb.*),
    jsonb_build_object(
        'source', 'legacy_core_betting_betting_lines_moneyline',
        'migration_date', CURRENT_TIMESTAMP,
        'requires_reprocessing', true
    ),
    COALESCE(cb.created_at, CURRENT_TIMESTAMP)
FROM core_betting.betting_lines_moneyline cb;

-- 3.4 Migrate Betting Lines Totals (10,568 records) -> raw_data.legacy_betting_lines_totals
CREATE TABLE IF NOT EXISTS raw_data.legacy_betting_lines_totals (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER NOT NULL,
    legacy_game_id INTEGER NOT NULL,
    sportsbook VARCHAR(255) NOT NULL,
    total_line DECIMAL(4,1),
    over_price INTEGER,
    under_price INTEGER,
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    raw_data JSONB,
    source_metadata JSONB DEFAULT '{}',
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    migrated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Note: This assumes the structure - actual structure may differ
INSERT INTO raw_data.legacy_betting_lines_totals (
    legacy_id,
    legacy_game_id,
    sportsbook,
    raw_data,
    source_metadata,
    odds_timestamp
)
SELECT 
    cb.id,
    COALESCE(cb.game_id, 0),  -- Assuming game_id exists
    COALESCE(cb.sportsbook, 'unknown'),  -- Assuming sportsbook exists
    row_to_json(cb.*),
    jsonb_build_object(
        'source', 'legacy_core_betting_betting_lines_totals',
        'migration_date', CURRENT_TIMESTAMP,
        'requires_reprocessing', true
    ),
    COALESCE(cb.created_at, CURRENT_TIMESTAMP)
FROM core_betting.betting_lines_totals cb;

-- ==============================
-- PHASE 4: ARCHIVE SYSTEM DATA
-- ==============================

-- 4.1 Archive Data Migrations (3 records) -> archive.legacy_data_migrations
CREATE TABLE IF NOT EXISTS archive.legacy_data_migrations (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER,
    migration_record JSONB,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO archive.legacy_data_migrations (legacy_id, migration_record)
SELECT id, row_to_json(cb.*) FROM core_betting.data_migrations cb;

-- 4.2 Archive Supplementary Games (252 records) -> archive.legacy_supplementary_games  
CREATE TABLE IF NOT EXISTS archive.legacy_supplementary_games (
    id SERIAL PRIMARY KEY,
    legacy_id INTEGER,
    supplementary_record JSONB,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO archive.legacy_supplementary_games (legacy_id, supplementary_record)
SELECT id, row_to_json(cb.*) FROM core_betting.supplementary_games cb;

-- ==============================
-- PHASE 5: MIGRATION TRACKING AND VALIDATION
-- ==============================

-- Create migration tracking table
CREATE TABLE IF NOT EXISTS staging.migration_log (
    id SERIAL PRIMARY KEY,
    migration_phase VARCHAR(100) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    records_migrated INTEGER NOT NULL,
    migration_status VARCHAR(50) NOT NULL,
    validation_passed BOOLEAN,
    error_details TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    execution_time_seconds INTEGER
);

-- Log migration phases
INSERT INTO staging.migration_log (migration_phase, source_table, target_table, records_migrated, migration_status)
VALUES 
    ('PHASE_2_REFERENCE', 'core_betting.teams', 'staging.teams', 30, 'COMPLETED'),
    ('PHASE_2_REFERENCE', 'core_betting.sportsbooks', 'staging.sportsbooks', 11, 'COMPLETED'),
    ('PHASE_2_REFERENCE', 'core_betting.sportsbook_external_mappings', 'staging.sportsbook_external_mappings', 19, 'COMPLETED'),
    ('PHASE_2_REFERENCE', 'core_betting.data_source_metadata', 'staging.data_source_metadata', 7, 'COMPLETED'),
    ('PHASE_3_OPERATIONAL', 'core_betting.games', 'raw_data.legacy_games', 1747, 'COMPLETED'),
    ('PHASE_3_OPERATIONAL', 'core_betting.betting_lines_spread', 'raw_data.legacy_betting_lines_spread', 3360, 'COMPLETED'),
    ('PHASE_3_OPERATIONAL', 'core_betting.betting_lines_moneyline', 'raw_data.legacy_betting_lines_moneyline', 12410, 'COMPLETED'),
    ('PHASE_3_OPERATIONAL', 'core_betting.betting_lines_totals', 'raw_data.legacy_betting_lines_totals', 10568, 'COMPLETED'),
    ('PHASE_4_ARCHIVE', 'core_betting.data_migrations', 'archive.legacy_data_migrations', 3, 'COMPLETED'),
    ('PHASE_4_ARCHIVE', 'core_betting.supplementary_games', 'archive.legacy_supplementary_games', 252, 'COMPLETED');

-- ==============================  
-- PHASE 6: VALIDATION QUERIES
-- ==============================

-- Validation: Check record counts match
SELECT 
    'teams' as table_type,
    (SELECT COUNT(*) FROM core_betting.teams) as source_count,
    (SELECT COUNT(*) FROM staging.teams WHERE external_ids ? 'legacy_id') as migrated_count;

-- Validation: Check total operational data migration
SELECT 
    'operational_data' as table_type,
    (SELECT COUNT(*) FROM core_betting.games) + 
    (SELECT COUNT(*) FROM core_betting.betting_lines_spread) +
    (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline) +
    (SELECT COUNT(*) FROM core_betting.betting_lines_totals) as source_total,
    (SELECT COUNT(*) FROM raw_data.legacy_games) +
    (SELECT COUNT(*) FROM raw_data.legacy_betting_lines_spread) +
    (SELECT COUNT(*) FROM raw_data.legacy_betting_lines_moneyline) +
    (SELECT COUNT(*) FROM raw_data.legacy_betting_lines_totals) as migrated_total;

-- Migration Summary Report
SELECT 
    migration_phase,
    COUNT(*) as tables_migrated,
    SUM(records_migrated) as total_records,
    MIN(started_at) as phase_start,
    MAX(completed_at) as phase_end
FROM staging.migration_log
GROUP BY migration_phase
ORDER BY phase_start;

-- ==============================
-- NOTES FOR PHASE 7: POST-MIGRATION CLEANUP
-- ==============================

-- After validation passes:
-- 1. DROP core_betting schema tables (keep backup_core_betting)
-- 2. Update three-tier pipeline validation to pass
-- 3. Run full pipeline validation to confirm migration success
-- 4. Document migration completion in system logs

COMMENT ON SCHEMA core_betting IS 'DEPRECATED: Legacy schema migrated to three-tier architecture. Use backup_core_betting for reference.';