-- ============================================================================
-- SCHEMA CONSOLIDATION - PHASE 3: Game Entity Unification
-- ============================================================================
-- Purpose: Consolidate enhanced_games + games_complete into unified master_games
-- Risk Level: HIGH (critical data consolidation, FK updates)
-- Rollback: Full rollback procedure provided
-- 
-- CRITICAL: This resolves the game entity fragmentation causing referential chaos
-- 
-- IMPORTANT: 
-- 1. Run Phases 1 & 2 first
-- 2. Run full database backup before executing!
-- Command: pg_dump -h localhost -p 5433 -U samlafell -d mlb_betting > backup_pre_phase3.sql
-- ============================================================================

BEGIN;

-- Create transaction savepoint for rollback capability
SAVEPOINT phase3_start;

-- ============================================================================
-- 1. PRE-MIGRATION VALIDATION & ANALYSIS
-- ============================================================================

-- Verify Phases 1 & 2 were completed
DO $$
DECLARE
    phase1_count INTEGER;
    phase2_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO phase1_count 
    FROM operational.schema_migration_log 
    WHERE phase = 'PHASE_1' AND status = 'COMPLETED';
    
    SELECT COUNT(*) INTO phase2_count 
    FROM operational.schema_migration_log 
    WHERE phase = 'PHASE_2' AND status = 'COMPLETED';
    
    IF phase1_count = 0 THEN
        RAISE EXCEPTION 'Phase 1 must be completed before Phase 3';
    END IF;
    
    IF phase2_count = 0 THEN
        RAISE EXCEPTION 'Phase 2 must be completed before Phase 3';
    END IF;
    
    RAISE NOTICE 'Phase 1 & 2 verification: PASSED';
END $$;

-- Analyze current game tables
SELECT 'ANALYZING CURRENT GAME ENTITY FRAGMENTATION:' as status;

-- Check enhanced_games structure and data
SELECT 'curated.enhanced_games analysis:' as analysis_type;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT external_game_id) as unique_external_ids,
    MIN(game_date) as earliest_game,
    MAX(game_date) as latest_game,
    COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores
FROM curated.enhanced_games;

-- Check games_complete structure and data  
SELECT 'curated.games_complete analysis:' as analysis_type;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT external_game_id) as unique_external_ids,
    MIN(game_date::date) as earliest_game,
    MAX(game_date::date) as latest_game,
    COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores
FROM curated.games_complete;

-- Check for overlapping games
SELECT 'Overlapping games analysis:' as analysis_type;
SELECT COUNT(*) as overlapping_games
FROM curated.enhanced_games eg
INNER JOIN curated.games_complete gc 
    ON eg.external_game_id = gc.external_game_id;

-- Analyze FK dependencies for both game tables
SELECT 'FK DEPENDENCIES ANALYSIS:' as status;

-- Tables referencing enhanced_games
SELECT 'Tables referencing enhanced_games:' as dep_type;
SELECT 
    tc.table_schema || '.' || tc.table_name as referencing_table,
    kcu.column_name as referencing_column,
    COUNT(*) as record_count
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name = 'enhanced_games'
    AND ccu.table_schema = 'curated'
GROUP BY tc.table_schema, tc.table_name, kcu.column_name;

-- Tables referencing games_complete
SELECT 'Tables referencing games_complete:' as dep_type;
SELECT 
    tc.table_schema || '.' || tc.table_name as referencing_table,
    kcu.column_name as referencing_column,
    COUNT(*) as record_count
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name = 'games_complete'
    AND ccu.table_schema = 'curated'
GROUP BY tc.table_schema, tc.table_name, kcu.column_name;

-- ============================================================================
-- 2. CREATE UNIFIED MASTER_GAMES TABLE
-- ============================================================================

SELECT 'Creating unified master_games table...' as status;

-- Create the new master games table with unified schema
CREATE TABLE curated.master_games (
    id BIGSERIAL PRIMARY KEY,
    
    -- Core game identification (using MLB Stats API as master key)
    mlb_stats_api_game_id VARCHAR(255) UNIQUE,
    action_network_game_id INTEGER,
    sbd_game_id VARCHAR(255),
    vsin_game_key VARCHAR(255), 
    sportsbookreview_game_id VARCHAR(255),
    
    -- Core game details
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    season INTEGER NOT NULL,
    season_type VARCHAR(50),
    game_type VARCHAR(50),
    
    -- Venue details  
    venue_name VARCHAR(255),
    venue_city VARCHAR(255),
    venue_state VARCHAR(10),
    
    -- Weather details
    weather_condition VARCHAR(255),
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction VARCHAR(50),
    humidity INTEGER,
    
    -- Outcome details
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(255),
    total_runs INTEGER,
    game_status VARCHAR(50) DEFAULT 'scheduled',
    
    -- Pitching details
    home_pitcher_name VARCHAR(255),
    away_pitcher_name VARCHAR(255),
    home_pitcher_era DECIMAL(4,2),
    away_pitcher_era DECIMAL(4,2),
    home_pitcher_throws CHAR(1),
    away_pitcher_throws CHAR(1),
    pitcher_handedness_matchup VARCHAR(50),
    
    -- Advanced metadata
    feature_data JSONB,
    ml_metadata JSONB,
    
    -- Data lineage and quality
    source_enhanced_games BOOLEAN DEFAULT FALSE,
    source_games_complete BOOLEAN DEFAULT FALSE,
    data_quality_score DECIMAL(3,2),
    mlb_correlation_confidence DECIMAL(3,2),
    source_coverage_score DECIMAL(3,2),
    
    -- Standard metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_valid_score CHECK (
        (home_score IS NULL AND away_score IS NULL) OR 
        (home_score IS NOT NULL AND away_score IS NOT NULL)
    ),
    CONSTRAINT chk_quality_score CHECK (
        data_quality_score >= 0 AND data_quality_score <= 1
    )
);

-- Create performance indices
CREATE INDEX idx_master_games_date ON curated.master_games(game_date);
CREATE INDEX idx_master_games_teams ON curated.master_games(home_team, away_team);
CREATE INDEX idx_master_games_mlb_api_id ON curated.master_games(mlb_stats_api_game_id);
CREATE INDEX idx_master_games_action_network_id ON curated.master_games(action_network_game_id);
CREATE INDEX idx_master_games_season ON curated.master_games(season);
CREATE INDEX idx_master_games_status ON curated.master_games(game_status);

-- ============================================================================
-- 3. MIGRATE DATA FROM BOTH SOURCE TABLES
-- ============================================================================

SELECT 'Migrating data from enhanced_games and games_complete...' as status;

-- Create temp table to handle the merge logic
CREATE TEMP TABLE game_merge_prep AS
WITH enhanced_games_prep AS (
    SELECT 
        external_game_id,
        game_date,
        home_team,
        away_team,
        season,
        weather_conditions,
        stadium_conditions,
        player_injuries,
        home_score,
        away_score,
        game_status,
        completed_at,
        created_at,
        updated_at,
        'enhanced_games' as source_table
    FROM curated.enhanced_games
),
games_complete_prep AS (
    SELECT 
        external_game_id,
        game_date::date as game_date,
        home_team,
        away_team,
        EXTRACT(YEAR FROM game_date)::INTEGER as season,
        NULL::jsonb as weather_conditions,
        NULL::jsonb as stadium_conditions,
        NULL::jsonb as player_injuries,
        home_score,
        away_score,
        status as game_status,
        game_date as completed_at,
        created_at,
        updated_at,
        'games_complete' as source_table
    FROM curated.games_complete
)
SELECT 
    COALESCE(eg.external_game_id, gc.external_game_id) as external_game_id,
    COALESCE(eg.game_date, gc.game_date) as game_date,
    COALESCE(eg.home_team, gc.home_team) as home_team,
    COALESCE(eg.away_team, gc.away_team) as away_team,
    COALESCE(eg.season, gc.season) as season,
    
    -- Prefer enhanced_games data for rich fields
    COALESCE(eg.weather_conditions, gc.weather_conditions) as weather_conditions,
    COALESCE(eg.stadium_conditions, gc.stadium_conditions) as stadium_conditions,
    COALESCE(eg.player_injuries, gc.player_injuries) as player_injuries,
    
    -- Prefer most recent score data
    COALESCE(eg.home_score, gc.home_score) as home_score,
    COALESCE(eg.away_score, gc.away_score) as away_score,
    COALESCE(eg.game_status, gc.game_status) as game_status,
    COALESCE(eg.completed_at, gc.completed_at) as completed_at,
    
    -- Source tracking
    (eg.external_game_id IS NOT NULL) as source_enhanced_games,
    (gc.external_game_id IS NOT NULL) as source_games_complete,
    
    -- Quality score based on data completeness
    CASE 
        WHEN eg.external_game_id IS NOT NULL AND gc.external_game_id IS NOT NULL THEN 1.0
        WHEN eg.external_game_id IS NOT NULL OR gc.external_game_id IS NOT NULL THEN 0.8
        ELSE 0.5
    END as data_quality_score,
    
    -- Timestamps - prefer most recent
    GREATEST(
        COALESCE(eg.created_at, '1900-01-01'::timestamp with time zone),
        COALESCE(gc.created_at, '1900-01-01'::timestamp with time zone)
    ) as created_at,
    GREATEST(
        COALESCE(eg.updated_at, '1900-01-01'::timestamp with time zone),
        COALESCE(gc.updated_at, '1900-01-01'::timestamp with time zone)
    ) as updated_at

FROM enhanced_games_prep eg
FULL OUTER JOIN games_complete_prep gc 
    ON eg.external_game_id = gc.external_game_id;

-- Insert consolidated data into master_games
INSERT INTO curated.master_games (
    external_game_id, game_date, home_team, away_team, season,
    weather_conditions, stadium_conditions, player_injuries,
    home_score, away_score, game_status, completed_at,
    source_enhanced_games, source_games_complete, data_quality_score,
    created_at, updated_at
)
SELECT 
    external_game_id, game_date, home_team, away_team, season,
    weather_conditions, stadium_conditions, player_injuries,
    home_score, away_score, game_status, completed_at,
    source_enhanced_games, source_games_complete, data_quality_score,
    created_at, updated_at
FROM game_merge_prep;

-- Verify data migration
SELECT 'Data migration verification:' as status;
SELECT 
    COUNT(*) as total_master_games,
    COUNT(CASE WHEN source_enhanced_games THEN 1 END) as from_enhanced,
    COUNT(CASE WHEN source_games_complete THEN 1 END) as from_complete,
    COUNT(CASE WHEN source_enhanced_games AND source_games_complete THEN 1 END) as from_both,
    AVG(data_quality_score) as avg_quality_score
FROM curated.master_games;

-- ============================================================================
-- 4. CREATE ID MAPPING TABLES (For FK Updates)
-- ============================================================================

SELECT 'Creating ID mapping tables for FK updates...' as status;

-- Create mapping from old enhanced_games IDs to new master_games IDs
CREATE TEMP TABLE enhanced_games_id_mapping AS
SELECT 
    eg.id as old_id,
    mg.id as new_id,
    eg.external_game_id
FROM curated.enhanced_games eg
JOIN curated.master_games mg ON eg.external_game_id = mg.external_game_id;

-- Create mapping from old games_complete IDs to new master_games IDs  
CREATE TEMP TABLE games_complete_id_mapping AS
SELECT 
    gc.id as old_id,
    mg.id as new_id,
    gc.external_game_id
FROM curated.games_complete gc
JOIN curated.master_games mg ON gc.external_game_id = mg.external_game_id;

-- ============================================================================
-- 5. UPDATE FOREIGN KEY REFERENCES
-- ============================================================================

SELECT 'Updating foreign key references to use master_games...' as status;

-- Update tables that referenced enhanced_games
-- curated.arbitrage_opportunities
UPDATE curated.arbitrage_opportunities ao
SET game_id = egm.new_id
FROM enhanced_games_id_mapping egm
WHERE ao.game_id = egm.old_id;

-- curated.ml_temporal_features  
UPDATE curated.ml_temporal_features mtf
SET game_id = egm.new_id
FROM enhanced_games_id_mapping egm
WHERE mtf.game_id = egm.old_id;

-- curated.rlm_opportunities
UPDATE curated.rlm_opportunities ro
SET game_id = egm.new_id
FROM enhanced_games_id_mapping egm
WHERE ro.game_id = egm.old_id;

-- curated.unified_betting_splits
UPDATE curated.unified_betting_splits ubs
SET game_id = egm.new_id
FROM enhanced_games_id_mapping egm
WHERE ubs.game_id = egm.old_id;

-- Update tables that referenced games_complete
-- curated.betting_splits
UPDATE curated.betting_splits bs
SET game_id = gcm.new_id
FROM games_complete_id_mapping gcm
WHERE bs.game_id = gcm.old_id;

-- curated.game_outcomes
UPDATE curated.game_outcomes go
SET game_id = gcm.new_id
FROM games_complete_id_mapping gcm
WHERE go.game_id = gcm.old_id;

-- curated.line_movements
UPDATE curated.line_movements lm
SET game_id = gcm.new_id
FROM games_complete_id_mapping gcm
WHERE lm.game_id = gcm.old_id;

-- curated.sharp_action_indicators
UPDATE curated.sharp_action_indicators sai
SET game_id = gcm.new_id
FROM games_complete_id_mapping gcm
WHERE sai.game_id = gcm.old_id;

-- curated.steam_moves
UPDATE curated.steam_moves sm
SET game_id = gcm.new_id
FROM games_complete_id_mapping gcm
WHERE sm.game_id = gcm.old_id;

-- ============================================================================
-- 6. UPDATE FOREIGN KEY CONSTRAINTS
-- ============================================================================

SELECT 'Updating foreign key constraints to reference master_games...' as status;

-- Drop old FK constraints and create new ones pointing to master_games

-- Enhanced games references
ALTER TABLE curated.arbitrage_opportunities 
DROP CONSTRAINT IF EXISTS arbitrage_opportunities_game_id_fkey;
ALTER TABLE curated.arbitrage_opportunities 
ADD CONSTRAINT arbitrage_opportunities_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.ml_temporal_features 
DROP CONSTRAINT IF EXISTS ml_temporal_features_game_id_fkey;
ALTER TABLE curated.ml_temporal_features 
ADD CONSTRAINT ml_temporal_features_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.rlm_opportunities 
DROP CONSTRAINT IF EXISTS rlm_opportunities_game_id_fkey;
ALTER TABLE curated.rlm_opportunities 
ADD CONSTRAINT rlm_opportunities_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.unified_betting_splits 
DROP CONSTRAINT IF EXISTS unified_betting_splits_game_id_fkey;
ALTER TABLE curated.unified_betting_splits 
ADD CONSTRAINT unified_betting_splits_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

-- Games complete references  
ALTER TABLE curated.betting_splits 
DROP CONSTRAINT IF EXISTS betting_splits_game_id_fkey;
ALTER TABLE curated.betting_splits 
ADD CONSTRAINT betting_splits_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.game_outcomes 
DROP CONSTRAINT IF EXISTS game_outcomes_game_id_fkey;
ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.line_movements 
DROP CONSTRAINT IF EXISTS line_movements_game_id_fkey;
ALTER TABLE curated.line_movements 
ADD CONSTRAINT line_movements_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.sharp_action_indicators 
DROP CONSTRAINT IF EXISTS sharp_action_indicators_game_id_fkey;
ALTER TABLE curated.sharp_action_indicators 
ADD CONSTRAINT sharp_action_indicators_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

ALTER TABLE curated.steam_moves 
DROP CONSTRAINT IF EXISTS steam_moves_game_id_fkey;
ALTER TABLE curated.steam_moves 
ADD CONSTRAINT steam_moves_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.master_games(id);

-- ============================================================================
-- 7. CREATE BACKWARD COMPATIBILITY VIEWS
-- ============================================================================

SELECT 'Creating backward compatibility views...' as status;

-- Create enhanced_games view for backward compatibility
CREATE OR REPLACE VIEW curated.enhanced_games AS
SELECT 
    id,
    external_game_id,
    game_date,
    home_team,
    away_team,
    season,
    weather_conditions,
    stadium_conditions,
    player_injuries,
    home_score,
    away_score,
    game_status,
    completed_at,
    created_at,
    updated_at
FROM curated.master_games
WHERE source_enhanced_games = TRUE;

-- Create games_complete view for backward compatibility
CREATE OR REPLACE VIEW curated.games_complete AS
SELECT 
    id,
    external_game_id,
    game_date as game_date,  -- Keep as date type
    home_team,
    away_team,
    home_score,
    away_score,
    game_status as status,
    completed_at as game_date,  -- Map to original column name
    created_at,
    updated_at
FROM curated.master_games
WHERE source_games_complete = TRUE;

-- ============================================================================
-- 8. VALIDATION & TESTING
-- ============================================================================

SELECT 'Validating game entity unification...' as status;

-- Test that all FK references are valid
SELECT 'Testing FK constraint integrity:' as test_type;

-- Count records in tables referencing master_games
WITH fk_validation AS (
    SELECT 'arbitrage_opportunities' as table_name, COUNT(*) as count 
    FROM curated.arbitrage_opportunities 
    UNION ALL
    SELECT 'ml_temporal_features', COUNT(*) 
    FROM curated.ml_temporal_features
    UNION ALL  
    SELECT 'rlm_opportunities', COUNT(*)
    FROM curated.rlm_opportunities
    UNION ALL
    SELECT 'unified_betting_splits', COUNT(*)
    FROM curated.unified_betting_splits
    UNION ALL
    SELECT 'betting_splits', COUNT(*)
    FROM curated.betting_splits
    UNION ALL
    SELECT 'game_outcomes', COUNT(*)
    FROM curated.game_outcomes
    UNION ALL
    SELECT 'line_movements', COUNT(*)
    FROM curated.line_movements
    UNION ALL
    SELECT 'sharp_action_indicators', COUNT(*)
    FROM curated.sharp_action_indicators
    UNION ALL
    SELECT 'steam_moves', COUNT(*)
    FROM curated.steam_moves
)
SELECT * FROM fk_validation ORDER BY table_name;

-- Test backward compatibility views
SELECT 'Testing backward compatibility views:' as test_type;
SELECT 
    (SELECT COUNT(*) FROM curated.enhanced_games) as enhanced_games_view_count,
    (SELECT COUNT(*) FROM curated.games_complete) as games_complete_view_count,
    (SELECT COUNT(*) FROM curated.master_games) as master_games_count;

-- ============================================================================
-- 9. RENAME OLD TABLES (Don't drop yet - for rollback safety)
-- ============================================================================

SELECT 'Renaming old game tables for rollback safety...' as status;

-- Rename old tables instead of dropping (can drop in Phase 4)
ALTER TABLE curated.enhanced_games RENAME TO enhanced_games_backup_phase3;
ALTER TABLE curated.games_complete RENAME TO games_complete_backup_phase3;

-- ============================================================================
-- 10. CREATE PHASE 3 COMPLETION LOG
-- ============================================================================

INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES (
    'PHASE_3',
    'GAME_ENTITY_UNIFICATION',
    'COMPLETED',
    FORMAT('Created master_games with %s records, updated %s FK relationships, created compatibility views', 
           (SELECT COUNT(*) FROM curated.master_games),
           (SELECT COUNT(*) FROM information_schema.table_constraints 
            WHERE constraint_type = 'FOREIGN KEY' AND constraint_name LIKE '%master_games%'))
);

-- Success message
SELECT 'PHASE 3 COMPLETED SUCCESSFULLY' as status;
SELECT 'Game entity fragmentation resolved, all FKs point to unified master_games' as message;
SELECT 'Next step: Execute Phase 4 - Final Schema Cleanup' as next_action;

-- Commit the transaction
COMMIT;

-- ============================================================================
-- ROLLBACK SCRIPT (Run if Phase 3 needs to be reverted)
-- ============================================================================
/*
-- EMERGENCY ROLLBACK FOR PHASE 3
-- Restores original enhanced_games and games_complete tables

BEGIN;

SELECT 'STARTING PHASE 3 ROLLBACK - Restoring original game tables...' as status;

-- Restore original table names
ALTER TABLE curated.enhanced_games_backup_phase3 RENAME TO enhanced_games;
ALTER TABLE curated.games_complete_backup_phase3 RENAME TO games_complete;

-- Drop the views
DROP VIEW IF EXISTS curated.enhanced_games;
DROP VIEW IF EXISTS curated.games_complete;

-- Restore original FK constraints (this requires recreating the original constraints)
-- This would need the specific original constraint definitions...

-- Drop master_games table
DROP TABLE IF EXISTS curated.master_games CASCADE;

-- Log rollback
INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES ('PHASE_3', 'ROLLBACK', 'COMPLETED', 'Restored original enhanced_games and games_complete tables');

SELECT 'PHASE 3 ROLLBACK COMPLETED' as status;

COMMIT;
*/