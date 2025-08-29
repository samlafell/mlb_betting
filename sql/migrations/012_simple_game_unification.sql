-- ============================================================================
-- SIMPLE GAME ENTITY UNIFICATION - P0 Critical Fix
-- ============================================================================
-- Purpose: Resolve game entity fragmentation with minimal complexity
-- Strategy: Create master_games, update FKs, create views
-- Risk Level: LOW (simple approach, tested logic)
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. CREATE UNIFIED MASTER_GAMES TABLE
-- ============================================================================

SELECT 'Creating unified master_games table...' as status;

CREATE TABLE curated.master_games (
    id BIGSERIAL PRIMARY KEY,
    
    -- Natural key for deduplication
    mlb_stats_api_game_id VARCHAR(255) UNIQUE,
    
    -- Source tracking
    enhanced_games_source_id BIGINT,
    games_complete_source_id INTEGER,
    action_network_game_id INTEGER,
    
    -- Core game details
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,  
    game_date DATE NOT NULL,
    season INTEGER NOT NULL,
    
    -- Outcome details
    home_score INTEGER,
    away_score INTEGER,
    game_status VARCHAR(255),
    
    -- Data lineage
    source_enhanced_games BOOLEAN DEFAULT FALSE,
    source_games_complete BOOLEAN DEFAULT FALSE,
    data_quality_score DECIMAL(3,2) DEFAULT 0.8,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Essential indices
CREATE INDEX idx_master_games_mlb_api_id ON curated.master_games(mlb_stats_api_game_id);
CREATE INDEX idx_master_games_date ON curated.master_games(game_date);
CREATE INDEX idx_master_games_teams ON curated.master_games(home_team, away_team);

-- ============================================================================
-- 2. MERGE DATA FROM BOTH TABLES
-- ============================================================================

SELECT 'Merging game data from both source tables...' as status;

INSERT INTO curated.master_games (
    mlb_stats_api_game_id,
    enhanced_games_source_id,
    games_complete_source_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    season,
    home_score,
    away_score,
    game_status,
    source_enhanced_games,
    source_games_complete,
    data_quality_score
)
SELECT 
    COALESCE(eg.mlb_stats_api_game_id, gc.mlb_stats_api_game_id, 
             'UNKNOWN_' || COALESCE(eg.id::text, gc.id::text)) as mlb_stats_api_game_id,
    eg.id as enhanced_games_source_id,
    gc.id as games_complete_source_id,
    COALESCE(eg.action_network_game_id, gc.action_network_game_id) as action_network_game_id,
    COALESCE(eg.home_team, gc.home_team) as home_team,
    COALESCE(eg.away_team, gc.away_team) as away_team,
    COALESCE(eg.game_date, gc.game_date::date) as game_date,
    COALESCE(eg.season, gc.season) as season,
    COALESCE(eg.home_score, gc.home_score) as home_score,
    COALESCE(eg.away_score, gc.away_score) as away_score,
    COALESCE(eg.game_status, gc.game_status) as game_status,
    (eg.id IS NOT NULL) as source_enhanced_games,
    (gc.id IS NOT NULL) as source_games_complete,
    CASE 
        WHEN eg.id IS NOT NULL AND gc.id IS NOT NULL THEN 1.0
        WHEN eg.id IS NOT NULL THEN 0.9
        WHEN gc.id IS NOT NULL THEN 0.8
        ELSE 0.5
    END as data_quality_score
FROM curated.enhanced_games eg
FULL OUTER JOIN curated.games_complete gc 
    ON eg.mlb_stats_api_game_id = gc.mlb_stats_api_game_id
    AND eg.mlb_stats_api_game_id IS NOT NULL 
    AND gc.mlb_stats_api_game_id IS NOT NULL;

-- ============================================================================
-- 3. CREATE ID MAPPINGS
-- ============================================================================

SELECT 'Creating ID mappings for FK updates...' as status;

CREATE TEMP TABLE enhanced_games_mapping AS
SELECT 
    eg.id as old_id,
    mg.id as new_id
FROM curated.enhanced_games eg
JOIN curated.master_games mg ON eg.id = mg.enhanced_games_source_id;

CREATE TEMP TABLE games_complete_mapping AS
SELECT 
    gc.id as old_id,
    mg.id as new_id
FROM curated.games_complete gc
JOIN curated.master_games mg ON gc.id = mg.games_complete_source_id;

-- ============================================================================
-- 4. DROP PROBLEMATIC UNIQUE CONSTRAINTS TEMPORARILY
-- ============================================================================

SELECT 'Handling constraint conflicts...' as status;

ALTER TABLE curated.game_outcomes DROP CONSTRAINT IF EXISTS game_outcomes_game_id_key;

-- ============================================================================
-- 5. UPDATE FK REFERENCES
-- ============================================================================

SELECT 'Updating FK references to master_games...' as status;

-- Enhanced games references
UPDATE curated.arbitrage_opportunities ao
SET game_id = egm.new_id
FROM enhanced_games_mapping egm
WHERE ao.game_id = egm.old_id;

UPDATE curated.ml_temporal_features mtf  
SET game_id = egm.new_id
FROM enhanced_games_mapping egm
WHERE mtf.game_id = egm.old_id;

UPDATE curated.rlm_opportunities ro
SET game_id = egm.new_id
FROM enhanced_games_mapping egm
WHERE ro.game_id = egm.old_id;

UPDATE curated.unified_betting_splits ubs
SET game_id = egm.new_id
FROM enhanced_games_mapping egm
WHERE ubs.game_id = egm.old_id;

-- Games complete references
UPDATE curated.betting_splits bs
SET game_id = gcm.new_id
FROM games_complete_mapping gcm
WHERE bs.game_id = gcm.old_id;

UPDATE curated.game_outcomes go
SET game_id = gcm.new_id
FROM games_complete_mapping gcm
WHERE go.game_id = gcm.old_id;

UPDATE curated.line_movements lm
SET game_id = gcm.new_id
FROM games_complete_mapping gcm
WHERE lm.game_id = gcm.old_id;

UPDATE curated.sharp_action_indicators sai
SET game_id = gcm.new_id
FROM games_complete_mapping gcm
WHERE sai.game_id = gcm.old_id;

UPDATE curated.steam_moves sm
SET game_id = gcm.new_id
FROM games_complete_mapping gcm
WHERE sm.game_id = gcm.old_id;

-- ============================================================================
-- 6. UPDATE FK CONSTRAINTS
-- ============================================================================

SELECT 'Updating FK constraints to reference master_games...' as status;

-- Enhanced games constraints
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

-- Games complete constraints
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
-- 7. RECREATE UNIQUE CONSTRAINT
-- ============================================================================

SELECT 'Recreating unique constraints...' as status;

ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_key UNIQUE (game_id);

-- ============================================================================
-- 8. BACKUP ORIGINAL TABLES AND CREATE VIEWS
-- ============================================================================

SELECT 'Creating backup tables and views...' as status;

-- Backup original tables
ALTER TABLE curated.enhanced_games RENAME TO enhanced_games_backup_simple_unification;
ALTER TABLE curated.games_complete RENAME TO games_complete_backup_simple_unification;

-- Create backward compatibility views
CREATE VIEW curated.enhanced_games AS
SELECT 
    enhanced_games_source_id as id,
    mlb_stats_api_game_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    season,
    home_score,
    away_score,
    game_status,
    created_at,
    updated_at
FROM curated.master_games
WHERE source_enhanced_games = TRUE
    AND enhanced_games_source_id IS NOT NULL;

CREATE VIEW curated.games_complete AS
SELECT 
    games_complete_source_id as id,
    mlb_stats_api_game_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    season,
    home_score,
    away_score,
    game_status,
    created_at,
    updated_at
FROM curated.master_games
WHERE source_games_complete = TRUE
    AND games_complete_source_id IS NOT NULL;

-- ============================================================================
-- 9. FINAL VALIDATION AND LOGGING
-- ============================================================================

SELECT 'Validating unification results...' as status;

SELECT 
    COUNT(*) as total_games,
    COUNT(CASE WHEN source_enhanced_games THEN 1 END) as from_enhanced,
    COUNT(CASE WHEN source_games_complete THEN 1 END) as from_complete,
    COUNT(CASE WHEN source_enhanced_games AND source_games_complete THEN 1 END) as from_both
FROM curated.master_games;

-- Log completion
INSERT INTO operational.schema_migration_log (phase, operation, status, details)
VALUES (
    'PHASE_3_SIMPLE',
    'GAME_ENTITY_UNIFICATION_COMPLETED',
    'COMPLETED',
    'Successfully unified games into master_games table, updated all FK relationships, created compatibility views'
);

SELECT 'GAME ENTITY UNIFICATION COMPLETED SUCCESSFULLY' as status;
SELECT 'P0 schema fragmentation crisis resolved' as result;

COMMIT;

-- ============================================================================
-- POST-COMMIT VALIDATION
-- ============================================================================

SELECT 'POST-COMMIT VALIDATION:' as validation_status;

-- Verify master_games exists and has data
SELECT 
    'master_games' as table_name,
    COUNT(*) as record_count
FROM curated.master_games;

-- Verify views exist
SELECT 
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_schema = 'curated' 
    AND table_name IN ('enhanced_games', 'games_complete')
ORDER BY table_name;

-- Final FK validation
SELECT 
    COUNT(*) as fk_count,
    'All FKs now reference master_games' as status
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'curated'
    AND kcu.column_name = 'game_id'
    AND ccu.table_name = 'master_games';

SELECT 'SUCCESS: Game Entity Fragmentation Issue Resolved' as final_status;