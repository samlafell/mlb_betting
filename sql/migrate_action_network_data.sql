-- ==================================================================================
-- MLB Sharp Betting System - Action Network Data Migration
-- ==================================================================================
-- 
-- This script migrates Action Network team and game data to the consolidated schema:
-- - Enhance core_betting.teams with Action Network data
-- - Migrate action.fact_games to core_betting.action_network_games
-- - Migrate action.games_with_teams to core_betting.action_network_games_enhanced
--
-- NON-DESTRUCTIVE: Preserves source tables for validation
-- ==================================================================================

-- Enable detailed logging
\set ON_ERROR_STOP on

-- Create migration tracking
CREATE TABLE IF NOT EXISTS operational.action_network_migration_log (
    id SERIAL PRIMARY KEY,
    migration_step VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    records_migrated INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started',
    error_message TEXT,
    notes TEXT
);

-- Migration logging function
CREATE OR REPLACE FUNCTION log_action_migration(
    p_step VARCHAR(100),
    p_source VARCHAR(100),
    p_target VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.action_network_migration_log (
        migration_step, source_table, target_table, 
        records_migrated, completed_at, status, error_message, notes
    ) VALUES (
        p_step, p_source, p_target, 
        p_records, NOW(), p_status, p_error, p_notes
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Action Network Data Migration';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- ENHANCE CORE_BETTING.TEAMS WITH ACTION NETWORK DATA
-- ==================================================================================

-- Add Action Network columns to teams table if they don't exist
DO $$
BEGIN
    -- Add Action Network specific columns
    ALTER TABLE core_betting.teams 
    ADD COLUMN IF NOT EXISTS action_network_id INTEGER,
    ADD COLUMN IF NOT EXISTS action_network_data JSONB;
    
    RAISE NOTICE 'Added Action Network columns to core_betting.teams';
    PERFORM log_action_migration('teams_schema_enhancement', 'action.dim_teams', 'core_betting.teams', 0, 'completed', NULL, 'Added Action Network columns');
END $$;

-- Link existing teams with Action Network IDs (teams are already migrated)
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    -- Update existing teams to set action_network_id where it matches
    UPDATE core_betting.teams 
    SET 
        action_network_id = action.dim_teams.team_id,
        action_network_data = jsonb_build_object(
            'source', 'action_network',
            'migrated_from', 'action.dim_teams'
        ),
        updated_at = NOW()
    FROM action.dim_teams
    WHERE core_betting.teams.team_id = action.dim_teams.team_id
    AND core_betting.teams.action_network_id IS NULL; -- Only update if not already set
    
    GET DIAGNOSTICS record_count = ROW_COUNT;
    PERFORM log_action_migration('teams_linking', 'action.dim_teams', 'core_betting.teams', record_count, 'completed', NULL, 'Linked existing teams with Action Network IDs');
    RAISE NOTICE 'Linked % teams with Action Network IDs', record_count;
END $$;

-- Check for any teams that might be missing (should be none since teams are already migrated)
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count
    FROM action.dim_teams
    WHERE NOT EXISTS (
        SELECT 1 FROM core_betting.teams 
        WHERE core_betting.teams.team_id = action.dim_teams.team_id
    );
    
    IF record_count > 0 THEN
        RAISE NOTICE 'Found % teams in action.dim_teams that are not in core_betting.teams', record_count;
        PERFORM log_action_migration('teams_missing_check', 'action.dim_teams', 'core_betting.teams', record_count, 'warning', NULL, 'Found teams not migrated yet');
    ELSE
        RAISE NOTICE 'All Action Network teams are already present in core_betting.teams';
        PERFORM log_action_migration('teams_complete_check', 'action.dim_teams', 'core_betting.teams', 0, 'completed', NULL, 'All teams already migrated');
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE ACTION NETWORK GAMES DATA
-- ==================================================================================

-- Migrate action.fact_games to core_betting.action_network_games
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM action.fact_games;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.action_network_games (
            id_action, id_mlbstatsapi, dim_home_team_actionid, dim_away_team_actionid,
            game_date, game_time, game_datetime, status, created_at, updated_at
        )
        SELECT 
            id_action,
            id_mlbstatsapi,
            dim_home_team_actionid,
            dim_away_team_actionid,
            dim_date as game_date,
            dim_time as game_time,
            dim_datetime as game_datetime,
            COALESCE(game_status, 'active') as status,
            NOW() as created_at,
            NOW() as updated_at
        FROM action.fact_games
        ON CONFLICT (id_action) DO UPDATE SET
            id_mlbstatsapi = EXCLUDED.id_mlbstatsapi,
            dim_home_team_actionid = EXCLUDED.dim_home_team_actionid,
            dim_away_team_actionid = EXCLUDED.dim_away_team_actionid,
            game_date = EXCLUDED.game_date,
            game_time = EXCLUDED.game_time,
            game_datetime = EXCLUDED.game_datetime,
            status = EXCLUDED.status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_action_migration('fact_games_migration', 'action.fact_games', 'core_betting.action_network_games', record_count);
        RAISE NOTICE 'Migrated % records from action.fact_games to core_betting.action_network_games', record_count;
    ELSE
        PERFORM log_action_migration('fact_games_migration', 'action.fact_games', 'core_betting.action_network_games', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table action.fact_games is empty, skipping migration';
    END IF;
END $$;

-- Migrate action.games_with_teams to core_betting.action_network_games_enhanced
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM action.games_with_teams;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.action_network_games_enhanced (
            id_action, id_mlbstatsapi, game_date, game_time, game_datetime,
            home_team_name, away_team_name, home_team_short, away_team_short,
            venue_name, created_at, updated_at
        )
        SELECT 
            id_action,
            id_mlbstatsapi,
            dim_date as game_date,
            dim_time as game_time,
            dim_datetime as game_datetime,
            home_team_name,
            away_team_name,
            home_team_abbr as home_team_short,
            away_team_abbr as away_team_short,
            venue_name,
            NOW() as created_at,
            NOW() as updated_at
        FROM action.games_with_teams;
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_action_migration('games_with_teams_migration', 'action.games_with_teams', 'core_betting.action_network_games_enhanced', record_count);
        RAISE NOTICE 'Migrated % records from action.games_with_teams to core_betting.action_network_games_enhanced', record_count;
    ELSE
        PERFORM log_action_migration('games_with_teams_migration', 'action.games_with_teams', 'core_betting.action_network_games_enhanced', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table action.games_with_teams is empty, skipping migration';
    END IF;
END $$;

-- ==================================================================================
-- MIGRATION VALIDATION
-- ==================================================================================

DO $$
DECLARE
    teams_with_action_data INTEGER;
    action_games_count INTEGER;
    enhanced_games_count INTEGER;
BEGIN
    -- Count teams with Action Network data
    SELECT COUNT(*) INTO teams_with_action_data 
    FROM core_betting.teams 
    WHERE action_network_id IS NOT NULL;
    
    -- Count migrated games
    SELECT COUNT(*) INTO action_games_count 
    FROM core_betting.action_network_games;
    
    SELECT COUNT(*) INTO enhanced_games_count 
    FROM core_betting.action_network_games_enhanced;
    
    RAISE NOTICE 'Migration Validation Results:';
    RAISE NOTICE 'Teams with Action Network data: %', teams_with_action_data;
    RAISE NOTICE 'Action Network games migrated: %', action_games_count;
    RAISE NOTICE 'Enhanced Action Network games migrated: %', enhanced_games_count;
    
    PERFORM log_action_migration('validation_summary', 'multiple', 'multiple', 
        teams_with_action_data + action_games_count + enhanced_games_count, 
        'completed', NULL, 
        format('Teams: %s, Games: %s, Enhanced: %s', teams_with_action_data, action_games_count, enhanced_games_count));
END $$;

-- ==================================================================================
-- COMPLETION SUMMARY
-- ==================================================================================

DO $$ 
DECLARE
    total_migrated INTEGER;
    migration_summary TEXT;
BEGIN
    -- Calculate total migrated records
    SELECT SUM(records_migrated) INTO total_migrated 
    FROM operational.action_network_migration_log 
    WHERE status = 'completed';
    
    -- Generate summary
    SELECT string_agg(
        migration_step || ': ' || records_migrated || ' records', 
        E'\n'
    ) INTO migration_summary
    FROM operational.action_network_migration_log 
    WHERE status = 'completed'
    ORDER BY id;
    
    RAISE NOTICE 'Action Network Data Migration completed successfully!';
    RAISE NOTICE 'Total records processed: %', COALESCE(total_migrated, 0);
    RAISE NOTICE 'Migration summary:';
    RAISE NOTICE '%', migration_summary;
    RAISE NOTICE 'Completion timestamp: %', NOW();
END $$; 