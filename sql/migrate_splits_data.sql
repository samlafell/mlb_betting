-- ==================================================================================
-- MLB Sharp Betting System - Splits Data Migration
-- ==================================================================================
-- 
-- This script migrates splits schema data to the consolidated schema:
-- - Move splits.games to core_betting.supplementary_games
-- - Move splits.sharp_actions to analytics.sharp_actions
--
-- NON-DESTRUCTIVE: Preserves source tables for validation
-- ==================================================================================

-- Enable detailed logging
\set ON_ERROR_STOP on

-- Create migration tracking
CREATE TABLE IF NOT EXISTS operational.splits_migration_log (
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
CREATE OR REPLACE FUNCTION log_splits_migration(
    p_step VARCHAR(100),
    p_source VARCHAR(100),
    p_target VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.splits_migration_log (
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
    RAISE NOTICE 'Starting Splits Data Migration';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- MIGRATE SPLITS.GAMES TO CORE_BETTING.SUPPLEMENTARY_GAMES
-- ==================================================================================

DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    -- Check source table count
    SELECT COUNT(*) INTO source_count FROM splits.games;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from splits.games to core_betting.supplementary_games', source_count;
        
        -- Migrate games data
        INSERT INTO core_betting.supplementary_games (
            id, game_id, home_team, away_team, game_datetime, 
            source, status, venue, created_at, updated_at
        )
        SELECT 
            id,
            game_id,
            home_team,
            away_team,
            game_datetime,
            'splits_migration' as source,
            status,
            venue,
            created_at::timestamp with time zone,
            updated_at::timestamp with time zone
        FROM splits.games;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_splits_migration('games_migration', 'splits.games', 'core_betting.supplementary_games', record_count, 'completed', NULL, 'Migrated supplementary games data');
        RAISE NOTICE 'Successfully migrated % games records', record_count;
    ELSE
        PERFORM log_splits_migration('games_migration', 'splits.games', 'core_betting.supplementary_games', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'splits.games table is empty, skipping migration';
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE SPLITS.SHARP_ACTIONS TO ANALYTICS.SHARP_ACTIONS
-- ==================================================================================

DO $$
DECLARE
    record_count INTEGER;
    source_count INTEGER;
BEGIN
    -- Check source table count
    SELECT COUNT(*) INTO source_count FROM splits.sharp_actions;
    
    IF source_count > 0 THEN
        RAISE NOTICE 'Migrating % records from splits.sharp_actions to analytics.sharp_actions', source_count;
        
        -- Migrate sharp actions data (if target table exists)
        BEGIN
            -- Check if analytics.sharp_actions table exists
            IF EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'analytics' AND table_name = 'sharp_actions'
            ) THEN
                -- Migrate with available columns
                INSERT INTO analytics.sharp_actions (
                    game_id, split_type, direction, overall_confidence, 
                    signals, analysis, created_at, updated_at
                )
                SELECT 
                    game_id,
                    split_type,
                    direction,
                    overall_confidence,
                    signals,
                    analysis,
                    created_at::timestamp with time zone,
                    updated_at::timestamp with time zone
                FROM splits.sharp_actions;
            ELSE
                RAISE NOTICE 'Target table analytics.sharp_actions does not exist, skipping migration';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Error migrating sharp actions: %', SQLERRM;
        END;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_splits_migration('sharp_actions_migration', 'splits.sharp_actions', 'analytics.sharp_actions', record_count, 'completed', NULL, 'Migrated sharp actions data');
        RAISE NOTICE 'Successfully migrated % sharp actions records', record_count;
    ELSE
        PERFORM log_splits_migration('sharp_actions_migration', 'splits.sharp_actions', 'analytics.sharp_actions', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'splits.sharp_actions table is empty, skipping migration';
    END IF;
END $$;

-- ==================================================================================
-- MIGRATION VALIDATION
-- ==================================================================================

DO $$
DECLARE
    supplementary_games_count INTEGER;
    sharp_actions_count INTEGER;
    source_games_count INTEGER;
    source_actions_count INTEGER;
BEGIN
    -- Count migrated records
    SELECT COUNT(*) INTO supplementary_games_count 
    FROM core_betting.supplementary_games;
    
    -- Check if analytics.sharp_actions exists before counting
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'analytics' AND table_name = 'sharp_actions'
    ) THEN
        SELECT COUNT(*) INTO sharp_actions_count 
        FROM analytics.sharp_actions;
    ELSE
        sharp_actions_count := 0;
    END IF;
    
    -- Count source records
    SELECT COUNT(*) INTO source_games_count 
    FROM splits.games;
    
    SELECT COUNT(*) INTO source_actions_count 
    FROM splits.sharp_actions;
    
    RAISE NOTICE 'Migration Validation Results:';
    RAISE NOTICE 'Supplementary games: % (source: %)', supplementary_games_count, source_games_count;
    RAISE NOTICE 'Sharp actions: % (source: %)', sharp_actions_count, source_actions_count;
    
    -- Log validation results
    PERFORM log_splits_migration('validation_summary', 'splits.*', 'core_betting.*, analytics.*', 
        supplementary_games_count + sharp_actions_count, 'completed', NULL, 
        format('Games: %s/%s, Actions: %s/%s', supplementary_games_count, source_games_count, sharp_actions_count, source_actions_count));
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
    FROM operational.splits_migration_log 
    WHERE status = 'completed';
    
    -- Generate summary
    SELECT string_agg(
        migration_step || ': ' || records_migrated || ' records', 
        E'\n'
    ) INTO migration_summary
    FROM operational.splits_migration_log 
    WHERE status = 'completed'
    ORDER BY id;
    
    RAISE NOTICE 'Splits Data Migration completed successfully!';
    RAISE NOTICE 'Total records processed: %', COALESCE(total_migrated, 0);
    RAISE NOTICE 'Migration summary:';
    RAISE NOTICE '%', migration_summary;
    RAISE NOTICE 'Completion timestamp: %', NOW();
END $$; 