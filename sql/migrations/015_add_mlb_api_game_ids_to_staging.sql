-- Migration: Add MLB Stats API Game IDs to All Staging Tables
-- Purpose: Enable cross-system integration and unified game outcome analysis
-- Created: 2025-07-24

-- ================================
-- Phase 1: Add MLB API Game ID to All Staging Tables
-- ================================

-- First, let's check what staging tables exist and need the column
DO $$
DECLARE
    tbl_name TEXT;
    table_list TEXT[] := ARRAY[
        'staging.action_network_odds_historical',
        'staging.action_network_games',
        'staging.vsin_betting_data',
        'staging.sbd_betting_splits', 
        'staging.betting_splits',
        'staging.odds_api_games',
        'staging.line_movements',
        'staging.sharp_action_indicators'
    ];
BEGIN
    FOREACH tbl_name IN ARRAY table_list
    LOOP
        -- Check if table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables t
            WHERE t.table_schema = split_part(tbl_name, '.', 1) 
            AND t.table_name = split_part(tbl_name, '.', 2)
        ) THEN
            -- Check if column already exists
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns c
                WHERE c.table_schema = split_part(tbl_name, '.', 1)
                AND c.table_name = split_part(tbl_name, '.', 2)
                AND c.column_name = 'mlb_stats_api_game_id'
            ) THEN
                EXECUTE format('ALTER TABLE %s ADD COLUMN mlb_stats_api_game_id VARCHAR(50)', tbl_name);
                RAISE NOTICE 'Added mlb_stats_api_game_id column to %', tbl_name;
            ELSE
                RAISE NOTICE 'Column mlb_stats_api_game_id already exists in %', tbl_name;
            END IF;
        ELSE
            RAISE NOTICE 'Table % does not exist, skipping', tbl_name;
        END IF;
    END LOOP;
END $$;

-- ================================
-- Phase 2: Add Indexes for Performance
-- ================================

-- Action Network tables
CREATE INDEX IF NOT EXISTS idx_an_odds_historical_mlb_api_game_id 
ON staging.action_network_odds_historical(mlb_stats_api_game_id);

CREATE INDEX IF NOT EXISTS idx_an_games_mlb_api_game_id 
ON staging.action_network_games(mlb_stats_api_game_id);

-- VSIN table (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'vsin_betting_data') THEN
        CREATE INDEX IF NOT EXISTS idx_vsin_betting_data_mlb_api_game_id 
        ON staging.vsin_betting_data(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.vsin_betting_data';
    END IF;
END $$;

-- SBD tables (if exist)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'sbd_betting_splits') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'sbd_betting_splits' AND c.column_name = 'mlb_stats_api_game_id') THEN
        CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_mlb_api_game_id 
        ON staging.sbd_betting_splits(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.sbd_betting_splits';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'betting_splits') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'betting_splits' AND c.column_name = 'mlb_stats_api_game_id') THEN
        CREATE INDEX IF NOT EXISTS idx_betting_splits_mlb_api_game_id 
        ON staging.betting_splits(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.betting_splits';
    END IF;
END $$;

-- Odds API table (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'odds_api_games') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'odds_api_games' AND c.column_name = 'mlb_stats_api_game_id') THEN
        CREATE INDEX IF NOT EXISTS idx_odds_api_games_mlb_api_game_id 
        ON staging.odds_api_games(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.odds_api_games';
    END IF;
END $$;

-- Line movements table (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'line_movements') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'line_movements' AND c.column_name = 'mlb_stats_api_game_id') THEN
        CREATE INDEX IF NOT EXISTS idx_line_movements_mlb_api_game_id 
        ON staging.line_movements(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.line_movements';
    END IF;
END $$;

-- Sharp action indicators table (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'sharp_action_indicators') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'sharp_action_indicators' AND c.column_name = 'mlb_stats_api_game_id') THEN
        CREATE INDEX IF NOT EXISTS idx_sharp_action_indicators_mlb_api_game_id 
        ON staging.sharp_action_indicators(mlb_stats_api_game_id);
        RAISE NOTICE 'Created index for staging.sharp_action_indicators';
    END IF;
END $$;

-- ================================
-- Phase 3: Create Composite Indexes for Common Queries
-- ================================

-- Action Network historical odds - game + market queries
CREATE INDEX IF NOT EXISTS idx_an_odds_historical_mlb_market 
ON staging.action_network_odds_historical(mlb_stats_api_game_id, market_type, side);

-- Action Network historical odds - game + sportsbook queries  
CREATE INDEX IF NOT EXISTS idx_an_odds_historical_mlb_sportsbook 
ON staging.action_network_odds_historical(mlb_stats_api_game_id, sportsbook_name);

-- SBD betting splits - game + sportsbook queries (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'sbd_betting_splits') 
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'sbd_betting_splits' AND c.column_name = 'mlb_stats_api_game_id')
       AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema = 'staging' AND c.table_name = 'sbd_betting_splits' AND c.column_name = 'sportsbook_name') THEN
        CREATE INDEX IF NOT EXISTS idx_sbd_splits_mlb_sportsbook 
        ON staging.sbd_betting_splits(mlb_stats_api_game_id, sportsbook_name);
        RAISE NOTICE 'Created composite index for staging.sbd_betting_splits';
    END IF;
END $$;

-- ================================
-- Phase 4: Add Comments for Documentation
-- ================================

-- Add comments to document the purpose of these columns
DO $$
DECLARE
    tbl_name TEXT;
    table_list TEXT[] := ARRAY[
        'staging.action_network_odds_historical',
        'staging.action_network_games'
    ];
BEGIN
    FOREACH tbl_name IN ARRAY table_list
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.tables t
            WHERE t.table_schema = split_part(tbl_name, '.', 1) 
            AND t.table_name = split_part(tbl_name, '.', 2)
        ) THEN
            EXECUTE format(
                'COMMENT ON COLUMN %s.mlb_stats_api_game_id IS %L',
                tbl_name,
                'MLB Stats API official game ID for cross-system integration and unified outcome analysis'
            );
        END IF;
    END LOOP;
END $$;

-- ================================
-- Phase 5: Create Utility Functions
-- ================================

-- Function to find games missing MLB API game IDs
CREATE OR REPLACE FUNCTION staging.find_games_missing_mlb_ids()
RETURNS TABLE(
    table_name TEXT,
    count_missing BIGINT,
    sample_external_id TEXT
) AS $$
BEGIN
    -- Check Action Network odds historical
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'action_network_odds_historical') THEN
        RETURN QUERY
        SELECT 
            'staging.action_network_odds_historical'::TEXT,
            COUNT(*) as count_missing,
            MIN(external_game_id)::TEXT as sample_external_id
        FROM staging.action_network_odds_historical 
        WHERE mlb_stats_api_game_id IS NULL;
    END IF;
    
    -- Check Action Network games
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'action_network_games') THEN
        RETURN QUERY
        SELECT 
            'staging.action_network_games'::TEXT,
            COUNT(*) as count_missing,
            MIN(external_game_id)::TEXT as sample_external_id
        FROM staging.action_network_games 
        WHERE mlb_stats_api_game_id IS NULL;
    END IF;
    
    -- Check SBD betting splits (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE t.table_schema = 'staging' AND t.table_name = 'sbd_betting_splits') THEN
        RETURN QUERY
        SELECT 
            'staging.sbd_betting_splits'::TEXT,
            COUNT(*) as count_missing,
            MIN(external_matchup_id)::TEXT as sample_external_id
        FROM staging.sbd_betting_splits 
        WHERE mlb_stats_api_game_id IS NULL;
    END IF;
    
    -- Check other tables as they're created...
    
END;
$$ LANGUAGE plpgsql;

-- Function to validate MLB API game ID format
CREATE OR REPLACE FUNCTION staging.validate_mlb_api_game_id(game_id TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    -- MLB Stats API game IDs are typically numeric strings (6-7 digits)
    -- Examples: 778494, 661309, etc.
    RETURN game_id ~ '^[0-9]{6,7}$';
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 6: Grant Permissions
-- ================================

-- Grant permissions for utility functions
GRANT EXECUTE ON FUNCTION staging.find_games_missing_mlb_ids() TO PUBLIC;
GRANT EXECUTE ON FUNCTION staging.validate_mlb_api_game_id(TEXT) TO PUBLIC;

-- ================================
-- Migration Summary and Validation
-- ================================

-- Display summary of changes
DO $$
DECLARE
    summary_text TEXT;
BEGIN
    summary_text := E'MLB Stats API Game ID Migration Summary:\n';
    summary_text := summary_text || E'- Added mlb_stats_api_game_id VARCHAR(50) to all existing staging tables\n';
    summary_text := summary_text || E'- Created indexes for performance optimization\n';
    summary_text := summary_text || E'- Added composite indexes for common query patterns\n';
    summary_text := summary_text || E'- Created utility functions for validation and monitoring\n';
    summary_text := summary_text || E'- Set up proper documentation and comments\n';
    summary_text := summary_text || E'\nNext Steps:\n';
    summary_text := summary_text || E'1. Update staging processors to populate MLB API game IDs\n';
    summary_text := summary_text || E'2. Run backfill script for existing data\n';
    summary_text := summary_text || E'3. Create unified views for cross-source analysis\n';
    
    RAISE NOTICE '%', summary_text;
END $$;

-- Check for any games missing MLB API IDs
SELECT * FROM staging.find_games_missing_mlb_ids();

-- ================================
-- Final Validation
-- ================================

-- Verify all indexes were created successfully
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE indexname LIKE '%mlb_api_game_id%'
ORDER BY schemaname, tablename, indexname;