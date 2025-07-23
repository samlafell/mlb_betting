-- Legacy Core Betting Schema Migration Analysis
-- Analysis of legacy core_betting tables and migration plan to three-tier architecture

-- ==============================
-- LEGACY TABLES IDENTIFIED
-- ==============================

-- From Phase 1 validation, these legacy core_betting tables exist:
-- 1. core_betting.supplementary_games
-- 2. core_betting.betting_lines_totals  
-- 3. core_betting.data_migrations
-- 4. core_betting.sportsbook_external_mappings
-- 5. core_betting.data_source_metadata
-- 6. core_betting.betting_lines_moneyline
-- 7. core_betting.games
-- 8. core_betting.game_outcomes
-- 9. core_betting.teams
-- 10. core_betting.sportsbooks
-- 11. core_betting.steam_moves
-- 12. core_betting.line_movements
-- 13. core_betting.sharp_action_indicators
-- 14. core_betting.betting_splits
-- 15. core_betting.betting_lines_spread
-- 16. core_betting.line_movement_history

-- ==============================
-- ANALYSIS QUERIES
-- ==============================

-- Check table sizes and record counts
SELECT 
    schemaname,
    tablename,
    n_tup_ins as total_insertions,
    n_tup_upd as total_updates,
    n_tup_del as total_deletions,
    n_live_tup as live_tuples,
    n_dead_tup as dead_tuples,
    last_vacuum,
    last_analyze
FROM pg_stat_user_tables 
WHERE schemaname = 'core_betting'
ORDER BY n_live_tup DESC;

-- Get table schemas for each legacy table
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'core_betting'
ORDER BY table_name, ordinal_position;

-- Check for foreign key relationships
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' 
    AND tc.table_schema = 'core_betting';

-- Check for indexes on legacy tables
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE schemaname = 'core_betting'
ORDER BY tablename, indexname;

-- ==============================
-- MIGRATION MAPPING ANALYSIS
-- ==============================

-- Based on three-tier architecture:
-- RAW_DATA: Raw, unprocessed data from external sources
-- STAGING: Processed, normalized data ready for analysis  
-- CURATED: Final, business-ready data with quality scoring

-- PROPOSED MIGRATION MAPPING:
-- 
-- REFERENCE DATA (should go to STAGING):
-- core_betting.teams -> staging.teams (enhanced)
-- core_betting.sportsbooks -> staging.sportsbooks (enhanced)
-- core_betting.data_source_metadata -> staging.data_source_metadata
-- core_betting.sportsbook_external_mappings -> staging.sportsbook_external_mappings
--
-- OPERATIONAL DATA (should go to RAW_DATA for reprocessing):
-- core_betting.games -> raw_data.legacy_games (for reprocessing to staging.games)
-- core_betting.game_outcomes -> raw_data.legacy_game_outcomes
-- core_betting.betting_lines_spread -> raw_data.legacy_betting_lines_spread
-- core_betting.betting_lines_moneyline -> raw_data.legacy_betting_lines_moneyline
-- core_betting.betting_lines_totals -> raw_data.legacy_betting_lines_totals
-- core_betting.line_movements -> raw_data.legacy_line_movements
-- core_betting.betting_splits -> raw_data.legacy_betting_splits
--
-- ANALYSIS DATA (should go to STAGING):
-- core_betting.sharp_action_indicators -> staging.sharp_action_signals (merge with existing)
-- core_betting.steam_moves -> staging.steam_moves
-- core_betting.line_movement_history -> staging.line_movement_history
--
-- SYSTEM DATA (keep for historical reference):
-- core_betting.data_migrations -> archive.legacy_data_migrations
-- core_betting.supplementary_games -> archive.legacy_supplementary_games

-- ==============================
-- DATA VALIDATION QUERIES
-- ==============================

-- Check for data quality issues in legacy tables
-- Check for NULL values in critical fields
SELECT 'games' as table_name, 
       COUNT(*) as total_records,
       COUNT(CASE WHEN game_id IS NULL THEN 1 END) as null_game_ids,
       COUNT(CASE WHEN home_team_id IS NULL THEN 1 END) as null_home_teams,
       COUNT(CASE WHEN away_team_id IS NULL THEN 1 END) as null_away_teams
FROM core_betting.games
UNION ALL
SELECT 'betting_lines_spread' as table_name,
       COUNT(*) as total_records, 
       COUNT(CASE WHEN game_id IS NULL THEN 1 END) as null_game_ids,
       COUNT(CASE WHEN sportsbook_id IS NULL THEN 1 END) as null_sportsbooks,
       COUNT(CASE WHEN spread_value IS NULL THEN 1 END) as null_spreads
FROM core_betting.betting_lines_spread;

-- Check date ranges in legacy data
SELECT 
    'games' as table_name,
    MIN(game_date) as earliest_date,
    MAX(game_date) as latest_date,
    COUNT(*) as total_records
FROM core_betting.games
WHERE game_date IS NOT NULL
UNION ALL
SELECT 
    'betting_lines_spread' as table_name,
    MIN(created_at) as earliest_date,
    MAX(created_at) as latest_date,
    COUNT(*) as total_records  
FROM core_betting.betting_lines_spread
WHERE created_at IS NOT NULL;

-- ==============================
-- MIGRATION SAFETY CHECKS
-- ==============================

-- Verify no active processes are using legacy tables
SELECT 
    schemaname,
    tablename,
    n_tup_ins - LAG(n_tup_ins) OVER (PARTITION BY schemaname, tablename ORDER BY schemaname, tablename) as recent_inserts,
    n_tup_upd - LAG(n_tup_upd) OVER (PARTITION BY schemaname, tablename ORDER BY schemaname, tablename) as recent_updates
FROM pg_stat_user_tables 
WHERE schemaname = 'core_betting'
ORDER BY tablename;

-- Create archive schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS archive;

-- Create backup schema  
CREATE SCHEMA IF NOT EXISTS backup_core_betting;

COMMENT ON SCHEMA archive IS 'Archive schema for legacy data that needs to be preserved but not actively used';
COMMENT ON SCHEMA backup_core_betting IS 'Backup of core_betting schema before migration';