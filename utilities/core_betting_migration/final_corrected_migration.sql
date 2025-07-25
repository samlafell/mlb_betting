-- =========================================================================
-- Core Betting Schema Decommission - FINAL CORRECTED Migration Scripts
-- =========================================================================
-- Generated: 2025-01-25 (Final correction based on ACTUAL schema analysis)
-- Purpose: Migrate all data from core_betting schema to curated schema
-- 
-- FINAL VERSION - Uses actual field names from database inspection
-- =========================================================================

-- Drop any existing migration progress and start clean
TRUNCATE TABLE operational.core_betting_migration_log;
TRUNCATE TABLE operational.pre_migration_counts;
DROP TABLE IF EXISTS operational.post_migration_counts CASCADE;

-- -------------------------------------------------------------------------
-- PHASE 1: RECORD ACTUAL PRE-MIGRATION COUNTS
-- -------------------------------------------------------------------------

INSERT INTO operational.core_betting_migration_log (phase, operation, status, details)
VALUES ('PHASE_1', 'FINAL_MIGRATION_START', 'started', '{"description": "Final corrected core betting schema migration"}');

-- Record ACTUAL current row counts
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
-- PHASE 2: MIGRATE BETTING LINES WITH ACTUAL FIELD NAMES
-- -------------------------------------------------------------------------

-- Migrate moneyline betting lines (using actual field names: home_ml, away_ml)
INSERT INTO curated.betting_lines_unified (
    external_line_id, game_id, sportsbook_id, market_type, line_type,
    home_moneyline, away_moneyline, recorded_at, data_quality_score,
    source_system, created_at, migrated_at
)
SELECT 
    CONCAT('ml_', id) as external_line_id,
    game_id, sportsbook_id, 'moneyline' as market_type, 
    'current' as line_type,  -- No line_type in source, use default
    home_ml as home_moneyline, away_ml as away_moneyline,
    COALESCE(odds_timestamp, updated_at, CURRENT_TIMESTAMP) as recorded_at,
    COALESCE(data_completeness_score, 1.0) as data_quality_score,
    'core_betting_migration' as source_system,
    COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
    CURRENT_TIMESTAMP as migrated_at
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
WHERE game_id IN (SELECT id FROM curated.games_complete)
  AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;

-- Migrate spread betting lines (check actual field names first)
DO $$
DECLARE
    spread_cols_exist BOOLEAN;
BEGIN
    -- Check if spread table has the expected columns
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core_betting' 
          AND table_name = 'betting_lines_spread'
          AND column_name IN ('home_spread', 'away_spread', 'home_odds', 'away_odds')
    ) INTO spread_cols_exist;
    
    IF spread_cols_exist THEN
        INSERT INTO curated.betting_lines_unified (
            external_line_id, game_id, sportsbook_id, market_type, line_type,
            spread_home, spread_away, spread_home_odds, spread_away_odds,
            recorded_at, data_quality_score, source_system, created_at, migrated_at
        )
        SELECT 
            CONCAT('sp_', id) as external_line_id,
            game_id, sportsbook_id, 'spread' as market_type,
            'current' as line_type,
            home_spread as spread_home, 
            away_spread as spread_away,
            home_odds as spread_home_odds, 
            away_odds as spread_away_odds,
            COALESCE(odds_timestamp, updated_at, CURRENT_TIMESTAMP) as recorded_at,
            COALESCE(data_completeness_score, 1.0) as data_quality_score,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'
        WHERE game_id IN (SELECT id FROM curated.games_complete)
          AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
        ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;
        
        RAISE NOTICE 'Migrated spread lines with standard columns';
    ELSE
        RAISE NOTICE 'Spread table columns do not match expected format, skipping';
    END IF;
END $$;

-- Migrate totals betting lines (check actual field names first)
DO $$
DECLARE
    totals_cols_exist BOOLEAN;
BEGIN
    -- Check if totals table has the expected columns
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core_betting' 
          AND table_name = 'betting_lines_totals'
          AND column_name IN ('total_line', 'over_odds', 'under_odds')
    ) INTO totals_cols_exist;
    
    IF totals_cols_exist THEN
        INSERT INTO curated.betting_lines_unified (
            external_line_id, game_id, sportsbook_id, market_type, line_type,
            total_line, over_odds, under_odds, recorded_at, data_quality_score,
            source_system, created_at, migrated_at
        )
        SELECT 
            CONCAT('tot_', id) as external_line_id,
            game_id, sportsbook_id, 'totals' as market_type,
            'current' as line_type,
            total_line, over_odds, under_odds,
            COALESCE(odds_timestamp, updated_at, CURRENT_TIMESTAMP) as recorded_at,
            COALESCE(data_completeness_score, 1.0) as data_quality_score,
            'core_betting_migration' as source_system,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            CURRENT_TIMESTAMP as migrated_at
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
        WHERE game_id IN (SELECT id FROM curated.games_complete)
          AND sportsbook_id IN (SELECT id FROM curated.sportsbooks)
        ON CONFLICT (external_line_id, sportsbook_id, market_type, line_type) DO NOTHING;
        
        RAISE NOTICE 'Migrated totals lines with standard columns';
    ELSE
        RAISE NOTICE 'Totals table columns do not match expected format, skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- PHASE 3: MIGRATE SPORTSBOOK MAPPINGS WITH CORRECT FIELD NAMES
-- -------------------------------------------------------------------------

-- Migrate sportsbook external mappings (using actual field names: external_id, external_name)
INSERT INTO curated.sportsbook_mappings (
    sportsbook_id, external_source, external_sportsbook_id, external_sportsbook_name,
    mapping_confidence, validation_status, source_system, created_at, updated_at, migrated_at
)
SELECT 
    sportsbook_id, external_source, external_id as external_sportsbook_id, external_name as external_sportsbook_name,
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

-- -------------------------------------------------------------------------
-- PHASE 4: FINAL VALIDATION AND COUNTS
-- -------------------------------------------------------------------------

-- Create post-migration counts table
CREATE TABLE operational.post_migration_counts (
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
SELECT 'curated.sportsbook_mappings', COUNT(*) FROM curated.sportsbook_mappings WHERE source_system = 'core_betting_migration';

-- Final validation query
CREATE OR REPLACE VIEW operational.v_final_migration_validation AS
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
        WHEN COALESCE(post.post_migration_count, 0) > 0 AND COALESCE(pre.pre_migration_count, 0) > 0 THEN 
            CASE WHEN COALESCE(post.post_migration_count, 0) >= COALESCE(pre.pre_migration_count, 0) * 0.9 THEN '⚠️ MOSTLY_MIGRATED'
                 ELSE '⚠️ PARTIAL_MIGRATION'
            END
        WHEN COALESCE(post.post_migration_count, 0) = 0 AND COALESCE(pre.pre_migration_count, 0) > 0 THEN '❌ MISSING_RECORDS'
        ELSE '⚠️ UNKNOWN_STATUS'
    END as validation_status
FROM pre_counts pre
FULL OUTER JOIN post_counts post ON pre.target_table = post.target_table
WHERE COALESCE(pre.target_table, post.target_table) IS NOT NULL
ORDER BY table_name;

-- Log final completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details, end_time)
VALUES ('PHASE_FINAL', 'BETTING_LINES_MIGRATION_COMPLETE', 'completed', 
    jsonb_build_object(
        'total_phases', 'FINAL',
        'migration_completed_at', CURRENT_TIMESTAMP,
        'validation_view', 'operational.v_final_migration_validation',
        'status_view', 'operational.v_core_betting_migration_status',
        'betting_lines_migrated', (SELECT COUNT(*) FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'),
        'next_steps', ARRAY[
            'Review final validation results',
            'Test betting lines queries',
            'Execute code refactoring',
            'Update external FK constraints (manual step)',
            'Drop core_betting schema (only after complete validation)'
        ]
    ),
    CURRENT_TIMESTAMP);

-- Display final results
SELECT 'FINAL CORE BETTING MIGRATION COMPLETED' as status;
SELECT * FROM operational.v_final_migration_validation;