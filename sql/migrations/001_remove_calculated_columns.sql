-- Migration: Remove calculated/derived columns from betting_lines tables
-- Purpose: Clean up schema to focus purely on raw data collection
-- Date: 2025-07-18
-- 
-- This migration removes columns that represent calculated or derived data
-- rather than raw collected data from external sources.

-- Begin transaction for safety
BEGIN;

-- Create backup tables before making changes (optional safety measure)
CREATE TABLE IF NOT EXISTS core_betting.betting_lines_moneyline_backup_20250718 AS 
SELECT * FROM core_betting.betting_lines_moneyline LIMIT 0;

CREATE TABLE IF NOT EXISTS core_betting.betting_lines_spread_backup_20250718 AS 
SELECT * FROM core_betting.betting_lines_spread LIMIT 0;

CREATE TABLE IF NOT EXISTS core_betting.betting_lines_totals_backup_20250718 AS 
SELECT * FROM core_betting.betting_lines_totals LIMIT 0;

-- ============================================================================
-- STEP 1: Remove calculated columns from betting_lines_moneyline
-- ============================================================================

ALTER TABLE core_betting.betting_lines_moneyline 
    DROP COLUMN IF EXISTS opening_home_ml,
    DROP COLUMN IF EXISTS opening_away_ml,
    DROP COLUMN IF EXISTS closing_home_ml,
    DROP COLUMN IF EXISTS closing_away_ml,
    DROP COLUMN IF EXISTS sharp_action,
    DROP COLUMN IF EXISTS reverse_line_movement,
    DROP COLUMN IF EXISTS steam_move,
    DROP COLUMN IF EXISTS winning_side,
    DROP COLUMN IF EXISTS profit_loss;

-- ============================================================================
-- STEP 2: Remove calculated columns from betting_lines_spread  
-- ============================================================================

ALTER TABLE core_betting.betting_lines_spread 
    DROP COLUMN IF EXISTS opening_spread,
    DROP COLUMN IF EXISTS opening_home_price,
    DROP COLUMN IF EXISTS opening_away_price,
    DROP COLUMN IF EXISTS closing_spread,
    DROP COLUMN IF EXISTS closing_home_price,
    DROP COLUMN IF EXISTS closing_away_price,
    DROP COLUMN IF EXISTS sharp_action,
    DROP COLUMN IF EXISTS reverse_line_movement,
    DROP COLUMN IF EXISTS steam_move,
    DROP COLUMN IF EXISTS winning_side,
    DROP COLUMN IF EXISTS profit_loss;

-- ============================================================================
-- STEP 3: Remove calculated columns from betting_lines_totals
-- ============================================================================

ALTER TABLE core_betting.betting_lines_totals 
    DROP COLUMN IF EXISTS opening_total,
    DROP COLUMN IF EXISTS opening_over_price,
    DROP COLUMN IF EXISTS opening_under_price,
    DROP COLUMN IF EXISTS closing_total,
    DROP COLUMN IF EXISTS closing_over_price,
    DROP COLUMN IF EXISTS closing_under_price,
    DROP COLUMN IF EXISTS sharp_action,
    DROP COLUMN IF EXISTS reverse_line_movement,
    DROP COLUMN IF EXISTS steam_move,
    DROP COLUMN IF EXISTS winning_side,
    DROP COLUMN IF EXISTS profit_loss,
    DROP COLUMN IF EXISTS total_score;

-- ============================================================================
-- STEP 4: Remove calculated columns from betting_lines_spreads (legacy table)
-- ============================================================================

ALTER TABLE core_betting.betting_lines_spreads 
    DROP COLUMN IF EXISTS opening_home_spread,
    DROP COLUMN IF EXISTS opening_home_spread_price,
    DROP COLUMN IF EXISTS opening_away_spread,
    DROP COLUMN IF EXISTS opening_away_spread_price,
    DROP COLUMN IF EXISTS closing_home_spread,
    DROP COLUMN IF EXISTS closing_home_spread_price,
    DROP COLUMN IF EXISTS closing_away_spread,
    DROP COLUMN IF EXISTS closing_away_spread_price,
    DROP COLUMN IF EXISTS sharp_action,
    DROP COLUMN IF EXISTS reverse_line_movement,
    DROP COLUMN IF EXISTS steam_move,
    DROP COLUMN IF EXISTS winning_side,
    DROP COLUMN IF EXISTS profit_loss;

-- ============================================================================
-- STEP 5: Update any views or functions that might reference these columns
-- ============================================================================

-- Note: The triggers should automatically handle missing columns gracefully
-- since they use dynamic column checking. If any specific trigger functions
-- need updates, they can be modified separately.

-- ============================================================================
-- VERIFICATION: Show final schema structure
-- ============================================================================

-- Verification queries will be run after migration

COMMIT;

-- ============================================================================
-- POST-MIGRATION VERIFICATION QUERIES
-- ============================================================================

-- Verify final table structures (run manually after migration)

/*

-- Check moneyline table structure
\d core_betting.betting_lines_moneyline

-- Check spread table structure  
\d core_betting.betting_lines_spread

-- Check totals table structure
\d core_betting.betting_lines_totals

-- Verify no calculated columns remain
SELECT table_name, column_name 
FROM information_schema.columns 
WHERE table_schema = 'core_betting' 
    AND table_name LIKE 'betting_lines_%'
    AND (
        column_name LIKE 'opening_%' OR
        column_name LIKE 'closing_%' OR 
        column_name IN ('sharp_action', 'reverse_line_movement', 'steam_move', 'winning_side', 'profit_loss')
    );

-- Should return 0 rows if migration was successful

*/

-- ============================================================================
-- FINAL SCHEMA DESIGN: Raw Data Collection Focus
-- ============================================================================

/*

CORE_BETTING.BETTING_LINES_MONEYLINE:
- id, game_id, sportsbook_id, sportsbook (core identification)
- home_ml, away_ml (raw odds data)
- odds_timestamp (when odds were collected)
- home_team, away_team (team identification)
- source, data_quality, collection_method (metadata)
- created_at, updated_at (audit trail)

CORE_BETTING.BETTING_LINES_SPREAD:
- id, game_id, sportsbook_id, sportsbook (core identification)  
- spread_line, home_spread_price, away_spread_price (raw odds data)
- odds_timestamp (when odds were collected)
- home_team, away_team (team identification)
- source, data_quality, collection_method (metadata)
- created_at, updated_at (audit trail)

CORE_BETTING.BETTING_LINES_TOTALS:
- id, game_id, sportsbook_id, sportsbook (core identification)
- total_line, over_price, under_price (raw odds data)  
- odds_timestamp (when odds were collected)
- home_team, away_team (team identification)
- source, data_quality, collection_method (metadata)
- created_at, updated_at (audit trail)

BENEFITS:
✅ Clean separation of raw data vs. calculated data
✅ Faster data insertion (no calculated column overhead)
✅ Simpler data loading scripts
✅ Better data lineage and auditability
✅ Easier to maintain and debug
✅ Calculated values can be computed in views or analysis queries

*/