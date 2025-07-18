-- Migration: Remove calculated/derived columns from betting_lines tables
-- Purpose: Clean up schema to focus purely on raw data collection
-- Date: 2025-07-18
-- 
-- This migration safely removes columns that represent calculated or derived data
-- by first backing up and dropping dependent views, then recreating them.

-- Begin transaction for safety
BEGIN;

-- ============================================================================
-- STEP 1: Create backup of dependent views
-- ============================================================================

-- Backup data_quality_dashboard view
CREATE OR REPLACE VIEW core_betting.data_quality_dashboard_backup AS
SELECT * FROM core_betting.data_quality_dashboard;

-- ============================================================================
-- STEP 2: Drop dependent views temporarily
-- ============================================================================

DROP VIEW IF EXISTS core_betting.data_quality_dashboard CASCADE;
DROP VIEW IF EXISTS core_betting.data_quality_trend CASCADE;
DROP VIEW IF EXISTS core_betting.data_source_quality_analysis CASCADE;
DROP VIEW IF EXISTS core_betting.sportsbook_mapping_status CASCADE;
DROP VIEW IF EXISTS core_betting.unmapped_sportsbook_analysis CASCADE;
DROP VIEW IF EXISTS analytics.unified_betting_lines CASCADE;

-- ============================================================================
-- STEP 3: Remove calculated columns from betting_lines_moneyline
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
-- STEP 4: Remove calculated columns from betting_lines_spread  
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
-- STEP 5: Remove calculated columns from betting_lines_totals
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
-- STEP 6: Remove calculated columns from betting_lines_spreads (legacy table)
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
-- STEP 7: Recreate essential views without calculated columns
-- ============================================================================

-- Recreate data_quality_dashboard view without sharp_action references
CREATE OR REPLACE VIEW core_betting.data_quality_dashboard AS
SELECT 
    'moneyline'::text AS table_name,
    count(*) AS total_rows,
    count(sportsbook_id) AS mapped_sportsbooks,
    round(avg(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS sportsbook_id_pct,
    0.0::numeric AS sharp_action_pct,  -- Removed: sharp_action no longer exists
    round(avg(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS betting_pct_pct,
    round(avg(CASE WHEN home_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS money_pct_pct,
    round(avg(data_completeness_score), 3) AS avg_completeness,
    count(CASE WHEN data_quality::text = 'HIGH'::text THEN 1 ELSE NULL::integer END) AS high_quality_count,
    count(CASE WHEN data_quality::text = 'MEDIUM'::text THEN 1 ELSE NULL::integer END) AS medium_quality_count,
    count(CASE WHEN data_quality::text = 'LOW'::text THEN 1 ELSE NULL::integer END) AS low_quality_count
FROM core_betting.betting_lines_moneyline

UNION ALL

SELECT 
    'spread'::text AS table_name,
    count(*) AS total_rows,
    count(sportsbook_id) AS mapped_sportsbooks,
    round(avg(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS sportsbook_id_pct,
    0.0::numeric AS sharp_action_pct,  -- Removed: sharp_action no longer exists
    round(avg(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS betting_pct_pct,
    round(avg(CASE WHEN home_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS money_pct_pct,
    round(avg(data_completeness_score), 3) AS avg_completeness,
    count(CASE WHEN data_quality::text = 'HIGH'::text THEN 1 ELSE NULL::integer END) AS high_quality_count,
    count(CASE WHEN data_quality::text = 'MEDIUM'::text THEN 1 ELSE NULL::integer END) AS medium_quality_count,
    count(CASE WHEN data_quality::text = 'LOW'::text THEN 1 ELSE NULL::integer END) AS low_quality_count
FROM core_betting.betting_lines_spread

UNION ALL

SELECT 
    'totals'::text AS table_name,
    count(*) AS total_rows,
    count(sportsbook_id) AS mapped_sportsbooks,
    round(avg(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS sportsbook_id_pct,
    0.0::numeric AS sharp_action_pct,  -- Removed: sharp_action no longer exists
    round(avg(CASE WHEN over_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS betting_pct_pct,
    round(avg(CASE WHEN over_money_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100::numeric, 2) AS money_pct_pct,
    round(avg(data_completeness_score), 3) AS avg_completeness,
    count(CASE WHEN data_quality::text = 'HIGH'::text THEN 1 ELSE NULL::integer END) AS high_quality_count,
    count(CASE WHEN data_quality::text = 'MEDIUM'::text THEN 1 ELSE NULL::integer END) AS medium_quality_count,
    count(CASE WHEN data_quality::text = 'LOW'::text THEN 1 ELSE NULL::integer END) AS low_quality_count
FROM core_betting.betting_lines_totals;

-- Recreate sportsbook_mapping_status view (simplified)
CREATE OR REPLACE VIEW core_betting.sportsbook_mapping_status AS
SELECT 
    'moneyline' as table_name,
    count(*) as total_records,
    count(sportsbook_id) as mapped_records,
    round(count(sportsbook_id)::numeric / count(*)::numeric * 100, 2) as mapping_percentage
FROM core_betting.betting_lines_moneyline
UNION ALL
SELECT 
    'spread' as table_name,
    count(*) as total_records,
    count(sportsbook_id) as mapped_records,
    round(count(sportsbook_id)::numeric / count(*)::numeric * 100, 2) as mapping_percentage
FROM core_betting.betting_lines_spread
UNION ALL
SELECT 
    'totals' as table_name,
    count(*) as total_records,
    count(sportsbook_id) as mapped_records,
    round(count(sportsbook_id)::numeric / count(*)::numeric * 100, 2) as mapping_percentage
FROM core_betting.betting_lines_totals;

-- ============================================================================
-- VERIFICATION: Confirm clean schema
-- ============================================================================

-- Check that calculated columns have been removed
DO $$
DECLARE
    remaining_calc_columns INTEGER;
BEGIN
    SELECT COUNT(*) INTO remaining_calc_columns
    FROM information_schema.columns 
    WHERE table_schema = 'core_betting' 
        AND table_name LIKE 'betting_lines_%'
        AND (
            column_name LIKE 'opening_%' OR
            column_name LIKE 'closing_%' OR 
            column_name IN ('sharp_action', 'reverse_line_movement', 'steam_move', 'winning_side', 'profit_loss')
        );
    
    IF remaining_calc_columns > 0 THEN
        RAISE NOTICE 'WARNING: % calculated columns still exist', remaining_calc_columns;
    ELSE
        RAISE NOTICE 'SUCCESS: All calculated columns removed';
    END IF;
END $$;

COMMIT;

-- ============================================================================
-- POST-MIGRATION SUMMARY
-- ============================================================================

/*

SCHEMA CLEANUP COMPLETE ✅

REMOVED COLUMNS:
- opening_* columns (opening odds/lines)  
- closing_* columns (closing odds/lines)
- sharp_action (calculated indicator)
- reverse_line_movement (calculated flag)
- steam_move (calculated flag)  
- winning_side (outcome data)
- profit_loss (calculated P&L)
- total_score (outcome data)

REMAINING COLUMNS (Raw Data Focus):
- Core identification: id, game_id, sportsbook_id, sportsbook
- Raw odds data: home_ml/away_ml, spread_line/prices, total_line/prices
- Collection metadata: odds_timestamp, source, collection_method
- Data quality: data_quality, data_completeness_score
- Audit trail: created_at, updated_at
- Team info: home_team, away_team, game_datetime

BENEFITS:
✅ Clean separation of raw vs calculated data
✅ Faster data insertion (no calculated overhead)
✅ Simpler data loading scripts
✅ Better data lineage
✅ Analysis/calculated columns can be created in views

NEXT STEPS:
1. Update data loading scripts to remove references to deleted columns
2. Create analytical views for calculated metrics when needed
3. Test data loading with cleaned schema

*/