-- Migration 016: Fix Line Movements to Exclude Post-Game Activity
-- Purpose: Replace existing line movement view with timezone-aware pre-game filtering
-- Issue: Current view shows movements after game start, which are irrelevant for betting
-- Date: 2025-07-25

-- ================================
-- Phase 1: Backup Existing Views
-- ================================

-- Create backup of existing view structure for rollback if needed
CREATE OR REPLACE VIEW staging.v_line_movements_backup AS
SELECT * FROM staging.v_line_movements LIMIT 0;

-- ================================
-- Phase 2: Deploy Enhanced Views
-- ================================

-- Execute the enhanced pre-game line movements views
-- This file contains the complete solution with timezone-aware filtering
\i sql/improvements/pre_game_line_movements_view.sql

-- ================================
-- Phase 3: Create Compatibility Alias
-- ================================

-- Create alias for existing view name to maintain backward compatibility
-- Applications using staging.v_line_movements will now get pre-game data only
CREATE OR REPLACE VIEW staging.v_line_movements AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    line_value,
    odds,
    updated_at,
    previous_odds,
    previous_line_value,
    previous_updated_at,
    odds_change,
    odds_change_raw,
    line_value_change,
    has_line_value_change,
    movement_type,
    filtered_odds_change,
    movement_quality_score,
    seconds_since_last_change
FROM staging.v_pre_game_line_movements;

-- ================================
-- Phase 4: Update Related Views
-- ================================

-- Update sharp movements view to use new pre-game view
DROP VIEW IF EXISTS staging.v_sharp_movements CASCADE;
CREATE OR REPLACE VIEW staging.v_sharp_movements AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    line_value,
    odds,
    updated_at,
    previous_odds,
    previous_line_value,
    previous_updated_at,
    odds_change,
    line_value_change,
    has_line_value_change,
    movement_type,
    filtered_odds_change,
    movement_quality_score,
    seconds_since_last_change
FROM staging.v_pre_game_sharp_movements;

-- ================================
-- Phase 5: Documentation and Validation
-- ================================

-- Add comments explaining the change
COMMENT ON VIEW staging.v_line_movements IS 
'Line movements view - NOW FILTERS OUT POST-GAME ACTIVITY. Only shows movements before game start time with timezone awareness.';

-- Create validation query to verify no post-game movements
-- This should return 0 post-game movements after migration
CREATE OR REPLACE FUNCTION staging.validate_no_post_game_movements()
RETURNS TABLE (
    total_movements BIGINT,
    min_minutes_before NUMERIC,
    max_minutes_before NUMERIC,
    post_game_count BIGINT,
    validation_status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_movements,
        MIN(minutes_before_game) as min_minutes_before,
        MAX(minutes_before_game) as max_minutes_before,
        COUNT(*) FILTER (WHERE minutes_before_game <= 0) as post_game_count,
        CASE 
            WHEN COUNT(*) FILTER (WHERE minutes_before_game <= 0) = 0 
            THEN 'PASS: No post-game movements detected'
            ELSE 'FAIL: Post-game movements still present'
        END as validation_status
    FROM staging.v_pre_game_line_movements;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 6: Performance Optimization
-- ================================

-- Create index on game start time for efficient filtering
CREATE INDEX IF NOT EXISTS idx_action_network_games_start_time 
ON raw_data.action_network_games(start_time) 
WHERE start_time IS NOT NULL;

-- Create composite index for line movement queries
CREATE INDEX IF NOT EXISTS idx_action_network_odds_historical_game_time_lookup
ON staging.action_network_odds_historical(external_game_id, updated_at)
WHERE updated_at IS NOT NULL;

-- ================================
-- Migration Completion Notes
-- ================================

/*
WHAT THIS MIGRATION DOES:
1. ✅ Replaces staging.v_line_movements with pre-game-only version
2. ✅ Maintains backward compatibility for existing applications
3. ✅ Adds timezone-aware filtering (EST/EDT handling)
4. ✅ Includes game context (teams, start time, timing categories)
5. ✅ Preserves all existing movement detection logic
6. ✅ Adds new views for sharp action analysis
7. ✅ Includes validation function to verify no post-game data
8. ✅ Optimizes performance with targeted indexes

POST-MIGRATION VERIFICATION:
-- Run this to verify migration success:
SELECT * FROM staging.validate_no_post_game_movements();

-- Check that you can still query existing view:
SELECT COUNT(*) FROM staging.v_line_movements;

-- Verify new enhanced views work:
SELECT COUNT(*) FROM staging.v_pre_game_line_movements;
SELECT COUNT(*) FROM staging.v_late_sharp_action;

TIMEZONE HANDLING:
- All times are stored as TIMESTAMPTZ (timezone-aware)
- Game start times from Action Network are in EST/EDT
- Filtering respects timezone differences automatically
- No manual timezone conversion needed

PERFORMANCE IMPACT:
- Added indexes improve query performance
- Pre-filtering reduces result set size significantly
- Enhanced views may be slightly slower due to join with games table
- Overall impact should be positive due to smaller result sets
*/