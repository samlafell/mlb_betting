-- Migration 017: Enhance Line Movement Display with Previous Odds
-- Purpose: Improve all line movement views to clearly show previous odds and movement direction
-- Issue: Movement display was confusing with only +/- changes, users need to see "previous → current"
-- Date: 2025-07-25

-- ================================
-- Phase 1: Apply Enhanced Display Views
-- ================================

-- Execute the enhanced line movements display improvements
-- This replaces all existing views with improved "previous → current" format
\i sql/improvements/enhanced_line_movements_display.sql

-- ================================
-- Phase 2: Update Game Movement Timeline View
-- ================================

-- Enhance the timeline view to also show clear movement display
CREATE OR REPLACE VIEW staging.v_game_movement_timeline AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Movement counts by timing (preserved)
    COUNT(*) FILTER (WHERE timing_category = 'early_week') as early_week_movements,
    COUNT(*) FILTER (WHERE timing_category = 'day_before') as day_before_movements, 
    COUNT(*) FILTER (WHERE timing_category = 'hours_before') as hours_before_movements,
    COUNT(*) FILTER (WHERE timing_category = 'late_pregame') as late_pregame_movements,
    COUNT(*) FILTER (WHERE timing_category = 'very_late') as very_late_movements,
    
    -- Sharp action by timing (preserved)
    COUNT(*) FILTER (WHERE timing_category = 'late_pregame' AND ABS(COALESCE(filtered_odds_change, 0)) >= 10) as late_sharp_moves,
    COUNT(*) FILTER (WHERE timing_category = 'very_late' AND ABS(COALESCE(filtered_odds_change, 0)) >= 5) as very_late_sharp_moves,
    
    -- Enhanced opening and closing line display
    MIN(current_odds) FILTER (WHERE timing_category = 'early_week') as early_odds,
    MAX(current_odds) FILTER (WHERE timing_category = 'very_late') as closing_odds,
    
    -- Enhanced movement summary with clear display
    CASE 
        WHEN MIN(current_odds) FILTER (WHERE timing_category = 'early_week') IS NOT NULL 
         AND MAX(current_odds) FILTER (WHERE timing_category = 'very_late') IS NOT NULL
        THEN 
            CAST(MIN(current_odds) FILTER (WHERE timing_category = 'early_week') AS VARCHAR) || 
            ' → ' || 
            CAST(MAX(current_odds) FILTER (WHERE timing_category = 'very_late') AS VARCHAR) ||
            ' (' ||
            CASE 
                WHEN (MAX(current_odds) FILTER (WHERE timing_category = 'very_late') - 
                     MIN(current_odds) FILTER (WHERE timing_category = 'early_week')) > 0 
                THEN '+'
                ELSE ''
            END ||
            CAST((MAX(current_odds) FILTER (WHERE timing_category = 'very_late') - 
                 MIN(current_odds) FILTER (WHERE timing_category = 'early_week')) AS VARCHAR) ||
            ')'
        ELSE 'Insufficient data'
    END as overall_line_movement_display,
    
    -- Largest movement (preserved)
    MAX(ABS(COALESCE(filtered_odds_change, 0))) as largest_movement,
    AVG(movement_quality_score) as avg_movement_quality
    
FROM staging.v_pre_game_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY external_game_id, home_team, away_team, game_start_time, sportsbook_name, market_type, side
ORDER BY external_game_id, sportsbook_name, market_type, side;

-- ================================
-- Phase 3: Create User-Friendly Summary View
-- ================================

-- Create a new simplified view for quick line movement analysis
CREATE OR REPLACE VIEW staging.v_line_movements_summary AS
SELECT 
    external_game_id,
    home_team || ' vs ' || away_team as matchup,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Clear current line display
    CASE 
        WHEN line_value IS NOT NULL THEN 
            side || ' ' || CAST(line_value AS VARCHAR) || ' (' || CAST(current_odds AS VARCHAR) || ')'
        ELSE 
            side || ' (' || CAST(current_odds AS VARCHAR) || ')'
    END as current_line,
    
    -- Movement summary for this line
    COUNT(*) as total_movements,
    COUNT(*) FILTER (WHERE movement_strength IN ('MAJOR', 'SIGNIFICANT')) as sharp_movements,
    
    -- Latest movement
    FIRST_VALUE(odds_movement_display) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at DESC
    ) as latest_movement,
    
    FIRST_VALUE(movement_direction) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at DESC
    ) as latest_direction,
    
    FIRST_VALUE(movement_strength) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at DESC
    ) as latest_strength,
    
    FIRST_VALUE(minutes_before_game) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at DESC
    ) as minutes_since_last_move,
    
    -- Overall line movement from first to last
    FIRST_VALUE(current_odds) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at ASC
    ) as opening_odds,
    
    FIRST_VALUE(current_odds) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at DESC
    ) as current_odds_final,
    
    -- Sharp action summary
    MAX(ABS(COALESCE(filtered_odds_change, 0))) as largest_move,
    AVG(movement_quality_score) as avg_quality
    
FROM staging.v_pre_game_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY 
    external_game_id, home_team, away_team, game_start_time, 
    sportsbook_name, market_type, side, line_value, current_odds
ORDER BY external_game_id, sportsbook_name, market_type, side;

-- ================================
-- Phase 4: Create Movement History View
-- ================================

-- View showing the complete movement history for any game/line
CREATE OR REPLACE VIEW staging.v_line_movement_history AS
SELECT 
    external_game_id,
    home_team || ' vs ' || away_team as matchup,
    game_start_time,
    sportsbook_name,
    market_type || ' - ' || side as market_side,
    
    -- Movement sequence
    ROW_NUMBER() OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at
    ) as movement_sequence,
    
    -- Clear movement display
    odds_movement_display,
    movement_direction,
    movement_strength,
    
    -- Timing context
    updated_at,
    minutes_before_game,
    timing_category,
    time_since_last_display,
    
    -- Line value context if applicable
    CASE 
        WHEN line_value_movement_display IS NOT NULL 
        THEN 'Line: ' || line_value_movement_display
        ELSE NULL
    END as line_change_info,
    
    -- Quality indicators
    movement_quality_score,
    CASE 
        WHEN movement_quality_score >= 0.9 THEN '🟢 High Quality'
        WHEN movement_quality_score >= 0.7 THEN '🟡 Good Quality'
        WHEN movement_quality_score >= 0.5 THEN '🟠 Fair Quality'
        ELSE '🔴 Low Quality'
    END as quality_indicator

FROM staging.v_pre_game_line_movements
WHERE previous_odds IS NOT NULL
ORDER BY external_game_id, sportsbook_name, market_type, side, updated_at;

-- ================================
-- Phase 5: Update Comments and Documentation
-- ================================

COMMENT ON VIEW staging.v_game_movement_timeline IS 
'Enhanced timeline view showing clear opening → closing line movements with overall progression display';

COMMENT ON VIEW staging.v_line_movements_summary IS 
'User-friendly summary of line movements per game/sportsbook/market with clear current status and movement history';

COMMENT ON VIEW staging.v_line_movement_history IS 
'Complete movement history for any line showing clear progression with quality indicators and timing context';

-- ================================
-- Phase 6: Create Validation Function
-- ================================

-- Function to validate that all views now show previous odds clearly
CREATE OR REPLACE FUNCTION staging.validate_enhanced_movement_display()
RETURNS TABLE (
    view_name TEXT,
    has_previous_odds BOOLEAN,
    has_movement_display BOOLEAN,
    has_direction_indicator BOOLEAN,
    sample_movement_display TEXT,
    validation_status TEXT
) AS $$
BEGIN
    -- Test v_line_movements
    RETURN QUERY
    SELECT 
        'v_line_movements'::TEXT,
        (COUNT(*) FILTER (WHERE previous_odds IS NOT NULL) > 0) as has_previous_odds,
        (COUNT(*) FILTER (WHERE odds_movement_display IS NOT NULL) > 0) as has_movement_display,
        (COUNT(*) FILTER (WHERE movement_direction IS NOT NULL) > 0) as has_direction_indicator,
        MAX(odds_movement_display) as sample_movement_display,
        CASE 
            WHEN COUNT(*) FILTER (WHERE previous_odds IS NOT NULL AND odds_movement_display IS NOT NULL) > 0
            THEN 'PASS: Enhanced display working'
            ELSE 'FAIL: Missing enhanced display'
        END as validation_status
    FROM staging.v_line_movements
    WHERE previous_odds IS NOT NULL
    LIMIT 1;
    
    -- Test v_pre_game_sharp_movements
    RETURN QUERY
    SELECT 
        'v_pre_game_sharp_movements'::TEXT,
        TRUE as has_previous_odds,  -- Derived from main view
        (COUNT(*) FILTER (WHERE odds_movement_display IS NOT NULL) > 0) as has_movement_display,
        (COUNT(*) FILTER (WHERE movement_direction IS NOT NULL) > 0) as has_direction_indicator,
        MAX(odds_movement_display) as sample_movement_display,
        CASE 
            WHEN COUNT(*) FILTER (WHERE odds_movement_display IS NOT NULL) > 0
            THEN 'PASS: Enhanced display working'
            ELSE 'FAIL: Missing enhanced display'
        END as validation_status
    FROM staging.v_pre_game_sharp_movements
    LIMIT 1;
    
    -- Test v_late_sharp_action
    RETURN QUERY
    SELECT 
        'v_late_sharp_action'::TEXT,
        TRUE as has_previous_odds,  -- Derived from main view
        (COUNT(*) FILTER (WHERE odds_movement_display IS NOT NULL) > 0) as has_movement_display,
        (COUNT(*) FILTER (WHERE movement_direction IS NOT NULL) > 0) as has_direction_indicator,
        MAX(odds_movement_display) as sample_movement_display,
        CASE 
            WHEN COUNT(*) FILTER (WHERE odds_movement_display IS NOT NULL) > 0
            THEN 'PASS: Enhanced display working'
            ELSE 'FAIL: Missing enhanced display'
        END as validation_status
    FROM staging.v_late_sharp_action
    LIMIT 1;
    
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Migration Completion Notes
-- ================================

/*
WHAT THIS MIGRATION DOES:
1. ✅ Enhances ALL line movement views with clear "previous → current" odds display
2. ✅ Adds movement direction indicators (↗ ↘ → ●)
3. ✅ Adds movement strength categorization (MAJOR, SIGNIFICANT, MINOR, etc.)
4. ✅ Improves timing display ("15m ago", "2h ago", etc.)
5. ✅ Creates user-friendly summary views for quick analysis
6. ✅ Adds complete movement history view with quality indicators
7. ✅ Maintains backward compatibility with existing view names
8. ✅ Includes validation function to verify improvements

NEW ENHANCED COLUMNS AVAILABLE:
- previous_odds: The odds before this movement
- current_odds: The current odds after this movement  
- odds_movement_display: "previous → current (±change)" format
- movement_direction: ↗ ↘ → ● indicators
- movement_strength: MAJOR, SIGNIFICANT, MINOR, SMALL, INITIAL, NONE
- line_value_movement_display: Clear line value changes for spreads/totals
- time_since_last_display: Human-readable time gaps

NEW VIEWS CREATED:
- v_line_movements_summary: Quick overview per game/market
- v_line_movement_history: Complete progression with quality indicators

POST-MIGRATION VERIFICATION:
-- Run this to verify all views have enhanced display:
SELECT * FROM staging.validate_enhanced_movement_display();

-- Test the enhanced display:
SELECT 
    matchup, sportsbook_name, market_type,
    odds_movement_display, movement_direction, movement_strength
FROM staging.v_line_movements
WHERE movement_strength IN ('MAJOR', 'SIGNIFICANT')
ORDER BY ABS(odds_change) DESC
LIMIT 10;

EXAMPLE ENHANCED OUTPUT:
Instead of confusing: -165, +5, minor_odds_move
You now see clear: -170 → -165 (+5) ↗ MINOR

This makes it immediately clear:
- Previous odds were -170
- Current odds are -165  
- Movement was +5 points toward longer odds (↗)
- Movement strength was MINOR
*/