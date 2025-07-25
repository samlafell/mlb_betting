-- =============================================================================
-- ENHANCED LINE MOVEMENTS DISPLAY - IMPROVED READABILITY
-- =============================================================================
-- Purpose: Improve line movement views with clearer movement display
-- Changes:
--   1. Add clear "previous → current" odds display
--   2. Improve movement direction indicators
--   3. Add movement strength categorization
--   4. Better column ordering for readability
-- =============================================================================

-- Drop and recreate the pre-game view with enhanced display
DROP VIEW IF EXISTS staging.v_pre_game_line_movements CASCADE;

-- Create enhanced pre-game line movements view with improved display
CREATE OR REPLACE VIEW staging.v_pre_game_line_movements AS
WITH game_context AS (
    -- Get game start times and team info from raw data
    SELECT 
        external_game_id,
        start_time,
        game_date,
        home_team,
        away_team,
        game_status
    FROM raw_data.action_network_games
    WHERE start_time IS NOT NULL
),
movement_data AS (
    SELECT 
        h.external_game_id,
        h.sportsbook_name,
        h.market_type,
        h.side,
        h.line_value,
        h.odds,
        h.updated_at,
        
        -- Game context
        g.start_time as game_start_time,
        g.game_date,
        g.home_team,
        g.away_team,
        g.game_status,
        
        -- Calculate time until game start (in minutes)
        EXTRACT(EPOCH FROM (g.start_time - h.updated_at)) / 60.0 as minutes_before_game,
        
        -- Previous values for comparison
        LAG(h.odds) OVER (
            PARTITION BY h.external_game_id, h.sportsbook_name, h.market_type, h.side 
            ORDER BY h.updated_at
        ) as previous_odds,
        LAG(h.line_value) OVER (
            PARTITION BY h.external_game_id, h.sportsbook_name, h.market_type, h.side 
            ORDER BY h.updated_at
        ) as previous_line_value,
        LAG(h.updated_at) OVER (
            PARTITION BY h.external_game_id, h.sportsbook_name, h.market_type, h.side 
            ORDER BY h.updated_at  
        ) as previous_updated_at
    FROM staging.action_network_odds_historical h
    INNER JOIN game_context g ON h.external_game_id = g.external_game_id
    -- Only pre-game movements
    WHERE h.updated_at < g.start_time  
      AND h.updated_at >= g.start_time - INTERVAL '7 days'
),
calculated_movements AS (
    SELECT *,
        -- =============================================================================
        -- IMPROVED AMERICAN ODDS CALCULATION
        -- =============================================================================
        CASE 
            WHEN previous_odds IS NULL THEN NULL
            -- Both odds same sign: simple difference
            WHEN (previous_odds > 0 AND odds > 0) OR (previous_odds < 0 AND odds < 0) THEN
                odds - previous_odds
            -- Crossing zero: special handling for American odds
            WHEN previous_odds < 0 AND odds > 0 THEN 
                (odds - 100) + (100 - ABS(previous_odds))
            WHEN previous_odds > 0 AND odds < 0 THEN
                -((ABS(odds) - 100) + (100 - previous_odds))
            ELSE 
                odds - previous_odds
        END as odds_change_corrected,
        
        -- Raw difference for comparison
        odds - previous_odds as odds_change_raw,
        
        -- =============================================================================
        -- LINE VALUE CHANGE DETECTION
        -- =============================================================================
        CASE 
            WHEN previous_line_value IS NULL THEN NULL
            ELSE line_value - previous_line_value
        END as line_value_change,
        
        CASE 
            WHEN previous_line_value IS NULL THEN FALSE
            WHEN market_type IN ('spread', 'total') AND ABS(line_value - previous_line_value) >= 0.5 THEN TRUE
            ELSE FALSE
        END as has_line_value_change,
        
        -- Time between movements
        EXTRACT(EPOCH FROM (
            updated_at - previous_updated_at
        )) as seconds_since_last_change,
        
        -- =============================================================================
        -- GAME TIMING ANALYSIS
        -- =============================================================================
        CASE 
            WHEN minutes_before_game > 1440 THEN 'early_week'      -- >24 hours
            WHEN minutes_before_game > 360 THEN 'day_before'       -- 6-24 hours  
            WHEN minutes_before_game > 60 THEN 'hours_before'      -- 1-6 hours
            WHEN minutes_before_game > 15 THEN 'late_pregame'      -- 15min-1hour
            WHEN minutes_before_game > 0 THEN 'very_late'          -- 0-15 minutes
            ELSE 'post_game'  -- This should not happen due to WHERE filter
        END as timing_category
    FROM movement_data
)
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    line_value,
    
    -- =============================================================================
    -- ENHANCED ODDS DISPLAY - CLEARER MOVEMENT VISUALIZATION
    -- =============================================================================
    previous_odds,
    odds as current_odds,
    
    -- Clear movement display: "prev → curr (±change)"
    CASE 
        WHEN previous_odds IS NULL THEN 
            CAST(odds AS VARCHAR) || ' (initial)'
        ELSE 
            CAST(previous_odds AS VARCHAR) || ' → ' || CAST(odds AS VARCHAR) || 
            CASE 
                WHEN odds_change_corrected > 0 THEN ' (+' || CAST(odds_change_corrected AS VARCHAR) || ')'
                WHEN odds_change_corrected < 0 THEN ' (' || CAST(odds_change_corrected AS VARCHAR) || ')'
                ELSE ' (no change)'
            END
    END as odds_movement_display,
    
    -- Movement direction indicator
    CASE 
        WHEN previous_odds IS NULL THEN '●'  -- Initial line
        WHEN odds_change_corrected > 0 THEN '↗'   -- Odds increased (longer odds)
        WHEN odds_change_corrected < 0 THEN '↘'   -- Odds decreased (shorter odds)
        ELSE '→'  -- No change
    END as movement_direction,
    
    -- Movement strength indicator
    CASE 
        WHEN previous_odds IS NULL THEN 'INITIAL'
        WHEN ABS(odds_change_corrected) >= 20 THEN 'MAJOR'
        WHEN ABS(odds_change_corrected) >= 10 THEN 'SIGNIFICANT'  
        WHEN ABS(odds_change_corrected) >= 5 THEN 'MINOR'
        WHEN ABS(odds_change_corrected) > 0 THEN 'SMALL'
        ELSE 'NONE'
    END as movement_strength,
    
    -- Numeric values for analysis
    odds_change_corrected as odds_change,
    odds_change_raw,
    
    -- =============================================================================
    -- LINE VALUE CHANGES (ENHANCED DISPLAY)
    -- =============================================================================
    previous_line_value,
    CASE 
        WHEN previous_line_value IS NULL AND line_value IS NOT NULL THEN 
            CAST(line_value AS VARCHAR) || ' (initial)'
        WHEN previous_line_value IS NOT NULL AND line_value IS NOT NULL THEN
            CAST(previous_line_value AS VARCHAR) || ' → ' || CAST(line_value AS VARCHAR) ||
            CASE 
                WHEN line_value_change > 0 THEN ' (+' || CAST(line_value_change AS VARCHAR) || ')'
                WHEN line_value_change < 0 THEN ' (' || CAST(line_value_change AS VARCHAR) || ')'
                ELSE ' (no change)'
            END
        ELSE NULL
    END as line_value_movement_display,
    
    line_value_change,
    has_line_value_change,
    
    -- =============================================================================
    -- TIMING AND CONTEXT
    -- =============================================================================
    updated_at,
    previous_updated_at,
    minutes_before_game,
    timing_category,
    
    -- Time gap display
    CASE 
        WHEN seconds_since_last_change IS NULL THEN 'First movement'
        WHEN seconds_since_last_change < 60 THEN CAST(ROUND(seconds_since_last_change) AS VARCHAR) || 's ago'
        WHEN seconds_since_last_change < 3600 THEN CAST(ROUND(seconds_since_last_change/60.0) AS VARCHAR) || 'm ago'
        ELSE CAST(ROUND(seconds_since_last_change/3600.0, 1) AS VARCHAR) || 'h ago'
    END as time_since_last_display,
    
    -- =============================================================================
    -- MOVEMENT CLASSIFICATION AND QUALITY
    -- =============================================================================
    CASE 
        WHEN previous_odds IS NULL THEN 'initial'
        WHEN has_line_value_change THEN 'line_change'
        WHEN ABS(odds_change_corrected) >= 20 THEN 'major_odds_move'
        WHEN ABS(odds_change_corrected) >= 10 THEN 'significant_odds_move'  
        WHEN ABS(odds_change_corrected) >= 5 THEN 'minor_odds_move'
        WHEN ABS(odds_change_corrected) > 0 THEN 'small_odds_move'
        ELSE 'no_change'
    END as movement_type,
    
    -- Filtered movement (excluding false positives)
    CASE 
        WHEN previous_odds IS NULL THEN NULL
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 50 THEN NULL  
        ELSE odds_change_corrected
    END as filtered_odds_change,
    
    -- Movement quality score
    CASE 
        WHEN previous_odds IS NULL THEN NULL
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 50 THEN 0.1
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 20 THEN 0.3
        WHEN has_line_value_change THEN 0.7
        WHEN ABS(odds_change_corrected) > 100 THEN 0.2
        WHEN ABS(odds_change_corrected) >= 20 THEN 1.0
        WHEN ABS(odds_change_corrected) >= 5 THEN 0.9
        ELSE 0.8
    END as movement_quality_score,
    
    seconds_since_last_change

FROM calculated_movements
WHERE minutes_before_game > 0
ORDER BY external_game_id, sportsbook_name, market_type, side, updated_at;

-- =============================================================================
-- UPDATE COMPATIBILITY VIEW WITH ENHANCED DISPLAY
-- =============================================================================
DROP VIEW IF EXISTS staging.v_line_movements CASCADE;
CREATE OR REPLACE VIEW staging.v_line_movements AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    line_value,
    
    -- Enhanced odds display
    previous_odds,
    current_odds,
    odds_movement_display,
    movement_direction,
    movement_strength,
    
    -- Line value display
    previous_line_value,
    line_value_movement_display,
    
    -- Timing
    updated_at,
    minutes_before_game,
    timing_category,
    time_since_last_display,
    
    -- Analysis values
    odds_change,
    line_value_change,
    movement_type,
    filtered_odds_change,
    movement_quality_score
    
FROM staging.v_pre_game_line_movements;

-- =============================================================================
-- ENHANCED SHARP MOVEMENTS VIEW
-- =============================================================================
CREATE OR REPLACE VIEW staging.v_pre_game_sharp_movements AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Clear movement display
    odds_movement_display,
    movement_direction,
    movement_strength,
    line_value_movement_display,
    
    -- Timing
    updated_at,
    minutes_before_game,
    timing_category,
    time_since_last_display,
    
    -- Analysis
    odds_change,
    filtered_odds_change,
    movement_quality_score,
    movement_type
    
FROM staging.v_pre_game_line_movements
WHERE movement_quality_score >= 0.8
  AND ABS(COALESCE(filtered_odds_change, 0)) >= 10
  AND movement_type NOT IN ('initial', 'no_change')
  AND seconds_since_last_change <= 3600
  AND timing_category IN ('late_pregame', 'very_late', 'hours_before')
ORDER BY external_game_id, ABS(filtered_odds_change) DESC;

-- =============================================================================
-- ENHANCED LATE SHARP ACTION VIEW
-- =============================================================================
CREATE OR REPLACE VIEW staging.v_late_sharp_action AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Enhanced display
    odds_movement_display,
    movement_direction,
    movement_strength,
    
    -- Timing context
    minutes_before_game,
    time_since_last_display,
    
    -- Sharp action metrics
    odds_change,
    filtered_odds_change,
    movement_quality_score,
    LEAST(10.0, ABS(COALESCE(filtered_odds_change, 0)) / 5.0) as sharp_intensity_score
    
FROM staging.v_pre_game_line_movements
WHERE minutes_before_game <= 60
  AND movement_quality_score >= 0.8
  AND ABS(COALESCE(filtered_odds_change, 0)) >= 5
  AND movement_type NOT IN ('initial', 'no_change')
ORDER BY external_game_id, minutes_before_game ASC, ABS(filtered_odds_change) DESC;

-- =============================================================================
-- COMPATIBILITY VIEWS FOR EXISTING QUERIES
-- =============================================================================

-- Update sharp movements view (legacy name)
DROP VIEW IF EXISTS staging.v_sharp_movements CASCADE;
CREATE OR REPLACE VIEW staging.v_sharp_movements AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    line_value,
    current_odds as odds,
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

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON VIEW staging.v_pre_game_line_movements IS 
'Enhanced pre-game line movements with clear "previous → current" odds display and movement indicators';

COMMENT ON VIEW staging.v_line_movements IS 
'User-friendly line movements view with enhanced movement display - shows previous odds clearly';

COMMENT ON VIEW staging.v_pre_game_sharp_movements IS 
'Enhanced sharp movements with clear movement visualization and timing context';

COMMENT ON VIEW staging.v_late_sharp_action IS 
'Late sharp action with enhanced display showing clear odds movements in final hour';

-- =============================================================================
-- EXAMPLE ENHANCED QUERIES
-- =============================================================================

/*
-- Example 1: See clear movement display for recent games
SELECT 
    home_team, away_team, sportsbook_name, market_type, side,
    odds_movement_display, movement_direction, movement_strength,
    minutes_before_game, timing_category
FROM staging.v_line_movements
WHERE game_start_time >= CURRENT_DATE
  AND movement_strength IN ('MAJOR', 'SIGNIFICANT')
ORDER BY ABS(odds_change) DESC;

-- Example 2: Track a specific game's line movement progression
SELECT 
    sportsbook_name, market_type, side,
    odds_movement_display, line_value_movement_display,
    timing_category, time_since_last_display
FROM staging.v_line_movements
WHERE external_game_id = 'YOUR_GAME_ID'
ORDER BY updated_at;

-- Example 3: Late sharp action with clear display
SELECT 
    home_team, away_team, sportsbook_name,
    odds_movement_display, movement_direction,
    minutes_before_game, sharp_intensity_score
FROM staging.v_late_sharp_action
WHERE game_start_time >= CURRENT_DATE
ORDER BY sharp_intensity_score DESC;
*/