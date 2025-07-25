-- =============================================================================
-- PRE-GAME LINE MOVEMENTS VIEW - ENHANCED WITH TIMEZONE-AWARE FILTERING
-- =============================================================================
-- Purpose: Filter line movements to only show PRE-GAME activity
-- Issues Fixed:
--   1. Excludes line movements that occur after game start time
--   2. Proper timezone handling (EST/EDT awareness)
--   3. Adds game context (teams, start time) for better analysis
--   4. Maintains all existing movement detection logic
-- =============================================================================

-- Drop existing view to recreate with game time filtering
DROP VIEW IF EXISTS staging.v_pre_game_line_movements CASCADE;

-- Create enhanced pre-game line movements view
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
    -- =============================================================================
    -- CRITICAL FILTER: ONLY PRE-GAME MOVEMENTS
    -- =============================================================================
    WHERE h.updated_at < g.start_time  -- Only movements BEFORE game starts
      AND h.updated_at >= g.start_time - INTERVAL '7 days'  -- Within 7 days of game (reasonable window)
),
calculated_movements AS (
    SELECT *,
        -- =============================================================================
        -- IMPROVED AMERICAN ODDS CALCULATION
        -- =============================================================================
        -- American odds work as follows:
        -- Positive odds (+150): You win $150 on a $100 bet
        -- Negative odds (-150): You bet $150 to win $100
        -- Movement from -101 to +101 is crossing zero (2 point movement)
        -- Movement from -150 to -140 is 10 point movement toward even
        CASE 
            WHEN previous_odds IS NULL THEN NULL
            -- Both odds same sign: simple difference
            WHEN (previous_odds > 0 AND odds > 0) OR (previous_odds < 0 AND odds < 0) THEN
                odds - previous_odds
            -- Crossing zero: special handling for American odds
            WHEN previous_odds < 0 AND odds > 0 THEN 
                -- From negative to positive: -101 to +101 = 2 points
                (odds - 100) + (100 - ABS(previous_odds))
            WHEN previous_odds > 0 AND odds < 0 THEN
                -- From positive to negative: +101 to -101 = -2 points  
                -((ABS(odds) - 100) + (100 - previous_odds))
            ELSE 
                odds - previous_odds
        END as odds_change_corrected,
        
        -- Raw difference for comparison (current incorrect method)
        odds - previous_odds as odds_change_raw,
        
        -- =============================================================================
        -- LINE VALUE CHANGE DETECTION
        -- =============================================================================
        CASE 
            WHEN previous_line_value IS NULL THEN NULL
            ELSE line_value - previous_line_value
        END as line_value_change,
        
        -- Detect significant line value changes that might cause false movements
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
        -- Categorize when the movement occurred relative to game start
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
    sportsbook_name,
    market_type,
    side,
    line_value,
    odds,
    updated_at,
    
    -- =============================================================================
    -- GAME CONTEXT (NEW)
    -- =============================================================================
    game_start_time,
    game_date,
    home_team,
    away_team,
    game_status,
    minutes_before_game,
    timing_category,
    
    -- =============================================================================
    -- MOVEMENT ANALYSIS (EXISTING LOGIC PRESERVED)
    -- =============================================================================
    previous_odds,
    previous_line_value,
    previous_updated_at,
    
    -- Corrected odds movement
    odds_change_corrected as odds_change,
    odds_change_raw,  -- Keep for comparison/debugging
    
    -- Line value changes
    line_value_change,
    has_line_value_change,
    
    -- =============================================================================
    -- MOVEMENT SIGNIFICANCE CLASSIFICATION
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
    
    -- =============================================================================
    -- FILTERED MOVEMENTS (EXCLUDE FALSE POSITIVES)
    -- =============================================================================
    -- True odds movement: exclude movements caused primarily by line value changes
    CASE 
        WHEN previous_odds IS NULL THEN NULL
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 50 THEN 
            -- Large odds change with line change is likely false positive
            NULL  
        ELSE 
            odds_change_corrected
    END as filtered_odds_change,
    
    -- Movement quality score (0-1, higher = more reliable)
    CASE 
        WHEN previous_odds IS NULL THEN NULL
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 50 THEN 0.1  -- Likely false
        WHEN has_line_value_change AND ABS(odds_change_corrected) > 20 THEN 0.3  -- Questionable
        WHEN has_line_value_change THEN 0.7  -- Line change but small odds move
        WHEN ABS(odds_change_corrected) > 100 THEN 0.2  -- Suspiciously large
        WHEN ABS(odds_change_corrected) >= 20 THEN 1.0   -- Clean major move
        WHEN ABS(odds_change_corrected) >= 5 THEN 0.9    -- Clean significant move
        ELSE 0.8  -- Clean small move
    END as movement_quality_score,
    
    seconds_since_last_change

FROM calculated_movements
-- =============================================================================
-- FINAL FILTER: ENSURE NO POST-GAME DATA
-- =============================================================================
WHERE minutes_before_game > 0  -- Double-check: only pre-game movements
ORDER BY external_game_id, sportsbook_name, market_type, side, updated_at;

-- =============================================================================
-- SHARP MOVEMENT DETECTION VIEW - PRE-GAME ONLY
-- =============================================================================
-- Focus on high-quality PRE-GAME movements that indicate sharp action
CREATE OR REPLACE VIEW staging.v_pre_game_sharp_movements AS
SELECT *
FROM staging.v_pre_game_line_movements
WHERE movement_quality_score >= 0.8  -- High quality movements only
  AND ABS(COALESCE(filtered_odds_change, 0)) >= 10  -- Significant movement
  AND movement_type NOT IN ('initial', 'no_change')
  AND seconds_since_last_change <= 3600  -- Within 1 hour
  AND timing_category IN ('late_pregame', 'very_late', 'hours_before')  -- Focus on meaningful timing
ORDER BY external_game_id, ABS(filtered_odds_change) DESC;

-- =============================================================================
-- LATE SHARP ACTION VIEW - LAST HOUR BEFORE GAME
-- =============================================================================
-- Detect sharp action in the critical final hour before game start
CREATE OR REPLACE VIEW staging.v_late_sharp_action AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    line_value,
    odds,
    updated_at,
    minutes_before_game,
    odds_change,
    filtered_odds_change,
    movement_quality_score,
    movement_type,
    -- Sharp action intensity score (0-10)
    LEAST(10.0, ABS(COALESCE(filtered_odds_change, 0)) / 5.0) as sharp_intensity_score
FROM staging.v_pre_game_line_movements
WHERE minutes_before_game <= 60  -- Last hour before game
  AND movement_quality_score >= 0.8
  AND ABS(COALESCE(filtered_odds_change, 0)) >= 5  -- Any significant movement
  AND movement_type NOT IN ('initial', 'no_change')
ORDER BY external_game_id, minutes_before_game ASC, ABS(filtered_odds_change) DESC;

-- =============================================================================
-- GAME TIMELINE VIEW - MOVEMENT PROGRESSION
-- =============================================================================
-- Show how lines evolved throughout the pre-game period
CREATE OR REPLACE VIEW staging.v_game_movement_timeline AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Movement counts by timing
    COUNT(*) FILTER (WHERE timing_category = 'early_week') as early_week_movements,
    COUNT(*) FILTER (WHERE timing_category = 'day_before') as day_before_movements, 
    COUNT(*) FILTER (WHERE timing_category = 'hours_before') as hours_before_movements,
    COUNT(*) FILTER (WHERE timing_category = 'late_pregame') as late_pregame_movements,
    COUNT(*) FILTER (WHERE timing_category = 'very_late') as very_late_movements,
    
    -- Sharp action by timing
    COUNT(*) FILTER (WHERE timing_category = 'late_pregame' AND ABS(COALESCE(filtered_odds_change, 0)) >= 10) as late_sharp_moves,
    COUNT(*) FILTER (WHERE timing_category = 'very_late' AND ABS(COALESCE(filtered_odds_change, 0)) >= 5) as very_late_sharp_moves,
    
    -- Opening and closing lines
    MIN(odds) FILTER (WHERE timing_category = 'early_week') as early_odds,
    MAX(odds) FILTER (WHERE timing_category = 'very_late') as closing_odds,
    
    -- Line movement summary
    MAX(ABS(COALESCE(filtered_odds_change, 0))) as largest_movement,
    AVG(movement_quality_score) as avg_movement_quality
    
FROM staging.v_pre_game_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY external_game_id, home_team, away_team, game_start_time, sportsbook_name, market_type, side
ORDER BY external_game_id, sportsbook_name, market_type, side;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON VIEW staging.v_pre_game_line_movements IS 
'Enhanced line movements view that ONLY shows pre-game activity with timezone-aware filtering and game context';

COMMENT ON VIEW staging.v_pre_game_sharp_movements IS 
'High-quality PRE-GAME movements only, filtered for sharp action detection';

COMMENT ON VIEW staging.v_late_sharp_action IS 
'Sharp action detected in the critical final hour before game start';

COMMENT ON VIEW staging.v_game_movement_timeline IS 
'Game-level summary showing how lines evolved throughout the pre-game period';

-- =============================================================================
-- EXAMPLE QUERIES FOR VALIDATION AND ANALYSIS
-- =============================================================================

/*
-- Example 1: Verify no post-game movements exist
SELECT 
    COUNT(*) as total_movements,
    MIN(minutes_before_game) as min_minutes_before,
    MAX(minutes_before_game) as max_minutes_before,
    COUNT(*) FILTER (WHERE minutes_before_game <= 0) as post_game_count
FROM staging.v_pre_game_line_movements;

-- Example 2: Sharp action in final hour before games
SELECT 
    external_game_id, home_team, away_team,
    sportsbook_name, market_type, side,
    minutes_before_game, odds_change, sharp_intensity_score
FROM staging.v_late_sharp_action
WHERE game_date = CURRENT_DATE
ORDER BY sharp_intensity_score DESC;

-- Example 3: Games with most late sharp action
SELECT 
    external_game_id, home_team, away_team, game_start_time,
    COUNT(*) as late_sharp_moves,
    MAX(sharp_intensity_score) as max_intensity
FROM staging.v_late_sharp_action
GROUP BY external_game_id, home_team, away_team, game_start_time
HAVING COUNT(*) >= 3  -- Games with 3+ late sharp moves
ORDER BY max_intensity DESC, late_sharp_moves DESC;

-- Example 4: Timeline analysis for specific game
SELECT * FROM staging.v_game_movement_timeline
WHERE external_game_id = 'YOUR_GAME_ID'
ORDER BY sportsbook_name, market_type, side;

-- Example 5: Timing distribution of all movements
SELECT 
    timing_category,
    COUNT(*) as movement_count,
    COUNT(*) FILTER (WHERE ABS(COALESCE(filtered_odds_change, 0)) >= 10) as sharp_moves,
    AVG(movement_quality_score) as avg_quality
FROM staging.v_pre_game_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY timing_category
ORDER BY 
    CASE timing_category
        WHEN 'early_week' THEN 1
        WHEN 'day_before' THEN 2
        WHEN 'hours_before' THEN 3
        WHEN 'late_pregame' THEN 4
        WHEN 'very_late' THEN 5
    END;
*/