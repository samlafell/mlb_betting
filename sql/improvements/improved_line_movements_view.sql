-- =============================================================================
-- IMPROVED LINE MOVEMENTS VIEW
-- =============================================================================
-- Purpose: Fix American odds calculations and detect line value changes
-- Issues Fixed:
--   1. American odds: -101 to +101 should be 2 points, not 202
--   2. Line flips: -1.5 to +1.5 spread changes cause false large movements
--   3. Total changes: 8.5 to 8.0 changes cause false large movements
-- =============================================================================

-- Drop existing view to recreate with improvements
DROP VIEW IF EXISTS staging.v_line_movements CASCADE;

-- Create improved line movements view
CREATE OR REPLACE VIEW staging.v_line_movements AS
WITH movement_data AS (
    SELECT 
        external_game_id,
        sportsbook_name,
        market_type,
        side,
        line_value,
        odds,
        updated_at,
        
        -- Previous values for comparison
        LAG(odds) OVER (
            PARTITION BY external_game_id, sportsbook_name, market_type, side 
            ORDER BY updated_at
        ) as previous_odds,
        LAG(line_value) OVER (
            PARTITION BY external_game_id, sportsbook_name, market_type, side 
            ORDER BY updated_at
        ) as previous_line_value,
        LAG(updated_at) OVER (
            PARTITION BY external_game_id, sportsbook_name, market_type, side 
            ORDER BY updated_at  
        ) as previous_updated_at
    FROM staging.action_network_odds_historical
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
        )) as seconds_since_last_change
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
    previous_odds,
    previous_line_value,
    previous_updated_at,
    
    -- =============================================================================
    -- CORRECTED ODDS MOVEMENT
    -- =============================================================================
    odds_change_corrected as odds_change,
    odds_change_raw,  -- Keep for comparison/debugging
    
    -- =============================================================================  
    -- LINE VALUE CHANGES
    -- =============================================================================
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
ORDER BY external_game_id, sportsbook_name, market_type, side, updated_at;

-- =============================================================================
-- SHARP MOVEMENT DETECTION VIEW 
-- =============================================================================
-- Focus on high-quality movements that indicate sharp action
CREATE OR REPLACE VIEW staging.v_sharp_movements AS
SELECT *
FROM staging.v_line_movements
WHERE movement_quality_score >= 0.8  -- High quality movements only
  AND ABS(COALESCE(filtered_odds_change, 0)) >= 10  -- Significant movement
  AND movement_type NOT IN ('initial', 'no_change')
  AND seconds_since_last_change <= 3600  -- Within 1 hour
ORDER BY external_game_id, ABS(filtered_odds_change) DESC;

-- =============================================================================
-- LINE VALUE CHANGES VIEW
-- =============================================================================  
-- Track spread/total line value changes separately
CREATE OR REPLACE VIEW staging.v_line_value_changes AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    line_value,
    previous_line_value,
    line_value_change,
    odds,
    previous_odds, 
    odds_change,
    updated_at,
    previous_updated_at,
    seconds_since_last_change
FROM staging.v_line_movements
WHERE has_line_value_change = TRUE
  AND market_type IN ('spread', 'total')
ORDER BY external_game_id, sportsbook_name, market_type, updated_at;

-- =============================================================================
-- MOVEMENT SUMMARY VIEW
-- =============================================================================
-- Aggregate movement statistics per game/market/sportsbook
CREATE OR REPLACE VIEW staging.v_movement_summary AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    COUNT(*) as total_movements,
    COUNT(*) FILTER (WHERE movement_quality_score >= 0.8) as high_quality_movements,
    COUNT(*) FILTER (WHERE has_line_value_change) as line_value_changes,
    COUNT(*) FILTER (WHERE ABS(COALESCE(filtered_odds_change, 0)) >= 10) as significant_moves,
    COUNT(*) FILTER (WHERE ABS(COALESCE(filtered_odds_change, 0)) >= 20) as major_moves,
    MIN(updated_at) as first_movement,
    MAX(updated_at) as last_movement,
    MAX(ABS(COALESCE(filtered_odds_change, 0))) as largest_move,
    AVG(movement_quality_score) as avg_quality_score
FROM staging.v_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY external_game_id, sportsbook_name, market_type, side
ORDER BY external_game_id, sportsbook_name, market_type, side;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON VIEW staging.v_line_movements IS 
'Improved line movements with corrected American odds calculations and line value change detection';

COMMENT ON VIEW staging.v_sharp_movements IS 
'High-quality movements only, filtered for sharp action detection';

COMMENT ON VIEW staging.v_line_value_changes IS 
'Spread and total line value changes that may cause false movement signals';

COMMENT ON VIEW staging.v_movement_summary IS 
'Aggregate movement statistics per game/market/sportsbook combination';

-- =============================================================================
-- EXAMPLE QUERIES FOR VALIDATION
-- =============================================================================

/*
-- Example 1: Compare old vs new odds calculations
SELECT 
    external_game_id, sportsbook_name, side,
    previous_odds, odds,
    odds_change_raw as old_calculation,
    odds_change as new_calculation,
    odds_change_raw - odds_change as difference
FROM staging.v_line_movements 
WHERE previous_odds IS NOT NULL
  AND ABS(odds_change_raw - odds_change) > 10
ORDER BY ABS(odds_change_raw - odds_change) DESC;

-- Example 2: Find movements caused by line value changes  
SELECT * FROM staging.v_line_value_changes
WHERE ABS(line_value_change) >= 1.0
ORDER BY ABS(line_value_change) DESC;

-- Example 3: High-quality sharp movements
SELECT * FROM staging.v_sharp_movements
ORDER BY ABS(filtered_odds_change) DESC;

-- Example 4: Movement quality analysis
SELECT 
    movement_type,
    COUNT(*) as count,
    AVG(movement_quality_score) as avg_quality
FROM staging.v_line_movements
WHERE previous_odds IS NOT NULL
GROUP BY movement_type
ORDER BY count DESC;
*/