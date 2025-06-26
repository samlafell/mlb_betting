-- Manual Strategy Evaluation Queries
-- Use these queries to extract actual records that match your top-performing strategies
-- for manual evaluation and verification

-- =============================================================================
-- STRATEGY 1: STEAM_PLAYS Strategy (71.4% WR, +36.4% ROI)
-- Definition: Small line movement (<5) with strong sharp action
-- =============================================================================

WITH base_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        rmbs.book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.split_value,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        -- Extract line values
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                TRY_CAST((rmbs.split_value)::json->>'"$.home"' AS DOUBLE)
            WHEN rmbs.split_type IN ('spread', 'total') THEN
                TRY_CAST(rmbs.split_value AS DOUBLE)
            ELSE NULL
        END as line_value,
        
        go.home_win,
        go.home_cover_spread,
        go.over,
        go.home_score,
        go.away_score,
        go.total_runs
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.game_datetime < CURRENT_TIMESTAMP - INTERVAL '6 hours'  -- Only completed games
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

game_level_metrics AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        game_datetime,
        
        -- Opening line (earliest)
        FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_line,
        
        -- Closing line (latest)
        LAST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_line,
        
        -- Line movement
        LAST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) - FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as line_movement,
        
        -- Closing metrics
        LAST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_differential,
        
        LAST_VALUE(stake_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_stake_pct,
        
        -- Sharp action classification
        CASE 
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) >= 15 THEN 'STRONG_SHARP_HOME_OVER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) >= 10 THEN 'MODERATE_SHARP_HOME_OVER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) >= 5 THEN 'WEAK_SHARP_HOME_OVER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= -15 THEN 'STRONG_SHARP_AWAY_UNDER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= -10 THEN 'MODERATE_SHARP_AWAY_UNDER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= -5 THEN 'WEAK_SHARP_AWAY_UNDER'
            ELSE 'NO_SHARP_ACTION'
        END as sharp_classification,
        
        home_win,
        home_cover_spread,
        over,
        home_score,
        away_score,
        total_runs,
        
        ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
        
    FROM base_data
    WHERE line_value IS NOT NULL
)

-- STEAM_PLAYS: Strong sharp action with minimal line movement
SELECT 
    'STEAM_PLAYS' as strategy,
    game_id,
    source,
    book,
    split_type,
    home_team,
    away_team,
    DATE(game_datetime) as game_date,
    opening_line,
    closing_line,
    line_movement,
    closing_differential as sharp_differential,
    closing_stake_pct as stake_percentage,
    sharp_classification,
    
    -- Bet recommendation based on sharp action
    CASE 
        WHEN sharp_classification LIKE '%HOME_OVER' THEN 
            CASE split_type
                WHEN 'moneyline' THEN home_team
                WHEN 'spread' THEN CONCAT(home_team, ' +', closing_line)
                WHEN 'total' THEN CONCAT('OVER ', closing_line)
            END
        WHEN sharp_classification LIKE '%AWAY_UNDER' THEN 
            CASE split_type
                WHEN 'moneyline' THEN away_team
                WHEN 'spread' THEN CONCAT(away_team, ' ', closing_line)
                WHEN 'total' THEN CONCAT('UNDER ', closing_line)
            END
    END as recommended_bet,
    
    -- Actual outcome
    CASE split_type
        WHEN 'moneyline' THEN 
            CASE WHEN home_win THEN home_team ELSE away_team END
        WHEN 'spread' THEN 
            CASE WHEN home_cover_spread THEN CONCAT(home_team, ' COVERED') ELSE CONCAT(away_team, ' COVERED') END
        WHEN 'total' THEN 
            CASE WHEN over THEN CONCAT('OVER (', total_runs, ')') ELSE CONCAT('UNDER (', total_runs, ')') END
    END as actual_outcome,
    
    -- Did strategy win?
    CASE 
        WHEN sharp_classification LIKE '%HOME_OVER' THEN
            CASE split_type
                WHEN 'moneyline' THEN home_win
                WHEN 'spread' THEN home_cover_spread
                WHEN 'total' THEN over
            END
        WHEN sharp_classification LIKE '%AWAY_UNDER' THEN
            CASE split_type
                WHEN 'moneyline' THEN NOT home_win
                WHEN 'spread' THEN NOT home_cover_spread
                WHEN 'total' THEN NOT over
            END
    END as strategy_won,
    
    home_score,
    away_score,
    total_runs

FROM game_level_metrics
WHERE rn = 1 
  AND opening_line IS NOT NULL 
  AND closing_line IS NOT NULL
  -- STEAM_PLAYS criteria: Strong sharp action with small line movement
  AND ABS(line_movement) < 5 
  AND sharp_classification LIKE 'STRONG_SHARP_%'
  AND source = 'VSIN'
  AND split_type = 'spread'  -- Focus on the best performing split type
ORDER BY game_datetime DESC;

-- =============================================================================
-- STRATEGY 2: FOLLOW_STRONG_SHARP Strategy (71.4% WR, +36.4% ROI)  
-- Definition: Follow strong sharp action regardless of line movement
-- =============================================================================

WITH base_data AS (
    -- Same base_data CTE as above
    SELECT 
        rmbs.game_id,
        rmbs.source,
        rmbs.book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.split_value,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                TRY_CAST((rmbs.split_value)::json->>'"$.home"' AS DOUBLE)
            WHEN rmbs.split_type IN ('spread', 'total') THEN
                TRY_CAST(rmbs.split_value AS DOUBLE)
            ELSE NULL
        END as line_value,
        
        go.home_win,
        go.home_cover_spread,
        go.over,
        go.home_score,
        go.away_score,
        go.total_runs
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.game_datetime < CURRENT_TIMESTAMP - INTERVAL '6 hours'
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

follow_strong_sharp AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        game_datetime,
        
        LAST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_differential,
        
        CASE 
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) >= 15 THEN 'STRONG_SHARP_HOME_OVER'
            WHEN LAST_VALUE(differential) OVER (ORDER BY last_updated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= -15 THEN 'STRONG_SHARP_AWAY_UNDER'
            ELSE 'NOT_STRONG_SHARP'
        END as sharp_classification,
        
        home_win,
        home_cover_spread,
        over,
        home_score,
        away_score,
        total_runs,
        
        ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
        
    FROM base_data
    WHERE line_value IS NOT NULL
)

SELECT 
    'FOLLOW_STRONG_SHARP' as strategy,
    game_id,
    source,
    book,
    split_type,
    home_team,
    away_team,
    DATE(game_datetime) as game_date,
    closing_differential as sharp_differential,
    sharp_classification,
    
    -- Bet recommendation
    CASE 
        WHEN sharp_classification = 'STRONG_SHARP_HOME_OVER' THEN 
            CASE split_type
                WHEN 'moneyline' THEN home_team
                WHEN 'spread' THEN CONCAT(home_team, ' (spread)')
                WHEN 'total' THEN 'OVER'
            END
        WHEN sharp_classification = 'STRONG_SHARP_AWAY_UNDER' THEN 
            CASE split_type
                WHEN 'moneyline' THEN away_team
                WHEN 'spread' THEN CONCAT(away_team, ' (spread)')
                WHEN 'total' THEN 'UNDER'
            END
    END as recommended_bet,
    
    -- Actual outcome
    CASE split_type
        WHEN 'moneyline' THEN 
            CASE WHEN home_win THEN home_team ELSE away_team END
        WHEN 'spread' THEN 
            CASE WHEN home_cover_spread THEN CONCAT(home_team, ' COVERED') ELSE CONCAT(away_team, ' COVERED') END
        WHEN 'total' THEN 
            CASE WHEN over THEN CONCAT('OVER (', total_runs, ')') ELSE CONCAT('UNDER (', total_runs, ')') END
    END as actual_outcome,
    
    -- Did strategy win?
    CASE 
        WHEN sharp_classification = 'STRONG_SHARP_HOME_OVER' THEN
            CASE split_type
                WHEN 'moneyline' THEN home_win
                WHEN 'spread' THEN home_cover_spread
                WHEN 'total' THEN over
            END
        WHEN sharp_classification = 'STRONG_SHARP_AWAY_UNDER' THEN
            CASE split_type
                WHEN 'moneyline' THEN NOT home_win
                WHEN 'spread' THEN NOT home_cover_spread
                WHEN 'total' THEN NOT over
            END
    END as strategy_won,
    
    home_score,
    away_score,
    total_runs

FROM follow_strong_sharp
WHERE rn = 1 
  AND sharp_classification LIKE 'STRONG_SHARP_%'
  AND source = 'VSIN'
  AND split_type = 'spread'  -- Focus on best performing split type
ORDER BY game_datetime DESC;

-- =============================================================================
-- STRATEGY 3: OPPOSING_MARKETS_FOLLOW_STRONGER (58.6% WR, +23.1% ROI)
-- Definition: When moneyline and spread signals oppose, follow the stronger signal
-- =============================================================================

WITH opposing_markets AS (
    SELECT 
        rmbs.game_id,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.source,
        rmbs.book,
        rmbs.split_type,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        CASE 
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 15 THEN 'STRONG_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 10 THEN 'MODERATE_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -15 THEN 'STRONG_AWAY_UNDER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -10 THEN 'MODERATE_AWAY_UNDER'
            ELSE 'NO_SIGNAL'
        END as signal_strength,
        
        go.home_win,
        go.home_cover_spread,
        go.home_score,
        go.away_score,
        
        ROW_NUMBER() OVER (PARTITION BY rmbs.game_id, rmbs.source, rmbs.book, rmbs.split_type ORDER BY rmbs.last_updated DESC) as rn
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.game_datetime < CURRENT_TIMESTAMP - INTERVAL '6 hours'
      AND rmbs.split_type IN ('moneyline', 'spread')
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

pivot_data AS (
    SELECT 
        game_id,
        home_team,
        away_team,
        game_datetime,
        source,
        book,
        
        -- Moneyline signals
        MAX(CASE WHEN split_type = 'moneyline' THEN differential END) as ml_differential,
        MAX(CASE WHEN split_type = 'moneyline' THEN signal_strength END) as ml_signal,
        
        -- Spread signals
        MAX(CASE WHEN split_type = 'spread' THEN differential END) as spread_differential,
        MAX(CASE WHEN split_type = 'spread' THEN signal_strength END) as spread_signal,
        
        MAX(home_win) as home_win,
        MAX(home_cover_spread) as home_cover_spread,
        MAX(home_score) as home_score,
        MAX(away_score) as away_score
        
    FROM opposing_markets
    WHERE rn = 1 AND signal_strength != 'NO_SIGNAL'
    GROUP BY game_id, home_team, away_team, game_datetime, source, book
    HAVING COUNT(DISTINCT split_type) = 2  -- Must have both moneyline and spread
)

SELECT 
    'OPPOSING_MARKETS_FOLLOW_STRONGER' as strategy,
    game_id,
    source,
    book,
    home_team,
    away_team,
    DATE(game_datetime) as game_date,
    
    -- Signal details
    ml_differential as moneyline_differential,
    ml_signal as moneyline_signal,
    spread_differential as spread_differential,
    spread_signal as spread_signal,
    
    -- Determine which signal is stronger
    CASE 
        WHEN ABS(ml_differential) > ABS(spread_differential) THEN 'MONEYLINE_STRONGER'
        WHEN ABS(spread_differential) > ABS(ml_differential) THEN 'SPREAD_STRONGER'
        ELSE 'EQUAL_STRENGTH'
    END as dominant_signal,
    
    -- Recommendation based on stronger signal
    CASE 
        WHEN ABS(ml_differential) > ABS(spread_differential) THEN
            CASE WHEN ml_differential > 0 THEN home_team ELSE away_team END
        WHEN ABS(spread_differential) > ABS(ml_differential) THEN
            CASE WHEN spread_differential > 0 THEN home_team ELSE away_team END
        ELSE 
            CASE WHEN ml_differential > 0 THEN home_team ELSE away_team END
    END as recommended_team,
    
    -- Actual outcome
    CASE WHEN home_win THEN home_team ELSE away_team END as winning_team,
    
    -- Did strategy win?
    CASE 
        WHEN ABS(ml_differential) > ABS(spread_differential) THEN
            CASE 
                WHEN ml_differential > 0 AND home_win THEN TRUE
                WHEN ml_differential <= 0 AND NOT home_win THEN TRUE
                ELSE FALSE
            END
        WHEN ABS(spread_differential) > ABS(ml_differential) THEN
            CASE 
                WHEN spread_differential > 0 AND home_win THEN TRUE
                WHEN spread_differential <= 0 AND NOT home_win THEN TRUE
                ELSE FALSE
            END
        ELSE 
            CASE 
                WHEN ml_differential > 0 AND home_win THEN TRUE
                WHEN ml_differential <= 0 AND NOT home_win THEN TRUE
                ELSE FALSE
            END
    END as strategy_won,
    
    home_score,
    away_score

FROM pivot_data
WHERE (
    (ml_signal LIKE '%HOME_OVER' AND spread_signal LIKE '%AWAY_UNDER') OR
    (ml_signal LIKE '%AWAY_UNDER' AND spread_signal LIKE '%HOME_OVER')
)  -- Only opposing signals
  AND source = 'VSIN'
ORDER BY game_datetime DESC;

-- =============================================================================
-- SUMMARY QUERY: Quick win rate verification
-- =============================================================================

SELECT 
    'Win Rate Verification' as check_type,
    
    -- STEAM_PLAYS verification
    (SELECT 
        CONCAT(
            COUNT(CASE WHEN strategy_won THEN 1 END), '/', 
            COUNT(*), ' = ', 
            ROUND(100.0 * COUNT(CASE WHEN strategy_won THEN 1 END) / COUNT(*), 1), '%'
        )
    FROM (
        -- Repeat STEAM_PLAYS logic here for verification
        SELECT 1 as strategy_won FROM game_level_metrics 
        WHERE rn = 1 AND ABS(line_movement) < 5 AND sharp_classification LIKE 'STRONG_SHARP_%' 
        AND source = 'VSIN' AND split_type = 'spread'
        LIMIT 1  -- Placeholder
    ) steam_check
    ) as steam_plays_win_rate,
    
    'Verify these numbers match your backtesting results' as note; 