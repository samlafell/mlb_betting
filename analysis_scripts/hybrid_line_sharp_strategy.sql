-- Hybrid Line Movement + Sharp Action Strategy
-- Combines line movement analysis with sharp action detection
-- Looks for confirmation between line movement and professional money flow

WITH comprehensive_data AS (
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
        
        -- Extract line values from JSON
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                TRY_CAST(json_extract_string(rmbs.split_value, '$.home') AS DOUBLE)
            WHEN rmbs.split_type IN ('spread', 'total') THEN
                TRY_CAST(rmbs.split_value AS DOUBLE)
            ELSE NULL
        END as line_value,
        
        -- Calculate hours before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        
        -- Sharp action indicators
        CASE 
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 15 THEN 'STRONG_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 10 THEN 'MODERATE_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage >= 5 THEN 'WEAK_SHARP_HOME_OVER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -15 THEN 'STRONG_SHARP_AWAY_UNDER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -10 THEN 'MODERATE_SHARP_AWAY_UNDER'
            WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage <= -5 THEN 'WEAK_SHARP_AWAY_UNDER'
            ELSE 'NO_SHARP_ACTION'
        END as sharp_indicator,
        
        go.home_win,
        go.home_cover_spread,
        go.over
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

opening_closing_with_sharp AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        
        -- Opening metrics
        FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_line,
        
        FIRST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_differential,
        
        -- Closing metrics
        LAST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_line,
        
        LAST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_differential,
        
        LAST_VALUE(sharp_indicator) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_sharp_indicator,
        
        LAST_VALUE(stake_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_stake_pct,
        
        -- Calculate line movement
        LAST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) - FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as line_movement,
        
        home_win,
        home_cover_spread,
        over,
        
        ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
        
    FROM comprehensive_data
    WHERE line_value IS NOT NULL
),

hybrid_strategy_classification AS (
    SELECT 
        *,
        -- Classify hybrid strategies
        CASE 
            -- Strong confirmation strategies (line movement + strong sharp action in same direction)
            WHEN ABS(line_movement) >= 10 AND closing_sharp_indicator LIKE 'STRONG_SHARP_%' THEN
                CASE 
                    WHEN (line_movement > 0 AND closing_sharp_indicator LIKE '%AWAY_UNDER') OR 
                         (line_movement < 0 AND closing_sharp_indicator LIKE '%HOME_OVER') THEN 'STRONG_CONFIRMATION'
                    ELSE 'STRONG_CONFLICT'
                END
            
            -- Moderate confirmation strategies
            WHEN ABS(line_movement) >= 5 AND closing_sharp_indicator LIKE 'MODERATE_SHARP_%' THEN
                CASE 
                    WHEN (line_movement > 0 AND closing_sharp_indicator LIKE '%AWAY_UNDER') OR 
                         (line_movement < 0 AND closing_sharp_indicator LIKE '%HOME_OVER') THEN 'MODERATE_CONFIRMATION'
                    ELSE 'MODERATE_CONFLICT'
                END
            
            -- Sharp action without significant line movement (steam play)
            WHEN ABS(line_movement) < 5 AND closing_sharp_indicator LIKE 'STRONG_SHARP_%' THEN 'STEAM_PLAY'
            
            -- Line movement without sharp confirmation (public money)
            WHEN ABS(line_movement) >= 10 AND closing_sharp_indicator = 'NO_SHARP_ACTION' THEN 'PUBLIC_MOVE'
            
            -- Reverse line movement (line moves opposite to public)
            WHEN ABS(line_movement) >= 5 AND 
                 ((line_movement > 0 AND closing_stake_pct < 40) OR 
                  (line_movement < 0 AND closing_stake_pct > 60)) THEN 'REVERSE_LINE_MOVEMENT'
            
            ELSE 'NO_CLEAR_SIGNAL'
        END as hybrid_strategy,
        
        -- Determine bet recommendation
        CASE split_type
            WHEN 'moneyline' THEN
                CASE 
                    WHEN closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME'
                    WHEN closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY'
                    WHEN line_movement > 0 THEN 'BET_AWAY'  -- Line moved away from home
                    WHEN line_movement < 0 THEN 'BET_HOME'  -- Line moved toward home
                    ELSE 'NO_BET'
                END
            WHEN 'spread' THEN
                CASE 
                    WHEN closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME'
                    WHEN closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY'
                    WHEN line_movement > 0 THEN 'BET_AWAY'  -- Spread moved against home
                    WHEN line_movement < 0 THEN 'BET_HOME'  -- Spread moved toward home
                    ELSE 'NO_BET'
                END
            WHEN 'total' THEN
                CASE 
                    WHEN closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER'
                    WHEN closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER'
                    WHEN line_movement > 0 THEN 'BET_OVER'   -- Total moved up
                    WHEN line_movement < 0 THEN 'BET_UNDER'  -- Total moved down
                    ELSE 'NO_BET'
                END
        END as bet_recommendation
        
    FROM opening_closing_with_sharp
    WHERE rn = 1 
      AND opening_line IS NOT NULL 
      AND closing_line IS NOT NULL
),

strategy_performance AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        hybrid_strategy,
        bet_recommendation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins based on bet recommendation
        SUM(CASE 
            WHEN bet_recommendation = 'BET_HOME' AND home_win = true THEN 1
            WHEN bet_recommendation = 'BET_AWAY' AND home_win = false THEN 1
            WHEN bet_recommendation = 'BET_HOME' AND split_type = 'spread' AND home_cover_spread = true THEN 1
            WHEN bet_recommendation = 'BET_AWAY' AND split_type = 'spread' AND home_cover_spread = false THEN 1
            WHEN bet_recommendation = 'BET_OVER' AND over = true THEN 1
            WHEN bet_recommendation = 'BET_UNDER' AND over = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Average metrics
        AVG(ABS(line_movement)) as avg_line_movement,
        AVG(closing_differential) as avg_closing_differential,
        AVG(closing_stake_pct) as avg_closing_stake_pct
        
    FROM hybrid_strategy_classification
    WHERE hybrid_strategy != 'NO_CLEAR_SIGNAL'
      AND bet_recommendation != 'NO_BET'
    GROUP BY source, book, split_type, hybrid_strategy, bet_recommendation
    HAVING COUNT(*) >= 5  -- Minimum sample size
)

SELECT 
    source_book_type,
    split_type,
    hybrid_strategy,
    bet_recommendation,
    total_bets,
    wins,
    
    -- Performance metrics
    ROUND(100.0 * wins / total_bets, 1) as win_rate,
    
    -- ROI calculation for $100 unit bets (assuming -110 odds)
    ROUND(
        (wins * 90.91) - ((total_bets - wins) * 100), 2
    ) as roi_per_100_unit,
    
    -- Profit per bet
    ROUND((wins * 90.91 - (total_bets - wins) * 100) / total_bets, 2) as profit_per_bet,
    
    -- Average metrics
    ROUND(avg_line_movement, 1) as avg_line_move,
    ROUND(avg_closing_differential, 1) as avg_diff,
    ROUND(avg_closing_stake_pct, 1) as avg_stake_pct,
    
    -- Strategy rating
    CASE 
        WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 15 THEN '🟢 EXCELLENT'
        WHEN (100.0 * wins / total_bets) >= 57 AND total_bets >= 10 THEN '🟢 VERY GOOD'
        WHEN (100.0 * wins / total_bets) >= 54 AND total_bets >= 10 THEN '🟡 GOOD'
        WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 10 THEN '🟡 PROFITABLE'
        ELSE '🔴 UNPROFITABLE'
    END as strategy_rating,
    
    -- Sample size confidence
    CASE 
        WHEN total_bets >= 50 THEN 'HIGH_CONFIDENCE'
        WHEN total_bets >= 25 THEN 'MEDIUM_CONFIDENCE'
        WHEN total_bets >= 10 THEN 'LOW_CONFIDENCE'
        ELSE 'VERY_LOW_CONFIDENCE'
    END as confidence_level
    
FROM strategy_performance
WHERE total_bets >= 5
ORDER BY 
    (100.0 * wins / total_bets) DESC,
    total_bets DESC; 