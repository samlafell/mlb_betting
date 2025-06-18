-- Comprehensive Strategy Comparison with ROI Analysis
-- Compares all betting strategies and provides detailed ROI calculations for $100 unit bets

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
                TRY_CAST(json_extract_string(rmbs.split_value, '$.home') AS DOUBLE)
            WHEN rmbs.split_type IN ('spread', 'total') THEN
                TRY_CAST(rmbs.split_value AS DOUBLE)
            ELSE NULL
        END as line_value,
        
        -- Calculate hours before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        
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

game_level_metrics AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        
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
        
        LAST_VALUE(bet_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_bet_pct,
        
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
        
        ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
        
    FROM base_data
    WHERE line_value IS NOT NULL
),

strategy_classifications AS (
    SELECT 
        *,
        
        -- Strategy 1: Line Movement (from original script)
        CASE 
            WHEN ABS(line_movement) >= 10 THEN 'BIG_LINE_MOVEMENT'
            WHEN ABS(line_movement) >= 5 THEN 'MODERATE_LINE_MOVEMENT'
            WHEN ABS(line_movement) > 0 THEN 'SMALL_LINE_MOVEMENT'
            ELSE 'NO_LINE_MOVEMENT'
        END as line_movement_category,
        
        -- Strategy 2: Sharp Action Only
        CASE 
            WHEN sharp_classification LIKE 'STRONG_SHARP_%' THEN 'STRONG_SHARP'
            WHEN sharp_classification LIKE 'MODERATE_SHARP_%' THEN 'MODERATE_SHARP'
            WHEN sharp_classification LIKE 'WEAK_SHARP_%' THEN 'WEAK_SHARP'
            ELSE 'NO_SHARP'
        END as sharp_category,
        
        -- Strategy 3: Hybrid Confirmation
        CASE 
            WHEN ABS(line_movement) >= 10 AND sharp_classification LIKE 'STRONG_SHARP_%' THEN
                CASE 
                    WHEN (line_movement > 0 AND sharp_classification LIKE '%AWAY_UNDER') OR 
                         (line_movement < 0 AND sharp_classification LIKE '%HOME_OVER') THEN 'STRONG_CONFIRMATION'
                    ELSE 'STRONG_CONFLICT'
                END
            WHEN ABS(line_movement) >= 5 AND sharp_classification LIKE 'MODERATE_SHARP_%' THEN
                CASE 
                    WHEN (line_movement > 0 AND sharp_classification LIKE '%AWAY_UNDER') OR 
                         (line_movement < 0 AND sharp_classification LIKE '%HOME_OVER') THEN 'MODERATE_CONFIRMATION'
                    ELSE 'MODERATE_CONFLICT'
                END
            WHEN ABS(line_movement) < 5 AND sharp_classification LIKE 'STRONG_SHARP_%' THEN 'STEAM_PLAY'
            WHEN ABS(line_movement) >= 10 AND sharp_classification = 'NO_SHARP_ACTION' THEN 'PUBLIC_MOVE'
            ELSE 'NO_HYBRID_SIGNAL'
        END as hybrid_category,
        
        -- Strategy 4: Reverse Line Movement (contrarian)
        CASE 
            WHEN ABS(line_movement) >= 5 AND 
                 ((line_movement > 0 AND closing_stake_pct < 40) OR 
                  (line_movement < 0 AND closing_stake_pct > 60)) THEN 'REVERSE_LINE_MOVEMENT'
            ELSE 'NO_REVERSE'
        END as reverse_category
        
    FROM game_level_metrics
    WHERE rn = 1 AND opening_line IS NOT NULL AND closing_line IS NOT NULL
),

strategy_results AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        
        -- Strategy 1: Follow Big Line Movement
        'FOLLOW_BIG_LINE_MOVEMENT' as strategy_name,
        'BIG_LINE_MOVEMENT' as strategy_classification,
        
        COUNT(CASE WHEN line_movement_category = 'BIG_LINE_MOVEMENT' THEN 1 END) as total_bets,
        
        SUM(CASE 
            WHEN line_movement_category = 'BIG_LINE_MOVEMENT' THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN (line_movement > 0 AND home_win = false) OR (line_movement < 0 AND home_win = true) THEN 1 ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN (line_movement > 0 AND home_cover_spread = false) OR (line_movement < 0 AND home_cover_spread = true) THEN 1 ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN (line_movement > 0 AND over = true) OR (line_movement < 0 AND over = false) THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as wins,
        
        AVG(CASE WHEN line_movement_category = 'BIG_LINE_MOVEMENT' THEN ABS(line_movement) END) as avg_line_movement,
        AVG(CASE WHEN line_movement_category = 'BIG_LINE_MOVEMENT' THEN closing_differential END) as avg_differential
        
    FROM strategy_classifications
    GROUP BY source, book, split_type
    HAVING COUNT(CASE WHEN line_movement_category = 'BIG_LINE_MOVEMENT' THEN 1 END) >= 5
    
    UNION ALL
    
    -- Strategy 2: Follow Strong Sharp Action
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        'FOLLOW_STRONG_SHARP' as strategy_name,
        'STRONG_SHARP' as strategy_classification,
        
        COUNT(CASE WHEN sharp_category = 'STRONG_SHARP' THEN 1 END) as total_bets,
        
        SUM(CASE 
            WHEN sharp_category = 'STRONG_SHARP' THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_win = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_win = false) THEN 1 ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_cover_spread = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_cover_spread = false) THEN 1 ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND over = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND over = false) THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as wins,
        
        AVG(CASE WHEN sharp_category = 'STRONG_SHARP' THEN ABS(line_movement) END) as avg_line_movement,
        AVG(CASE WHEN sharp_category = 'STRONG_SHARP' THEN closing_differential END) as avg_differential
        
    FROM strategy_classifications
    GROUP BY source, book, split_type
    HAVING COUNT(CASE WHEN sharp_category = 'STRONG_SHARP' THEN 1 END) >= 5
    
    UNION ALL
    
    -- Strategy 3: Hybrid Confirmation
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        'HYBRID_CONFIRMATION' as strategy_name,
        'CONFIRMATION' as strategy_classification,
        
        COUNT(CASE WHEN hybrid_category IN ('STRONG_CONFIRMATION', 'MODERATE_CONFIRMATION') THEN 1 END) as total_bets,
        
        SUM(CASE 
            WHEN hybrid_category IN ('STRONG_CONFIRMATION', 'MODERATE_CONFIRMATION') THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_win = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_win = false) THEN 1 ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_cover_spread = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_cover_spread = false) THEN 1 ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND over = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND over = false) THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as wins,
        
        AVG(CASE WHEN hybrid_category IN ('STRONG_CONFIRMATION', 'MODERATE_CONFIRMATION') THEN ABS(line_movement) END) as avg_line_movement,
        AVG(CASE WHEN hybrid_category IN ('STRONG_CONFIRMATION', 'MODERATE_CONFIRMATION') THEN closing_differential END) as avg_differential
        
    FROM strategy_classifications
    GROUP BY source, book, split_type
    HAVING COUNT(CASE WHEN hybrid_category IN ('STRONG_CONFIRMATION', 'MODERATE_CONFIRMATION') THEN 1 END) >= 3
    
    UNION ALL
    
    -- Strategy 4: Steam Plays
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        'STEAM_PLAYS' as strategy_name,
        'STEAM_PLAY' as strategy_classification,
        
        COUNT(CASE WHEN hybrid_category = 'STEAM_PLAY' THEN 1 END) as total_bets,
        
        SUM(CASE 
            WHEN hybrid_category = 'STEAM_PLAY' THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_win = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_win = false) THEN 1 ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND home_cover_spread = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND home_cover_spread = false) THEN 1 ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN (sharp_classification LIKE '%HOME_OVER' AND over = true) OR (sharp_classification LIKE '%AWAY_UNDER' AND over = false) THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as wins,
        
        AVG(CASE WHEN hybrid_category = 'STEAM_PLAY' THEN ABS(line_movement) END) as avg_line_movement,
        AVG(CASE WHEN hybrid_category = 'STEAM_PLAY' THEN closing_differential END) as avg_differential
        
    FROM strategy_classifications
    GROUP BY source, book, split_type
    HAVING COUNT(CASE WHEN hybrid_category = 'STEAM_PLAY' THEN 1 END) >= 3
)

SELECT 
    source_book_type,
    split_type,
    strategy_name,
    total_bets,
    wins,
    
    -- Core Performance Metrics
    ROUND(100.0 * wins / total_bets, 1) as win_rate,
    
    -- ROI Calculations for $100 unit bets
    -- Assuming standard -110 odds (risk $110 to win $100)
    ROUND((wins * 100) - ((total_bets - wins) * 110), 2) as roi_dollars_110_odds,
    
    -- Alternative: Assuming -105 odds (slightly better)
    ROUND((wins * 100) - ((total_bets - wins) * 105), 2) as roi_dollars_105_odds,
    
    -- Break-even analysis
    ROUND(110.0 / 210.0 * 100, 1) as breakeven_rate_110_odds,
    ROUND(105.0 / 205.0 * 100, 1) as breakeven_rate_105_odds,
    
    -- Profit per bet
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / total_bets, 2) as profit_per_bet_110,
    ROUND(((wins * 100) - ((total_bets - wins) * 105)) / total_bets, 2) as profit_per_bet_105,
    
    -- ROI Percentage 
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100, 1) as roi_percentage_110,
    ROUND(((wins * 100) - ((total_bets - wins) * 105)) / (total_bets * 105) * 100, 1) as roi_percentage_105,
    
    -- Supporting metrics
    ROUND(avg_line_movement, 1) as avg_line_move,
    ROUND(avg_differential, 1) as avg_diff,
    
    -- Strategy Rating
    CASE 
        WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 10 THEN '🟢 EXCELLENT'
        WHEN (100.0 * wins / total_bets) >= 57 AND total_bets >= 8 THEN '🟢 VERY GOOD'
        WHEN (100.0 * wins / total_bets) >= 54 AND total_bets >= 5 THEN '🟡 GOOD'  
        WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 5 THEN '🟡 PROFITABLE'
        ELSE '🔴 UNPROFITABLE'
    END as strategy_rating,
    
    -- Confidence Level
    CASE 
        WHEN total_bets >= 50 THEN 'HIGH'
        WHEN total_bets >= 20 THEN 'MEDIUM'
        WHEN total_bets >= 10 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level,
    
    -- Kelly Criterion recommendation (simplified)
    CASE 
        WHEN (100.0 * wins / total_bets) > 52.4 THEN 
            ROUND(((100.0 * wins / total_bets / 100) * 2.1 - 1) / 1.1 * 100, 1)
        ELSE 0
    END as kelly_bet_percentage

FROM strategy_results
WHERE total_bets >= 3
ORDER BY 
    roi_percentage_110 DESC,
    total_bets DESC; 