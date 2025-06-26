-- Line Movement Strategy Analysis
-- Tests whether following or fading line movement is profitable
-- Uses actual moneyline odds changes from our splits data

WITH line_data AS (
    SELECT 
        rmbs.game_id,
        rmbs.source,
        COALESCE(rmbs.book, 'UNKNOWN') as book,
        rmbs.split_type,
        rmbs.home_team,
        rmbs.away_team,
        rmbs.game_datetime,
        rmbs.last_updated,
        rmbs.home_or_over_stake_percentage as stake_pct,
        rmbs.home_or_over_bets_percentage as bet_pct,
        rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
        
        -- Extract home odds from moneyline JSON using PostgreSQL JSONB operators
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                (rmbs.split_value::JSONB->>'home')::INTEGER
            ELSE NULL
        END as home_odds,
        
        -- Extract away odds from moneyline JSON using PostgreSQL JSONB operators
        CASE 
            WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                (rmbs.split_value::JSONB->>'away')::INTEGER
            ELSE NULL
        END as away_odds,
        
        -- For spread/total, use split_value directly with safe casting
        CASE 
            WHEN rmbs.split_type IN ('spread', 'total') AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                rmbs.split_value::DOUBLE PRECISION
            ELSE NULL
        END as line_value,
        
        go.home_win,
        go.home_cover_spread,
        go.over
        
    FROM splits.raw_mlb_betting_splits rmbs
    JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND go.home_score IS NOT NULL
      AND go.away_score IS NOT NULL
),

-- Get opening and closing lines for each game/source/book/type
line_movement AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        
        -- Opening lines (earliest timestamp)
        FIRST_VALUE(home_odds) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_home_odds,
        
        FIRST_VALUE(away_odds) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_away_odds,
        
        FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_line,
        
        -- Closing lines (latest timestamp)
        LAST_VALUE(home_odds) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_home_odds,
        
        LAST_VALUE(away_odds) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_away_odds,
        
        LAST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_line,
        
        -- Final betting percentages
        LAST_VALUE(stake_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_stake_pct,
        
        LAST_VALUE(bet_pct) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_bet_pct,
        
        LAST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_differential,
        
        home_win,
        home_cover_spread,
        over,
        
        ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
        
    FROM line_data
),

-- Calculate line movements and determine betting strategies
movement_analysis AS (
    SELECT 
        source || '-' || book || '-' || split_type as source_book_type,
        split_type,
        game_id,
        
        -- Calculate line movement
        CASE 
            WHEN split_type = 'moneyline' THEN
                closing_home_odds - opening_home_odds
            ELSE
                closing_line - opening_line
        END as line_movement,
        
        final_stake_pct,
        final_bet_pct,
        final_differential,
        
        -- Determine if there was significant line movement
        CASE 
            WHEN split_type = 'moneyline' THEN
                CASE WHEN ABS(closing_home_odds - opening_home_odds) >= 20 THEN 'SIGNIFICANT'
                     WHEN ABS(closing_home_odds - opening_home_odds) >= 10 THEN 'MODERATE'
                     WHEN ABS(closing_home_odds - opening_home_odds) > 0 THEN 'MINOR'
                     ELSE 'NONE' END
            ELSE
                CASE WHEN ABS(closing_line - opening_line) >= 1.0 THEN 'SIGNIFICANT'
                     WHEN ABS(closing_line - opening_line) >= 0.5 THEN 'MODERATE'
                     WHEN ABS(closing_line - opening_line) > 0 THEN 'MINOR'
                     ELSE 'NONE' END
        END as movement_category,
        
        home_win,
        home_cover_spread,
        over
        
    FROM line_movement
    WHERE rn = 1 
      AND ((split_type = 'moneyline' AND opening_home_odds IS NOT NULL AND closing_home_odds IS NOT NULL)
           OR (split_type != 'moneyline' AND opening_line IS NOT NULL AND closing_line IS NOT NULL))
),

-- Strategy performance analysis
strategy_results AS (
    SELECT 
        source_book_type,
        split_type,
        
        -- Count games by movement category
        COUNT(CASE WHEN movement_category = 'SIGNIFICANT' THEN 1 END) as significant_moves,
        COUNT(CASE WHEN movement_category = 'MODERATE' THEN 1 END) as moderate_moves,
        COUNT(CASE WHEN movement_category = 'MINOR' THEN 1 END) as minor_moves,
        COUNT(CASE WHEN movement_category = 'NONE' THEN 1 END) as no_moves,
        
        -- Strategy 1: Follow significant line movement
        SUM(CASE 
            WHEN movement_category = 'SIGNIFICANT' THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN line_movement > 0 AND home_win = false THEN 1  -- Home odds got worse, bet away
                             WHEN line_movement < 0 AND home_win = true THEN 1   -- Home odds got better, bet home
                             ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN line_movement > 0 AND home_cover_spread = false THEN 1  -- Spread moved against home
                             WHEN line_movement < 0 AND home_cover_spread = true THEN 1   -- Spread moved toward home
                             ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN line_movement > 0 AND over = true THEN 1       -- Total moved up, bet over
                             WHEN line_movement < 0 AND over = false THEN 1      -- Total moved down, bet under
                             ELSE 0 END
                END
            ELSE 0
        END) as follow_significant_wins,
        
        -- Strategy 2: Fade significant line movement
        SUM(CASE 
            WHEN movement_category = 'SIGNIFICANT' THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN line_movement > 0 AND home_win = true THEN 1   -- Fade: home odds got worse, bet home
                             WHEN line_movement < 0 AND home_win = false THEN 1  -- Fade: home odds got better, bet away
                             ELSE 0 END
                    WHEN 'spread' THEN 
                        CASE WHEN line_movement > 0 AND home_cover_spread = true THEN 1
                             WHEN line_movement < 0 AND home_cover_spread = false THEN 1
                             ELSE 0 END
                    WHEN 'total' THEN 
                        CASE WHEN line_movement > 0 AND over = false THEN 1      -- Fade: total moved up, bet under
                             WHEN line_movement < 0 AND over = true THEN 1       -- Fade: total moved down, bet over
                             ELSE 0 END
                END
            ELSE 0
        END) as fade_significant_wins
        
    FROM movement_analysis
    GROUP BY source_book_type, split_type
    HAVING COUNT(*) >= 5  -- Minimum sample size
)

-- Final results formatted for backtesting service
SELECT 
    source_book_type,
    split_type,
    'follow_significant_movement' as strategy_variant,
    significant_moves as total_bets,
    follow_significant_wins as wins,
    CASE WHEN significant_moves > 0 
         THEN ROUND((100.0 * follow_significant_wins / significant_moves)::NUMERIC, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN significant_moves > 0 
         THEN ROUND((((follow_significant_wins * 100) - ((significant_moves - follow_significant_wins) * 110))::NUMERIC / (significant_moves * 110) * 100), 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_results
WHERE significant_moves >= 3

UNION ALL

SELECT 
    source_book_type,
    split_type,
    'fade_significant_movement' as strategy_variant,
    significant_moves as total_bets,
    fade_significant_wins as wins,
    CASE WHEN significant_moves > 0 
         THEN ROUND((100.0 * fade_significant_wins / significant_moves)::NUMERIC, 1) 
         ELSE 0 END as win_rate,
    CASE WHEN significant_moves > 0 
         THEN ROUND((((fade_significant_wins * 100) - ((significant_moves - fade_significant_wins) * 110))::NUMERIC / (significant_moves * 110) * 100), 1)
         ELSE 0 END as roi_per_100_unit
FROM strategy_results
WHERE significant_moves >= 3

ORDER BY roi_per_100_unit DESC;
