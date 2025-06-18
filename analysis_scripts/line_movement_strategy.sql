
WITH line_movement_data AS (
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
        
        go.home_win,
        go.home_cover_spread,
        go.over
        
    FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
    JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
    WHERE rmbs.last_updated < rmbs.game_datetime
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
),

opening_closing_lines AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        
        -- Get opening line (earliest line we have)
        FIRST_VALUE(line_value) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_line,
        
        -- Get closing line (latest line before game)
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
        
    FROM line_movement_data
    WHERE line_value IS NOT NULL
),

line_movement_analysis AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        
        -- Categorize line movement magnitude
        COUNT(CASE WHEN ABS(line_movement) >= 10 THEN 1 END) as big_line_moves,
        COUNT(CASE WHEN ABS(line_movement) >= 5 AND ABS(line_movement) < 10 THEN 1 END) as med_line_moves,
        COUNT(CASE WHEN ABS(line_movement) > 0 AND ABS(line_movement) < 5 THEN 1 END) as small_line_moves,
        COUNT(CASE WHEN line_movement = 0 THEN 1 END) as no_line_moves,
        
        -- Success rates for different line movement scenarios
        -- Strategy: Follow the line movement direction
        SUM(CASE 
            WHEN ABS(line_movement) >= 10 THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN line_movement > 0 AND home_win = false THEN 1  -- Line moved away from home (home got worse odds), bet away
                             WHEN line_movement < 0 AND home_win = true THEN 1   -- Line moved toward home (home got better odds), bet home
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
        END) as big_move_wins,
        
        -- Strategy: Fade significant line movement (contrarian)
        SUM(CASE 
            WHEN ABS(line_movement) >= 10 THEN
                CASE split_type
                    WHEN 'moneyline' THEN 
                        CASE WHEN line_movement > 0 AND home_win = true THEN 1   -- Fade: line moved away from home, bet home
                             WHEN line_movement < 0 AND home_win = false THEN 1  -- Fade: line moved toward home, bet away
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
        END) as big_move_fade_wins
        
    FROM opening_closing_lines
    WHERE rn = 1 
      AND opening_line IS NOT NULL 
      AND closing_line IS NOT NULL
      AND opening_line != closing_line  -- Only games with actual line movement
    GROUP BY source, book, split_type
)

SELECT 
    source_book_type,
    split_type,
    big_line_moves,
    med_line_moves,
    small_line_moves,
    no_line_moves,
    
    -- Follow line movement strategy
    CASE WHEN big_line_moves > 0 
         THEN ROUND(100.0 * big_move_wins / big_line_moves, 1) 
         ELSE 0 END as follow_big_moves_rate,
    
    -- Fade line movement strategy
    CASE WHEN big_line_moves > 0 
         THEN ROUND(100.0 * big_move_fade_wins / big_line_moves, 1) 
         ELSE 0 END as fade_big_moves_rate,
    
    -- Best strategy
    CASE 
        WHEN big_line_moves >= 3 AND (100.0 * big_move_wins / big_line_moves) > 60 THEN '🟢 FOLLOW BIG MOVES'
        WHEN big_line_moves >= 3 AND (100.0 * big_move_fade_wins / big_line_moves) > 60 THEN '🟢 FADE BIG MOVES'
        WHEN big_line_moves >= 3 AND (100.0 * big_move_wins / big_line_moves) > 52.4 THEN '🟡 FOLLOW PROFITABLE'
        WHEN big_line_moves >= 3 AND (100.0 * big_move_fade_wins / big_line_moves) > 52.4 THEN '🟡 FADE PROFITABLE'
        ELSE '🔴 NO PROFITABLE STRATEGY'
    END as recommendation
    
FROM line_movement_analysis
WHERE big_line_moves > 0 OR med_line_moves > 0
ORDER BY 
    GREATEST(
        CASE WHEN big_line_moves > 0 THEN 100.0 * big_move_wins / big_line_moves ELSE 0 END,
        CASE WHEN big_line_moves > 0 THEN 100.0 * big_move_fade_wins / big_line_moves ELSE 0 END
    ) DESC;
