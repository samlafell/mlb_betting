-- Sharp Action Detection Strategy
-- Identifies when stake % significantly exceeds bet % (indicating larger average bet sizes)
-- This typically suggests "sharp" or professional money is on that side
--
-- DEDUPLICATION APPROACH:
-- This script preserves ALL line movement data for analysis but implements 
-- recommendation-level deduplication to ensure only ONE final bet per game per market.
-- Final recommendations use data closest to 5 minutes before first pitch.

WITH sharp_action_data AS (
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
        
        -- Calculate hours before game
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
        
        -- Calculate minutes before game for 5-minute rule
        EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 60 AS minutes_before_game,
        
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
        
        -- Classify timing
        CASE 
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 2 THEN 'CLOSING'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 6 THEN 'LATE'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 24 THEN 'EARLY'
            ELSE 'VERY_EARLY'
        END as timing_category,
        
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

closing_sharp_action AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        
        -- Get the final sharp action indicator (closest to game time)
        LAST_VALUE(sharp_indicator) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_sharp_indicator,
        
        LAST_VALUE(differential) OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as final_differential,
        
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
        
        home_win,
        home_cover_spread,
        over,
        
        -- RECOMMENDATION-LEVEL DEDUPLICATION:
        -- Select the record closest to 5 minutes before game time for final betting recommendation
        ROW_NUMBER() OVER (
            PARTITION BY game_id, source, book, split_type 
            ORDER BY 
                ABS(minutes_before_game - 5) ASC,  -- Closest to 5 minutes before game
                last_updated DESC                  -- Most recent if tied
        ) as rn
        
    FROM sharp_action_data
),

sharp_strategy_analysis AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        final_sharp_indicator,
        
        -- Count total bets for each sharp indicator
        COUNT(*) as total_bets,
        
        -- Calculate win rates for following sharp action
        SUM(CASE 
            WHEN final_sharp_indicator LIKE '%HOME_OVER' THEN
                CASE split_type
                    WHEN 'moneyline' THEN CASE WHEN home_win = true THEN 1 ELSE 0 END
                    WHEN 'spread' THEN CASE WHEN home_cover_spread = true THEN 1 ELSE 0 END
                    WHEN 'total' THEN CASE WHEN over = true THEN 1 ELSE 0 END
                END
            WHEN final_sharp_indicator LIKE '%AWAY_UNDER' THEN
                CASE split_type
                    WHEN 'moneyline' THEN CASE WHEN home_win = false THEN 1 ELSE 0 END
                    WHEN 'spread' THEN CASE WHEN home_cover_spread = false THEN 1 ELSE 0 END
                    WHEN 'total' THEN CASE WHEN over = false THEN 1 ELSE 0 END
                END
            ELSE 0
        END) as sharp_wins,
        
        -- Calculate average differentials
        AVG(final_differential) as avg_differential,
        AVG(final_stake_pct) as avg_stake_pct,
        AVG(final_bet_pct) as avg_bet_pct
        
    FROM closing_sharp_action
    WHERE rn = 1 
      AND final_sharp_indicator != 'NO_SHARP_ACTION'
    GROUP BY source, book, split_type, final_sharp_indicator
    HAVING COUNT(*) >= 5  -- Minimum sample size
)

SELECT 
    source_book_type,
    split_type,
    final_sharp_indicator,
    total_bets,
    sharp_wins,
    
    -- Win rate
    ROUND(100.0 * sharp_wins / total_bets, 1) as win_rate,
    
    -- ROI calculation (assuming -110 odds)
    ROUND(
        ((sharp_wins * 100) - ((total_bets - sharp_wins) * 110)) / (total_bets * 110) * 100, 1
    ) as roi_per_100_unit,
    
    -- Average differential
    ROUND(avg_differential, 1) as avg_diff,
    
    -- Classification
    CASE 
        WHEN (100.0 * sharp_wins / total_bets) >= 60 AND total_bets >= 10 THEN '🟢 STRONG SHARP EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 55 AND total_bets >= 10 THEN '🟡 MODERATE SHARP EDGE'
        WHEN (100.0 * sharp_wins / total_bets) >= 52.4 AND total_bets >= 10 THEN '🟡 SLIGHT EDGE'
        ELSE '🔴 NO EDGE'
    END as strategy_rating,
    
    -- Confidence level based on sample size
    CASE 
        WHEN total_bets >= 50 THEN 'HIGH'
        WHEN total_bets >= 20 THEN 'MEDIUM'
        WHEN total_bets >= 10 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as confidence_level
    
FROM sharp_strategy_analysis
WHERE total_bets >= 5
ORDER BY 
    (100.0 * sharp_wins / total_bets) DESC,
    total_bets DESC; 