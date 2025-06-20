-- Timing-Based Sharp Action Strategy
-- Analyzes the effectiveness of betting based on when sharp action occurs
-- Early sharp action vs late sharp action vs closing line value

WITH timing_data AS (
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
        
        -- Timing categories
        CASE 
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 2 THEN 'CLOSING_2H'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 6 THEN 'LATE_6H'
            WHEN EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 <= 24 THEN 'EARLY_24H'
            ELSE 'VERY_EARLY'
        END as timing_category,
        
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
      AND rmbs.game_datetime < CURRENT_TIMESTAMP - INTERVAL '6 hours'  -- Only completed games
      AND rmbs.split_value IS NOT NULL
      AND rmbs.game_datetime IS NOT NULL
      AND rmbs.home_or_over_stake_percentage IS NOT NULL
      AND rmbs.home_or_over_bets_percentage IS NOT NULL
),

timing_aggregated AS (
    SELECT 
        game_id,
        source,
        book,
        split_type,
        home_team,
        away_team,
        home_win,
        home_cover_spread,
        over,
        
        -- Find earliest sharp action (if any)
        MIN(CASE WHEN sharp_indicator != 'NO_SHARP_ACTION' THEN hours_before_game END) as earliest_sharp_hours,
        
        -- Count distinct sharp indicators
        COUNT(DISTINCT CASE WHEN sharp_indicator != 'NO_SHARP_ACTION' THEN sharp_indicator END) as distinct_sharp_count,
        
        -- Find timing of strong sharp action
        MIN(CASE WHEN sharp_indicator LIKE 'STRONG_SHARP_%' THEN hours_before_game END) as earliest_strong_sharp_hours
        
    FROM timing_data
    GROUP BY game_id, source, book, split_type, home_team, away_team, home_win, home_cover_spread, over
),

early_vs_late_sharp AS (
    SELECT 
        ta.*,
        
        -- Get closing sharp action (latest data point)
        LAST_VALUE(td.sharp_indicator) OVER (
            PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type 
            ORDER BY td.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_sharp_indicator,
        
        -- Get closing differential
        LAST_VALUE(td.differential) OVER (
            PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type 
            ORDER BY td.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as closing_differential,
        
        -- Classification based on timing pattern
        CASE 
            -- Early sharp action that persists
            WHEN ta.earliest_strong_sharp_hours > 12 THEN 'EARLY_PERSISTENT_SHARP'
            
            -- Late developing sharp action
            WHEN ta.earliest_strong_sharp_hours <= 6 AND ta.earliest_strong_sharp_hours > 2 THEN 'LATE_SHARP_ACTION'
            
            -- Steam (sharp action appears suddenly close to game)
            WHEN ta.earliest_strong_sharp_hours <= 2 THEN 'STEAM_MOVE'
            
            -- Inconsistent sharp action
            WHEN ta.distinct_sharp_count > 1 THEN 'CONFLICTING_SHARP'
            
            ELSE 'NO_CLEAR_PATTERN'
        END as timing_pattern,
        
        ROW_NUMBER() OVER (PARTITION BY ta.game_id, ta.source, ta.book, ta.split_type ORDER BY td.last_updated DESC) as rn
        
    FROM timing_aggregated ta
    JOIN timing_data td ON ta.game_id = td.game_id AND ta.source = td.source AND ta.book = td.book AND ta.split_type = td.split_type
),

early_vs_late_with_bets AS (
    SELECT 
        *,
        -- Determine bet recommendation based on timing and sharp action
        CASE split_type
            WHEN 'moneyline' THEN
                CASE 
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_EARLY'
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_EARLY'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_LATE'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_LATE'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_STEAM'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_STEAM'
                    ELSE 'NO_BET'
                END
            WHEN 'spread' THEN
                CASE 
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_EARLY'
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_EARLY'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_LATE'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_LATE'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_HOME_STEAM'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_AWAY_STEAM'
                    ELSE 'NO_BET'
                END
            WHEN 'total' THEN
                CASE 
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_EARLY'
                    WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_EARLY'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_LATE'
                    WHEN timing_pattern = 'LATE_SHARP_ACTION' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_LATE'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%HOME_OVER' THEN 'BET_OVER_STEAM'
                    WHEN timing_pattern = 'STEAM_MOVE' AND closing_sharp_indicator LIKE '%AWAY_UNDER' THEN 'BET_UNDER_STEAM'
                    ELSE 'NO_BET'
                END
        END as bet_recommendation
        
    FROM early_vs_late_sharp
    WHERE rn = 1 
      AND timing_pattern != 'NO_CLEAR_PATTERN'
      AND closing_sharp_indicator != 'NO_SHARP_ACTION'
),

timing_strategy_performance AS (
    SELECT 
        CONCAT(source, '-', book, '-', split_type) as source_book_type,
        split_type,
        timing_pattern,
        closing_sharp_indicator,
        bet_recommendation,
        
        COUNT(*) as total_bets,
        
        -- Calculate wins
        SUM(CASE 
            WHEN bet_recommendation LIKE 'BET_HOME%' AND home_win = true THEN 1
            WHEN bet_recommendation LIKE 'BET_AWAY%' AND home_win = false THEN 1
            WHEN bet_recommendation LIKE 'BET_HOME%' AND split_type = 'spread' AND home_cover_spread = true THEN 1
            WHEN bet_recommendation LIKE 'BET_AWAY%' AND split_type = 'spread' AND home_cover_spread = false THEN 1
            WHEN bet_recommendation LIKE 'BET_OVER%' AND over = true THEN 1
            WHEN bet_recommendation LIKE 'BET_UNDER%' AND over = false THEN 1
            ELSE 0
        END) as wins,
        
        -- Average closing differential for the timing pattern
        AVG(closing_differential) as avg_closing_differential,
        AVG(earliest_sharp_hours) as avg_earliest_sharp_hours
        
    FROM early_vs_late_with_bets
    WHERE bet_recommendation != 'NO_BET'
    GROUP BY source, book, split_type, timing_pattern, closing_sharp_indicator, bet_recommendation
    HAVING COUNT(*) >= 3  -- Minimum sample size for timing analysis
)

SELECT 
    source_book_type,
    split_type,
    timing_pattern,
    bet_recommendation,
    total_bets,
    wins,
    
    -- Performance metrics
    ROUND(100.0 * wins / total_bets, 1) as win_rate,
    
    -- ROI calculation for $100 unit bets (assuming -110 odds)
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / (total_bets * 110) * 100, 1) as roi_per_100_unit,
    
    -- Profit per bet
    ROUND(((wins * 100) - ((total_bets - wins) * 110)) / total_bets, 2) as profit_per_bet,
    
    -- Timing metrics
    ROUND(avg_earliest_sharp_hours, 1) as avg_early_hours,
    ROUND(avg_closing_differential, 1) as avg_closing_diff,
    
    -- Strategy effectiveness
    CASE 
        WHEN (100.0 * wins / total_bets) >= 65 AND total_bets >= 8 THEN '🟢 ELITE TIMING'
        WHEN (100.0 * wins / total_bets) >= 60 AND total_bets >= 5 THEN '🟢 EXCELLENT TIMING'
        WHEN (100.0 * wins / total_bets) >= 55 AND total_bets >= 5 THEN '🟡 GOOD TIMING'
        WHEN (100.0 * wins / total_bets) >= 52.4 AND total_bets >= 5 THEN '🟡 PROFITABLE TIMING'
        ELSE '🔴 POOR TIMING'
    END as timing_effectiveness,
    
    -- Specific timing insights
    CASE 
        WHEN timing_pattern = 'EARLY_PERSISTENT_SHARP' THEN 'Follow early sharp money that persists'
        WHEN timing_pattern = 'LATE_SHARP_ACTION' THEN 'Sharp money appears in final hours'
        WHEN timing_pattern = 'STEAM_MOVE' THEN 'Last-minute professional action'
        WHEN timing_pattern = 'CONFLICTING_SHARP' THEN 'Mixed signals - be cautious'
        ELSE 'No clear timing pattern'
    END as timing_insight
    
FROM timing_strategy_performance
WHERE bet_recommendation != 'NO_BET'
  AND total_bets >= 3
ORDER BY 
    (100.0 * wins / total_bets) DESC,
    total_bets DESC; 