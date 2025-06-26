-- Late Sharp Flip Strategy (PostgreSQL)
-- Strategy: When sharp money flips direction in the final 2-3 hours before game time,
-- fade the late sharp action and follow the early sharp action
-- Theory: Late flips are often "dumb money" disguised as sharp action

WITH game_timeline AS (
    -- Get all betting data with timeline information
    SELECT 
        s.game_id,
        s.home_team,
        s.away_team,
        s.game_datetime,
        s.split_type,
        s.source,
        s.book,
        s.home_or_over_stake_percentage,
        s.home_or_over_bets_percentage,
        (s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) as differential,
        s.last_updated,
        EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
        -- Classify time periods
        CASE 
            WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4 THEN 'EARLY'
            WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 1 THEN 'LATE'
            ELSE 'VERY_LATE'
        END as time_period,
        ROW_NUMBER() OVER (
            PARTITION BY s.game_id, s.split_type, s.source, s.book 
            ORDER BY s.last_updated ASC
        ) as sequence_number,
        COUNT(*) OVER (
            PARTITION BY s.game_id, s.split_type, s.source, s.book
        ) as total_updates
    FROM splits.raw_mlb_betting_splits s
    WHERE s.split_type = 'moneyline'  -- Focus on moneyline for now
    AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8  -- Minimum sharp threshold
    AND s.game_datetime >= '2024-01-01'  -- Reasonable date range
),

early_late_comparison AS (
    -- Compare early vs late sharp action for each game/source combination
    SELECT 
        gt.game_id,
        gt.home_team,
        gt.away_team,
        gt.game_datetime,
        gt.source,
        gt.book,
        
        -- Early sharp action (first significant reading, 4+ hours before)
        FIRST_VALUE(gt.differential) OVER (
            PARTITION BY gt.game_id, gt.source, gt.book 
            ORDER BY gt.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as early_differential,
        
        FIRST_VALUE(gt.last_updated) OVER (
            PARTITION BY gt.game_id, gt.source, gt.book 
            ORDER BY gt.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as early_timestamp,
        
        -- Late sharp action (final reading, within 3 hours)
        LAST_VALUE(gt.differential) OVER (
            PARTITION BY gt.game_id, gt.source, gt.book 
            ORDER BY gt.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as late_differential,
        
        LAST_VALUE(gt.last_updated) OVER (
            PARTITION BY gt.game_id, gt.source, gt.book 
            ORDER BY gt.last_updated ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as late_timestamp,
        
        gt.total_updates,
        
        ROW_NUMBER() OVER (
            PARTITION BY gt.game_id, gt.source, gt.book 
            ORDER BY gt.last_updated DESC
        ) as latest_rank
    FROM game_timeline gt
    WHERE gt.total_updates >= 3  -- Need multiple updates to detect flip
),

flip_detection AS (
    -- Identify games where sharp action flipped
    SELECT 
        elc.*,
        
        -- Detect flip: early and late have opposite signs and both are significant
        CASE 
            WHEN (elc.early_differential > 0 AND elc.late_differential < 0) 
                 AND ABS(elc.early_differential) >= 10 AND ABS(elc.late_differential) >= 10
            THEN 'HOME_TO_AWAY'
            WHEN (elc.early_differential < 0 AND elc.late_differential > 0)
                 AND ABS(elc.early_differential) >= 10 AND ABS(elc.late_differential) >= 10  
            THEN 'AWAY_TO_HOME'
            ELSE 'NO_FLIP'
        END as flip_type,
        
        -- Determine early and late sharp sides
        CASE WHEN elc.early_differential > 0 THEN 'HOME' ELSE 'AWAY' END as early_sharp_side,
        CASE WHEN elc.late_differential > 0 THEN 'HOME' ELSE 'AWAY' END as late_sharp_side,
        
        -- Time gap between early and late readings
        EXTRACT(EPOCH FROM (elc.late_timestamp - elc.early_timestamp))/3600 as hours_between_readings
        
    FROM early_late_comparison elc
    WHERE elc.latest_rank = 1  -- Only get one record per game/source
),

game_outcomes_joined AS (
    -- Join with game outcomes
    SELECT 
        fd.*,
        go.home_score,
        go.away_score,
        
        -- Determine actual winner
        CASE 
            WHEN go.home_score > go.away_score THEN 'HOME'
            WHEN go.away_score > go.home_score THEN 'AWAY'
            ELSE 'TIE'
        END as actual_winner,
        
        -- Strategy recommendations
        CASE 
            WHEN fd.flip_type = 'HOME_TO_AWAY' THEN 'AWAY'  -- Fade late flip, follow early
            WHEN fd.flip_type = 'AWAY_TO_HOME' THEN 'HOME'  -- Fade late flip, follow early  
            ELSE NULL
        END as fade_late_flip_pick,
        
        fd.early_sharp_side as follow_early_pick,
        fd.late_sharp_side as follow_late_pick
        
    FROM flip_detection fd
    LEFT JOIN game_outcomes go ON fd.game_id = go.game_id
    WHERE fd.flip_type != 'NO_FLIP'  -- Only analyze games with flips
    AND go.home_score IS NOT NULL   -- Only completed games
    AND go.away_score IS NOT NULL
),

strategy_results AS (
    -- Calculate strategy performance
    SELECT 
        goj.*,
        
        -- Fade Late Flip strategy results
        CASE 
            WHEN goj.fade_late_flip_pick = goj.actual_winner THEN 1 
            ELSE 0 
        END as fade_late_flip_win,
        
        -- Follow Early Sharp strategy results  
        CASE 
            WHEN goj.follow_early_pick = goj.actual_winner THEN 1
            ELSE 0
        END as follow_early_win,
        
        -- Follow Late Sharp strategy results
        CASE 
            WHEN goj.follow_late_pick = goj.actual_winner THEN 1
            ELSE 0  
        END as follow_late_win,
        
        -- Categorize flip strength
        CASE 
            WHEN ABS(goj.early_differential) >= 20 AND ABS(goj.late_differential) >= 20 THEN 'STRONG_FLIP'
            WHEN ABS(goj.early_differential) >= 15 OR ABS(goj.late_differential) >= 15 THEN 'MEDIUM_FLIP'
            ELSE 'WEAK_FLIP'
        END as flip_strength,
        
        -- Time-based categories
        CASE 
            WHEN goj.hours_between_readings >= 6 THEN 'LONG_DEVELOPMENT'
            WHEN goj.hours_between_readings >= 3 THEN 'MEDIUM_DEVELOPMENT'  
            ELSE 'QUICK_FLIP'
        END as flip_timing
        
    FROM game_outcomes_joined goj
)

-- Final strategy analysis by source AND book combination
SELECT 
    'LATE_SHARP_FLIP_FADE' as strategy_name,
    sr.source as source,
    COALESCE(sr.book, 'NULL') as book,
    sr.source || '-' || COALESCE(sr.book, 'NULL') as source_book_combo,
    'moneyline' as split_type,
    
    -- Overall performance
    COUNT(*) as total_games,
    SUM(sr.fade_late_flip_win) as fade_late_wins,
    ROUND(AVG(sr.fade_late_flip_win::decimal), 3) as fade_late_win_rate,
    
    -- Comparison strategies
    SUM(sr.follow_early_win) as follow_early_wins,
    ROUND(AVG(sr.follow_early_win::decimal), 3) as follow_early_win_rate,
    
    SUM(sr.follow_late_win) as follow_late_wins, 
    ROUND(AVG(sr.follow_late_win::decimal), 3) as follow_late_win_rate,
    
    -- Performance by flip type
    COUNT(*) FILTER (WHERE sr.flip_type = 'HOME_TO_AWAY') as home_to_away_flips,
    ROUND(AVG(sr.fade_late_flip_win::decimal) FILTER (WHERE sr.flip_type = 'HOME_TO_AWAY'), 3) as home_to_away_win_rate,
    
    COUNT(*) FILTER (WHERE sr.flip_type = 'AWAY_TO_HOME') as away_to_home_flips,
    ROUND(AVG(sr.fade_late_flip_win::decimal) FILTER (WHERE sr.flip_type = 'AWAY_TO_HOME'), 3) as away_to_home_win_rate,
    
    -- Performance by flip strength
    COUNT(*) FILTER (WHERE sr.flip_strength = 'STRONG_FLIP') as strong_flips,
    ROUND(AVG(sr.fade_late_flip_win::decimal) FILTER (WHERE sr.flip_strength = 'STRONG_FLIP'), 3) as strong_flip_win_rate,
    
    COUNT(*) FILTER (WHERE sr.flip_strength = 'MEDIUM_FLIP') as medium_flips,
    ROUND(AVG(sr.fade_late_flip_win::decimal) FILTER (WHERE sr.flip_strength = 'MEDIUM_FLIP'), 3) as medium_flip_win_rate,
    
    -- Performance by timing
    COUNT(*) FILTER (WHERE sr.flip_timing = 'QUICK_FLIP') as quick_flips,
    ROUND(AVG(sr.fade_late_flip_win::decimal) FILTER (WHERE sr.flip_timing = 'QUICK_FLIP'), 3) as quick_flip_win_rate,
    
    -- ROI calculations (assuming -110 odds)
    ROUND((SUM(sr.fade_late_flip_win) * 0.909 - (COUNT(*) - SUM(sr.fade_late_flip_win))) * 100.0 / COUNT(*), 2) as fade_late_roi_per_100,
    
    -- Sample recent games for validation
    STRING_AGG(
        CASE WHEN sr.game_datetime >= CURRENT_DATE - INTERVAL '30 days' 
        THEN sr.home_team || ' vs ' || sr.away_team || ' (' || 
             CASE WHEN sr.fade_late_flip_win = 1 THEN 'W' ELSE 'L' END || ')'
        ELSE NULL END, 
        ', ' 
        ORDER BY sr.game_datetime DESC
    ) as recent_examples

FROM strategy_results sr
GROUP BY sr.source, sr.book
HAVING COUNT(*) >= 5  -- Minimum sample size
ORDER BY fade_late_win_rate DESC, total_games DESC; 