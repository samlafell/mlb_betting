-- Sharp Action Analysis Queries
-- Collection of useful queries for analyzing sharp money performance

-- 1. Overall Sharp Action Success Rate
SELECT 
    COUNT(*) as total_sharp_plays,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful_plays,
    SUM(CASE WHEN sharp_success = false THEN 1 ELSE 0 END) as failed_plays,
    ROUND(AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100, 1) as success_rate_pct
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL;

-- 2. Sharp Success Rate by Split Type
SELECT 
    split_type,
    COUNT(*) as total_plays,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN sharp_success = false THEN 1 ELSE 0 END) as failed,
    ROUND(AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100, 1) as success_rate_pct
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL
GROUP BY split_type
ORDER BY success_rate_pct DESC;

-- 3. Best and Worst Sharp Plays
-- Most successful sharp plays
SELECT 
    game_id,
    home_team,
    away_team,
    split_type,
    outcome,
    home_or_over_bets_percentage,
    home_or_over_stake_percentage,
    away_or_under_bets_percentage,
    away_or_under_stake_percentage,
    ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as home_discrepancy,
    ABS(away_or_under_stake_percentage - away_or_under_bets_percentage) as away_discrepancy
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success = true
ORDER BY GREATEST(
    ABS(home_or_over_stake_percentage - home_or_over_bets_percentage),
    ABS(away_or_under_stake_percentage - away_or_under_bets_percentage)
) DESC
LIMIT 5;

-- 4. Sharp Action by Team Performance
-- Which teams have the most sharp action (as favorites/underdogs)
SELECT 
    CASE 
        WHEN home_or_over_stake_percentage > home_or_over_bets_percentage THEN home_team
        WHEN away_or_under_stake_percentage > away_or_under_bets_percentage THEN away_team
        ELSE 'Unknown'
    END as sharp_team,
    COUNT(*) as sharp_plays,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful,
    ROUND(AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100, 1) as success_rate_pct
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL
AND split_type = 'Moneyline'
GROUP BY sharp_team
HAVING COUNT(*) >= 2  -- Only teams with multiple sharp plays
ORDER BY success_rate_pct DESC, sharp_plays DESC;

-- 5. Sharp Money Confidence vs Success Rate
-- Analyze if higher confidence (bigger discrepancy) leads to better results
SELECT 
    CASE 
        WHEN GREATEST(
            ABS(home_or_over_stake_percentage - home_or_over_bets_percentage),
            ABS(away_or_under_stake_percentage - away_or_under_bets_percentage)
        ) >= 30 THEN 'Very High (30+)'
        WHEN GREATEST(
            ABS(home_or_over_stake_percentage - home_or_over_bets_percentage),
            ABS(away_or_under_stake_percentage - away_or_under_bets_percentage)
        ) >= 20 THEN 'High (20-29)'
        ELSE 'Medium (15-19)'
    END as confidence_level,
    COUNT(*) as total_plays,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful,
    ROUND(AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100, 1) as success_rate_pct
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL
GROUP BY confidence_level
ORDER BY 
    CASE confidence_level
        WHEN 'Very High (30+)' THEN 1
        WHEN 'High (20-29)' THEN 2
        ELSE 3
    END;

-- 6. Daily Sharp Performance
SELECT 
    DATE(game_datetime) as game_date,
    COUNT(*) as sharp_plays,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful,
    ROUND(AVG(CASE WHEN sharp_success IS NOT NULL THEN CAST(sharp_success AS INTEGER) END) * 100, 1) as success_rate_pct
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL
GROUP BY DATE(game_datetime)
ORDER BY game_date DESC;

-- 7. Sharp Action Patterns
-- Find games where sharp money was on multiple split types
SELECT 
    game_id,
    home_team,
    away_team,
    outcome,
    COUNT(*) as sharp_split_types,
    SUM(CASE WHEN sharp_success = true THEN 1 ELSE 0 END) as successful_splits,
    STRING_AGG(split_type, ', ') as split_types_with_sharp_action
FROM splits.raw_mlb_betting_splits 
WHERE sharp_action = true 
AND sharp_success IS NOT NULL
GROUP BY game_id, home_team, away_team, outcome
HAVING COUNT(*) > 1  -- Games with sharp action on multiple split types
ORDER BY successful_splits DESC, sharp_split_types DESC; 