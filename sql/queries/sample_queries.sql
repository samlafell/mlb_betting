-- Sample queries for MLB betting splits analysis

-- Get all splits for a specific game
SELECT * FROM MLB_BETTING_SPLITS 
WHERE game_id = 'your_game_id_here';

-- Get betting splits by source
SELECT source, COUNT(*) as split_count
FROM MLB_BETTING_SPLITS 
GROUP BY source;

-- Get average betting percentages by split type
SELECT 
    split_type,
    AVG(home_team_bets_percentage) as avg_home_pct,
    AVG(away_team_bets_percentage) as avg_away_pct
FROM MLB_BETTING_SPLITS 
WHERE split_type IN ('Spread', 'Moneyline')
GROUP BY split_type;

-- Get over/under betting trends
SELECT 
    AVG(over_bets_percentage) as avg_over_pct,
    AVG(under_bets_percentage) as avg_under_pct
FROM MLB_BETTING_SPLITS 
WHERE split_type = 'Total';

-- Get recent splits (last 24 hours)
SELECT game_id, home_team, away_team, split_type, last_updated
FROM MLB_BETTING_SPLITS 
WHERE last_updated >= NOW() - INTERVAL '24 hours'
ORDER BY last_updated DESC; 