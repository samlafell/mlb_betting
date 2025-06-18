-- Correct queries to verify MLB betting splits data

-- 1. Check all tables in the database
SHOW TABLES;

-- 2. Get row count from the main table
SELECT COUNT(*) as total_records FROM splits.raw_mlb_betting_splits;

-- 3. View recent data (last 10 records)
SELECT 
    game_id, 
    home_team, 
    away_team, 
    split_type, 
    source, 
    book,
    last_updated
FROM splits.raw_mlb_betting_splits 
ORDER BY last_updated DESC 
LIMIT 10;

-- 4. View all data (if you want to see everything)
SELECT * FROM splits.raw_mlb_betting_splits;

-- 5. Check data by split type
SELECT 
    split_type, 
    COUNT(*) as count,
    MIN(last_updated) as earliest,
    MAX(last_updated) as latest
FROM splits.raw_mlb_betting_splits 
GROUP BY split_type;

-- 6. Check data by source
SELECT 
    source, 
    COUNT(*) as count
FROM splits.raw_mlb_betting_splits 
GROUP BY source;

-- Verify data in MLB betting splits table (Long Format)

-- 1. Count total records
SELECT COUNT(*) as total_records FROM splits.raw_mlb_betting_splits;

-- 2. Count by split type
SELECT split_type, COUNT(*) as count 
FROM splits.raw_mlb_betting_splits 
GROUP BY split_type;

-- 3. Count by source
SELECT source, COUNT(*) as count 
FROM splits.raw_mlb_betting_splits 
GROUP BY source;

-- 4. Recent entries (last 10 records)
SELECT game_id, home_team, away_team, split_type, source, book, last_updated 
FROM splits.raw_mlb_betting_splits 
ORDER BY last_updated DESC 
LIMIT 10;

-- 5. Check data by split type with timestamps
SELECT 
    split_type, 
    COUNT(*) as count,
    MIN(last_updated) as earliest,
    MAX(last_updated) as latest
FROM splits.raw_mlb_betting_splits 
GROUP BY split_type;

-- 6. Sample data for each split type (Long Format)
SELECT 
    split_type,
    game_id, 
    home_team, 
    away_team, 
    split_value,
    home_or_over_bets_percentage,
    away_or_under_bets_percentage,
    CASE 
        WHEN split_type = 'Total' THEN 'Over'
        ELSE 'Home'
    END as home_or_over_label,
    CASE 
        WHEN split_type = 'Total' THEN 'Under' 
        ELSE 'Away'
    END as away_or_under_label
FROM splits.raw_mlb_betting_splits 
WHERE split_type IN ('Spread', 'Total', 'Moneyline')
ORDER BY split_type, game_id
LIMIT 9;

-- 7. Verify no sparse data (all records should have both home_or_over and away_or_under data)
SELECT 
    split_type,
    COUNT(*) as total_records,
    COUNT(home_or_over_bets) as records_with_home_or_over,
    COUNT(away_or_under_bets) as records_with_away_or_under
FROM splits.raw_mlb_betting_splits 
GROUP BY split_type;

-- 8. View all data (if you want to see everything)
-- SELECT * FROM splits.raw_mlb_betting_splits; 