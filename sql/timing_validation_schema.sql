-- Timing Validation Schema Changes for PostgreSQL
-- Supports 5-minute grace period validation for betting splits

-- =======================
-- COMPOSITE INDEXES
-- =======================

-- Create composite index for efficient game start time queries
-- This supports fast lookups when validating if games started more than 5 minutes ago
CREATE INDEX IF NOT EXISTS idx_games_start_time 
ON splits.games(game_datetime, created_at);

-- Create composite index for betting splits timing validation
-- This supports efficient queries for recent splits by game timing
CREATE INDEX IF NOT EXISTS idx_splits_game_timing 
ON splits.raw_mlb_betting_splits(game_datetime, last_updated, game_id);

-- Create index for game status tracking (for MLB API integration)
CREATE INDEX IF NOT EXISTS idx_games_status 
ON splits.games(status, game_datetime) 
WHERE status IS NOT NULL;

-- =======================
-- CONSTRAINTS
-- =======================

-- Ensure game_datetime is not null for timing validation
-- This prevents splits without valid game times from being stored
ALTER TABLE splits.raw_mlb_betting_splits 
ADD CONSTRAINT chk_game_datetime_not_null 
CHECK (game_datetime IS NOT NULL);

-- Ensure game_datetime is not null for games table
ALTER TABLE splits.games 
ADD CONSTRAINT chk_game_datetime_not_null 
CHECK (game_datetime IS NOT NULL);

-- Ensure game_datetime is reasonable (not too far in future or past)
-- Allow up to 30 days in future, 1 year in past for historical data
ALTER TABLE splits.raw_mlb_betting_splits 
ADD CONSTRAINT chk_game_datetime_reasonable 
CHECK (
    game_datetime >= CURRENT_DATE - INTERVAL '1 year' 
    AND game_datetime <= CURRENT_DATE + INTERVAL '30 days'
);

-- Ensure last_updated is reasonable and not in future
ALTER TABLE splits.raw_mlb_betting_splits 
ADD CONSTRAINT chk_last_updated_reasonable 
CHECK (
    last_updated >= CURRENT_DATE - INTERVAL '1 year' 
    AND last_updated <= CURRENT_TIMESTAMP + INTERVAL '1 hour'
);

-- =======================
-- VALIDATION FUNCTIONS
-- =======================

-- Function to check if a game is within the grace period
CREATE OR REPLACE FUNCTION is_within_grace_period(
    game_start_time TIMESTAMP WITH TIME ZONE,
    grace_period_minutes INTEGER DEFAULT 5
) RETURNS BOOLEAN AS $$
BEGIN
    -- Return true if game hasn't started or started within grace period
    RETURN (
        game_start_time > CURRENT_TIMESTAMP OR 
        CURRENT_TIMESTAMP - game_start_time <= INTERVAL '1 minute' * grace_period_minutes
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get grace period status for a game
CREATE OR REPLACE FUNCTION get_grace_period_status(
    game_start_time TIMESTAMP WITH TIME ZONE,
    grace_period_minutes INTEGER DEFAULT 5
) RETURNS TEXT AS $$
BEGIN
    IF game_start_time IS NULL THEN
        RETURN 'invalid_time';
    ELSIF game_start_time > CURRENT_TIMESTAMP THEN
        RETURN 'future_game';
    ELSIF CURRENT_TIMESTAMP - game_start_time <= INTERVAL '1 minute' * grace_period_minutes THEN
        RETURN 'within_grace_period';
    ELSE
        RETURN 'expired';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =======================
-- MONITORING VIEWS
-- =======================

-- View for monitoring timing validation rejection rates
CREATE OR REPLACE VIEW timing_validation_monitor AS
SELECT 
    DATE(game_datetime) as game_date,
    COUNT(*) as total_splits,
    COUNT(*) FILTER (WHERE is_within_grace_period(game_datetime)) as valid_splits,
    COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime)) as expired_splits,
    ROUND(
        COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime))::NUMERIC / 
        COUNT(*)::NUMERIC * 100, 2
    ) as rejection_rate_percent,
    MIN(game_datetime) as earliest_game,
    MAX(game_datetime) as latest_game,
    MAX(last_updated) as last_data_update
FROM splits.raw_mlb_betting_splits 
WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(game_datetime)
ORDER BY game_date DESC;

-- View for current game timing status
CREATE OR REPLACE VIEW current_game_timing_status AS
SELECT 
    g.game_id,
    g.home_team,
    g.away_team,
    g.game_datetime,
    g.status,
    get_grace_period_status(g.game_datetime) as timing_status,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - g.game_datetime))/60 as minutes_since_start,
    COUNT(s.id) as split_count,
    MAX(s.last_updated) as latest_split_update
FROM splits.games g
LEFT JOIN splits.raw_mlb_betting_splits s ON g.game_id = s.game_id
WHERE g.game_datetime >= CURRENT_DATE - INTERVAL '1 day'
  AND g.game_datetime <= CURRENT_DATE + INTERVAL '1 day'
GROUP BY g.game_id, g.home_team, g.away_team, g.game_datetime, g.status
ORDER BY g.game_datetime;

-- =======================
-- CLEANUP FUNCTION
-- =======================

-- Function to clean up expired splits older than specified days
CREATE OR REPLACE FUNCTION cleanup_expired_splits(
    retention_days INTEGER DEFAULT 30
) RETURNS TABLE(
    deleted_splits INTEGER,
    deleted_games INTEGER,
    cleanup_timestamp TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
    splits_deleted INTEGER := 0;
    games_deleted INTEGER := 0;
    cutoff_date TIMESTAMP WITH TIME ZONE;
BEGIN
    cutoff_date := CURRENT_TIMESTAMP - INTERVAL '1 day' * retention_days;
    
    -- Delete old betting splits
    DELETE FROM splits.raw_mlb_betting_splits 
    WHERE last_updated < cutoff_date;
    
    GET DIAGNOSTICS splits_deleted = ROW_COUNT;
    
    -- Delete orphaned games (games with no associated splits)
    DELETE FROM splits.games g
    WHERE g.game_datetime < cutoff_date
      AND NOT EXISTS (
          SELECT 1 FROM splits.raw_mlb_betting_splits s 
          WHERE s.game_id = g.game_id
      );
    
    GET DIAGNOSTICS games_deleted = ROW_COUNT;
    
    RETURN QUERY SELECT splits_deleted, games_deleted, CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- =======================
-- SAMPLE QUERIES
-- =======================

-- Check current rejection rate for today's data
/*
SELECT 
    COUNT(*) as total_splits,
    COUNT(*) FILTER (WHERE is_within_grace_period(game_datetime)) as valid_splits,
    COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime)) as expired_splits,
    ROUND(
        COUNT(*) FILTER (WHERE NOT is_within_grace_period(game_datetime))::NUMERIC / 
        COUNT(*)::NUMERIC * 100, 2
    ) as rejection_rate_percent
FROM splits.raw_mlb_betting_splits 
WHERE DATE(last_updated) = CURRENT_DATE;
*/

-- Find splits for games that started more than 5 minutes ago
/*
SELECT 
    game_id,
    home_team,
    away_team,
    game_datetime,
    last_updated,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - game_datetime))/60 as minutes_since_start
FROM splits.raw_mlb_betting_splits 
WHERE NOT is_within_grace_period(game_datetime)
ORDER BY game_datetime DESC;
*/

-- Monitor timing validation performance
/*
SELECT * FROM timing_validation_monitor 
WHERE game_date >= CURRENT_DATE - INTERVAL '7 days';
*/ 