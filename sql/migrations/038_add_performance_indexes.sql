-- Migration: Add Performance Indexes for Games Population Service
-- File: 038_add_performance_indexes.sql
-- Description: Adds composite indexes to improve performance for games population operations

-- Composite index for team and date matching (Action Network ID population)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_games_complete_team_date 
ON curated.games_complete(home_team, away_team, game_date)
WHERE action_network_game_id IS NULL;

-- Index for Action Network game matching (venue/weather population)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_action_network_games_external_id 
ON raw_data.action_network_games(external_game_id)
WHERE external_game_id IS NOT NULL AND external_game_id != '';

-- Index for games without scores (score population)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_games_complete_missing_scores 
ON curated.games_complete(id)
WHERE home_score IS NULL;

-- Index for games without venue data (venue population)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_games_complete_missing_venue 
ON curated.games_complete(action_network_game_id)
WHERE venue_name IS NULL AND action_network_game_id IS NOT NULL;

-- Index for games without weather data (weather population)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_games_complete_missing_weather 
ON curated.games_complete(action_network_game_id)
WHERE weather_condition IS NULL AND action_network_game_id IS NOT NULL;

-- Index for recent updates (data quality updates)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_games_complete_recent_updates 
ON curated.games_complete(updated_at)
WHERE updated_at >= (NOW() - INTERVAL '1 hour');

-- Index for game outcomes lookup
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_game_outcomes_game_id_scores 
ON curated.game_outcomes(game_id)
WHERE home_score IS NOT NULL AND away_score IS NOT NULL;

-- Verify index creation
DO $$
BEGIN
    RAISE NOTICE 'Performance indexes created successfully for games population service';
    RAISE NOTICE 'Indexes created:';
    RAISE NOTICE '  - idx_games_complete_team_date: Team and date matching';
    RAISE NOTICE '  - idx_action_network_games_external_id: External ID lookup';
    RAISE NOTICE '  - idx_games_complete_missing_scores: Missing scores filter';
    RAISE NOTICE '  - idx_games_complete_missing_venue: Missing venue filter';
    RAISE NOTICE '  - idx_games_complete_missing_weather: Missing weather filter';
    RAISE NOTICE '  - idx_games_complete_recent_updates: Recent updates filter';
    RAISE NOTICE '  - idx_game_outcomes_game_id_scores: Game outcomes lookup';
END $$;