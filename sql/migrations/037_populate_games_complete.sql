-- Migration: 037_populate_games_complete.sql
-- Purpose: Populate missing critical fields in curated.games_complete table
-- Issue: #70 - Fix games_complete Table Population
-- Date: 2025-08-17

-- Description:
-- This migration addresses the critical data gaps in curated.games_complete where
-- all games are missing home_score, away_score, action_network_game_id, venue_name,
-- and weather data. The migration uses available data from curated.game_outcomes
-- and raw_data.action_network_games to populate these fields.

BEGIN;

-- Phase 1: Populate game scores from curated.game_outcomes
-- This addresses the critical missing home_score and away_score fields
UPDATE curated.games_complete gc
SET 
    home_score = go.home_score,
    away_score = go.away_score,
    winning_team = CASE 
        WHEN go.home_score > go.away_score THEN gc.home_team
        WHEN go.away_score > go.home_score THEN gc.away_team
        ELSE NULL -- tie game (rare but possible)
    END,
    game_status = 'completed',
    updated_at = NOW()
FROM curated.game_outcomes go
WHERE gc.id = go.game_id;

-- Phase 2: Populate Action Network game IDs and venue data
-- Match based on team abbreviations and game date
UPDATE curated.games_complete gc
SET 
    action_network_game_id = ang.external_game_id::integer,
    updated_at = NOW()
FROM raw_data.action_network_games ang
WHERE gc.home_team = ang.home_team_abbr 
  AND gc.away_team = ang.away_team_abbr
  AND gc.game_date = ang.game_date
  AND ang.external_game_id IS NOT NULL
  AND ang.external_game_id != '';

-- Phase 3: Extract venue information from Action Network raw JSON data
-- Extract venue from raw_game_data JSON if available
UPDATE curated.games_complete gc
SET 
    venue_name = (ang.raw_game_data->>'venue_name'),
    venue_id = CASE 
        WHEN (ang.raw_game_data->>'venue_id') ~ '^[0-9]+$' 
        THEN (ang.raw_game_data->>'venue_id')::integer
        ELSE NULL
    END,
    updated_at = NOW()
FROM raw_data.action_network_games ang
WHERE gc.action_network_game_id::text = ang.external_game_id
  AND ang.raw_game_data IS NOT NULL
  AND ang.raw_game_data->>'venue_name' IS NOT NULL;

-- Phase 4: Extract weather data from Action Network raw JSON data
-- Extract weather information if available in the JSON
UPDATE curated.games_complete gc
SET 
    weather_condition = (ang.raw_game_data->'weather'->>'condition'),
    temperature = CASE 
        WHEN (ang.raw_game_data->'weather'->>'temperature') ~ '^[0-9]+$' 
        THEN (ang.raw_game_data->'weather'->>'temperature')::integer
        ELSE NULL
    END,
    wind_speed = CASE 
        WHEN (ang.raw_game_data->'weather'->>'wind_speed') ~ '^[0-9]+$' 
        THEN (ang.raw_game_data->'weather'->>'wind_speed')::integer
        ELSE NULL
    END,
    wind_direction = (ang.raw_game_data->'weather'->>'wind_direction'),
    humidity = CASE 
        WHEN (ang.raw_game_data->'weather'->>'humidity') ~ '^[0-9]+$' 
        THEN (ang.raw_game_data->'weather'->>'humidity')::integer
        ELSE NULL
    END,
    updated_at = NOW()
FROM raw_data.action_network_games ang
WHERE gc.action_network_game_id::text = ang.external_game_id
  AND ang.raw_game_data IS NOT NULL
  AND ang.raw_game_data->'weather' IS NOT NULL;

-- Phase 5: Update data quality indicators
-- Mark games with complete data as HIGH quality
UPDATE curated.games_complete
SET 
    data_quality = CASE 
        WHEN home_score IS NOT NULL 
         AND away_score IS NOT NULL 
         AND action_network_game_id IS NOT NULL 
         AND venue_name IS NOT NULL 
        THEN 'HIGH'
        WHEN home_score IS NOT NULL 
         AND away_score IS NOT NULL 
         AND action_network_game_id IS NOT NULL 
        THEN 'MEDIUM'
        WHEN home_score IS NOT NULL 
         AND away_score IS NOT NULL 
        THEN 'LOW'
        ELSE 'MINIMAL'
    END,
    has_mlb_enrichment = CASE 
        WHEN action_network_game_id IS NOT NULL THEN true
        ELSE false
    END,
    mlb_correlation_confidence = CASE 
        WHEN home_score IS NOT NULL 
         AND away_score IS NOT NULL 
         AND action_network_game_id IS NOT NULL 
        THEN 0.9500
        WHEN home_score IS NOT NULL 
         AND away_score IS NOT NULL 
        THEN 0.8000
        ELSE 0.5000
    END,
    updated_at = NOW()
WHERE updated_at >= (NOW() - INTERVAL '1 hour'); -- Only update recently modified records

-- Create validation view for monitoring
CREATE OR REPLACE VIEW analytics.games_complete_data_quality AS
SELECT 
    COUNT(*) as total_games,
    COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores,
    COUNT(CASE WHEN action_network_game_id IS NOT NULL THEN 1 END) as games_with_external_ids,
    COUNT(CASE WHEN venue_name IS NOT NULL THEN 1 END) as games_with_venue,
    COUNT(CASE WHEN weather_condition IS NOT NULL THEN 1 END) as games_with_weather,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_games,
    COUNT(CASE WHEN data_quality = 'MEDIUM' THEN 1 END) as medium_quality_games,
    COUNT(CASE WHEN data_quality = 'LOW' THEN 1 END) as low_quality_games,
    COUNT(CASE WHEN data_quality = 'MINIMAL' THEN 1 END) as minimal_quality_games,
    MIN(game_date) as earliest_game,
    MAX(game_date) as latest_game,
    NOW() as last_updated
FROM curated.games_complete;

-- Grant access to the new view
GRANT SELECT ON analytics.games_complete_data_quality TO PUBLIC;

-- Add helpful indexes for performance
CREATE INDEX IF NOT EXISTS idx_games_complete_action_network_id 
ON curated.games_complete(action_network_game_id) 
WHERE action_network_game_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_complete_scores 
ON curated.games_complete(home_score, away_score) 
WHERE home_score IS NOT NULL AND away_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_complete_data_quality 
ON curated.games_complete(data_quality);

COMMIT;

-- Post-migration validation queries
-- These should be run after the migration to verify success

-- Validation Query 1: Overall data completeness
-- Expected: All games should have scores, most should have Action Network IDs
/*
SELECT 
    total_games,
    games_with_scores,
    games_with_external_ids,
    games_with_venue,
    games_with_weather,
    high_quality_games,
    medium_quality_games,
    low_quality_games,
    minimal_quality_games
FROM analytics.games_complete_data_quality;
*/

-- Validation Query 2: Data quality distribution
-- Expected: Majority should be HIGH or MEDIUM quality
/*
SELECT 
    data_quality,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM curated.games_complete
GROUP BY data_quality
ORDER BY count DESC;
*/

-- Validation Query 3: Sample of populated data
-- Expected: Show actual populated fields
/*
SELECT 
    id,
    home_team,
    away_team,
    home_score,
    away_score,
    action_network_game_id,
    venue_name,
    weather_condition,
    data_quality
FROM curated.games_complete
WHERE home_score IS NOT NULL
LIMIT 10;
*/