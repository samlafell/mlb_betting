-- Migration: Add readable game information columns to raw_data.action_network_games
-- Purpose: Improve data accessibility by extracting key game details from JSON
-- Date: 2025-07-23
-- Issue: Users can't identify games from raw_data.action_network_games without parsing JSON

BEGIN;

-- Add readable columns for game identification
ALTER TABLE raw_data.action_network_games 
ADD COLUMN IF NOT EXISTS home_team VARCHAR(255),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(255),
ADD COLUMN IF NOT EXISTS home_team_abbr VARCHAR(10),
ADD COLUMN IF NOT EXISTS away_team_abbr VARCHAR(10),
ADD COLUMN IF NOT EXISTS game_status VARCHAR(50),
ADD COLUMN IF NOT EXISTS start_time TIMESTAMP WITH TIME ZONE;

-- Create index for common game lookups
CREATE INDEX IF NOT EXISTS idx_action_network_games_teams 
ON raw_data.action_network_games (home_team, away_team);

CREATE INDEX IF NOT EXISTS idx_action_network_games_date_status 
ON raw_data.action_network_games (game_date, game_status);

CREATE INDEX IF NOT EXISTS idx_action_network_games_start_time 
ON raw_data.action_network_games (start_time);

-- Add comments for documentation
COMMENT ON COLUMN raw_data.action_network_games.home_team IS 'Full name of home team (e.g., Cincinnati Reds)';
COMMENT ON COLUMN raw_data.action_network_games.away_team IS 'Full name of away team (e.g., Washington Nationals)';
COMMENT ON COLUMN raw_data.action_network_games.home_team_abbr IS 'Home team abbreviation (e.g., CIN)';
COMMENT ON COLUMN raw_data.action_network_games.away_team_abbr IS 'Away team abbreviation (e.g., WSH)';
COMMENT ON COLUMN raw_data.action_network_games.game_status IS 'Game status (scheduled, complete, in_progress, etc.)';
COMMENT ON COLUMN raw_data.action_network_games.start_time IS 'Scheduled game start time in UTC';

-- Update existing records to populate new columns from raw_response JSON
-- This backfills data for existing records
UPDATE raw_data.action_network_games 
SET 
    home_team = CASE 
        WHEN jsonb_array_length(raw_response->'teams') >= 2 
        THEN COALESCE(
            raw_response->'teams'->1->>'full_name',
            raw_response->'teams'->1->>'display_name'
        )
        ELSE NULL 
    END,
    away_team = CASE 
        WHEN jsonb_array_length(raw_response->'teams') >= 2 
        THEN COALESCE(
            raw_response->'teams'->0->>'full_name',
            raw_response->'teams'->0->>'display_name'
        )
        ELSE NULL 
    END,
    home_team_abbr = CASE 
        WHEN jsonb_array_length(raw_response->'teams') >= 2 
        THEN COALESCE(
            raw_response->'teams'->1->>'abbr',
            raw_response->'teams'->1->>'abbreviation'
        )
        ELSE NULL 
    END,
    away_team_abbr = CASE 
        WHEN jsonb_array_length(raw_response->'teams') >= 2 
        THEN COALESCE(
            raw_response->'teams'->0->>'abbr',
            raw_response->'teams'->0->>'abbreviation'
        )
        ELSE NULL 
    END,
    game_status = raw_response->>'status',
    start_time = CASE 
        WHEN raw_response->>'start_time' IS NOT NULL 
        THEN (raw_response->>'start_time')::timestamp with time zone
        ELSE NULL 
    END
WHERE home_team IS NULL 
  AND raw_response ? 'teams'
  AND jsonb_typeof(raw_response->'teams') = 'array';

-- Create a view for easy game identification
CREATE OR REPLACE VIEW raw_data.v_action_network_games_readable AS
SELECT 
    external_game_id,
    CASE 
        WHEN away_team IS NOT NULL AND home_team IS NOT NULL 
        THEN away_team || ' @ ' || home_team
        ELSE 'Game ' || external_game_id
    END as game_description,
    CASE 
        WHEN away_team_abbr IS NOT NULL AND home_team_abbr IS NOT NULL 
        THEN away_team_abbr || ' @ ' || home_team_abbr
        ELSE COALESCE(away_team_abbr || ' @ ' || home_team_abbr, 'TBD')
    END as game_short,
    away_team,
    home_team,
    away_team_abbr,
    home_team_abbr,
    game_status,
    start_time,
    game_date,
    collected_at,
    processed_at
FROM raw_data.action_network_games
ORDER BY start_time DESC NULLS LAST, collected_at DESC;

COMMENT ON VIEW raw_data.v_action_network_games_readable IS 'User-friendly view of Action Network games with readable team names and descriptions';

COMMIT;