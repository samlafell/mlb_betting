-- Add game information columns to mlb_betting tables for easier identification
-- This script adds game_datetime, home_team, and away_team columns to make betting data more accessible

-- ==============================================================================
-- ADD COLUMNS TO MLB_BETTING TABLES
-- ==============================================================================

-- Add columns to moneyline table
ALTER TABLE mlb_betting.moneyline 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to spreads table
ALTER TABLE mlb_betting.spreads 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to totals table
ALTER TABLE mlb_betting.totals 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- ==============================================================================
-- POPULATE THE NEW COLUMNS WITH EXISTING DATA
-- ==============================================================================

-- Update moneyline table
UPDATE mlb_betting.moneyline 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM public.games g
WHERE mlb_betting.moneyline.game_id = g.id
AND mlb_betting.moneyline.game_datetime IS NULL;

-- Update spreads table
UPDATE mlb_betting.spreads 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM public.games g
WHERE mlb_betting.spreads.game_id = g.id
AND mlb_betting.spreads.game_datetime IS NULL;

-- Update totals table
UPDATE mlb_betting.totals 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM public.games g
WHERE mlb_betting.totals.game_id = g.id
AND mlb_betting.totals.game_datetime IS NULL;

-- ==============================================================================
-- CREATE INDEXES FOR BETTER QUERY PERFORMANCE
-- ==============================================================================

-- Indexes for moneyline table
CREATE INDEX IF NOT EXISTS idx_moneyline_game_datetime ON mlb_betting.moneyline(game_datetime);
CREATE INDEX IF NOT EXISTS idx_moneyline_teams ON mlb_betting.moneyline(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_moneyline_game_date ON mlb_betting.moneyline(DATE(game_datetime));

-- Indexes for spreads table
CREATE INDEX IF NOT EXISTS idx_spreads_game_datetime ON mlb_betting.spreads(game_datetime);
CREATE INDEX IF NOT EXISTS idx_spreads_teams ON mlb_betting.spreads(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_spreads_game_date ON mlb_betting.spreads(DATE(game_datetime));

-- Indexes for totals table
CREATE INDEX IF NOT EXISTS idx_totals_game_datetime ON mlb_betting.totals(game_datetime);
CREATE INDEX IF NOT EXISTS idx_totals_teams ON mlb_betting.totals(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_totals_game_date ON mlb_betting.totals(DATE(game_datetime));

-- ==============================================================================
-- CREATE TRIGGER FUNCTIONS TO AUTOMATICALLY POPULATE NEW COLUMNS
-- ==============================================================================

-- Function to automatically populate game info when inserting betting data
CREATE OR REPLACE FUNCTION populate_game_info()
RETURNS TRIGGER AS $$
BEGIN
    -- Get game information from public.games table
    SELECT game_datetime, home_team, away_team
    INTO NEW.game_datetime, NEW.home_team, NEW.away_team
    FROM public.games
    WHERE id = NEW.game_id;
    
    -- If no game found, keep NULL values but log a warning
    IF NEW.game_datetime IS NULL THEN
        RAISE WARNING 'No game found for game_id: %', NEW.game_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for each table
DROP TRIGGER IF EXISTS trigger_populate_moneyline_game_info ON mlb_betting.moneyline;
CREATE TRIGGER trigger_populate_moneyline_game_info
    BEFORE INSERT ON mlb_betting.moneyline
    FOR EACH ROW
    EXECUTE FUNCTION populate_game_info();

DROP TRIGGER IF EXISTS trigger_populate_spreads_game_info ON mlb_betting.spreads;
CREATE TRIGGER trigger_populate_spreads_game_info
    BEFORE INSERT ON mlb_betting.spreads
    FOR EACH ROW
    EXECUTE FUNCTION populate_game_info();

DROP TRIGGER IF EXISTS trigger_populate_totals_game_info ON mlb_betting.totals;
CREATE TRIGGER trigger_populate_totals_game_info
    BEFORE INSERT ON mlb_betting.totals
    FOR EACH ROW
    EXECUTE FUNCTION populate_game_info();

-- ==============================================================================
-- VERIFICATION QUERIES
-- ==============================================================================

-- Check that the columns were added and populated
SELECT 
    'moneyline' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM mlb_betting.moneyline

UNION ALL

SELECT 
    'spreads' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM mlb_betting.spreads

UNION ALL

SELECT 
    'totals' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM mlb_betting.totals;

-- Sample data to verify the changes
SELECT 
    'moneyline' as table_name,
    game_datetime,
    home_team,
    away_team,
    sportsbook,
    home_ml,
    away_ml
FROM mlb_betting.moneyline 
WHERE game_datetime IS NOT NULL
ORDER BY game_datetime DESC
LIMIT 5;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON COLUMN mlb_betting.moneyline.game_datetime IS 'Game datetime for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.moneyline.home_team IS 'Home team abbreviation for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.moneyline.away_team IS 'Away team abbreviation for easier identification (denormalized from public.games)';

COMMENT ON COLUMN mlb_betting.spreads.game_datetime IS 'Game datetime for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.spreads.home_team IS 'Home team abbreviation for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.spreads.away_team IS 'Away team abbreviation for easier identification (denormalized from public.games)';

COMMENT ON COLUMN mlb_betting.totals.game_datetime IS 'Game datetime for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.totals.home_team IS 'Home team abbreviation for easier identification (denormalized from public.games)';
COMMENT ON COLUMN mlb_betting.totals.away_team IS 'Away team abbreviation for easier identification (denormalized from public.games)';

COMMENT ON FUNCTION populate_game_info() IS 'Trigger function to automatically populate game_datetime, home_team, and away_team columns when inserting betting data'; 