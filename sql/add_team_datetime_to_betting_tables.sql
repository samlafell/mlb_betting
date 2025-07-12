-- Add team and datetime columns to betting tables for easier identification
-- This script adds home_team, away_team, and game_datetime columns to make betting data more accessible

-- ==============================================================================
-- ADD COLUMNS TO CORE_BETTING TABLES
-- ==============================================================================

-- Add columns to betting_lines_moneyline table
ALTER TABLE core_betting.betting_lines_moneyline 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_lines_spreads table
ALTER TABLE core_betting.betting_lines_spreads 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_lines_totals table
ALTER TABLE core_betting.betting_lines_totals 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_splits table
ALTER TABLE core_betting.betting_splits 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to line_movements table
ALTER TABLE core_betting.line_movements 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to steam_moves table
ALTER TABLE core_betting.steam_moves 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- ==============================================================================
-- POPULATE THE NEW COLUMNS WITH EXISTING DATA
-- ==============================================================================

-- Update betting_lines_moneyline table
UPDATE core_betting.betting_lines_moneyline 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.betting_lines_moneyline.game_id = g.id
AND core_betting.betting_lines_moneyline.game_datetime IS NULL;

-- Update betting_lines_spreads table
UPDATE core_betting.betting_lines_spreads 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.betting_lines_spreads.game_id = g.id
AND core_betting.betting_lines_spreads.game_datetime IS NULL;

-- Update betting_lines_totals table
UPDATE core_betting.betting_lines_totals 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.betting_lines_totals.game_id = g.id
AND core_betting.betting_lines_totals.game_datetime IS NULL;

-- Update betting_splits table
UPDATE core_betting.betting_splits 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.betting_splits.game_id = g.id
AND core_betting.betting_splits.game_datetime IS NULL;

-- Update line_movements table
UPDATE core_betting.line_movements 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.line_movements.game_id = g.id
AND core_betting.line_movements.game_datetime IS NULL;

-- Update steam_moves table
UPDATE core_betting.steam_moves 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM core_betting.games g
WHERE core_betting.steam_moves.game_id = g.id
AND core_betting.steam_moves.game_datetime IS NULL;

-- ==============================================================================
-- CREATE INDEXES FOR BETTER PERFORMANCE
-- ==============================================================================

-- Indexes for betting_lines_moneyline
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_game_datetime ON core_betting.betting_lines_moneyline(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_teams ON core_betting.betting_lines_moneyline(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_home_team ON core_betting.betting_lines_moneyline(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_away_team ON core_betting.betting_lines_moneyline(away_team);

-- Indexes for betting_lines_spreads
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_game_datetime ON core_betting.betting_lines_spreads(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_teams ON core_betting.betting_lines_spreads(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_home_team ON core_betting.betting_lines_spreads(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_away_team ON core_betting.betting_lines_spreads(away_team);

-- Indexes for betting_lines_totals
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_game_datetime ON core_betting.betting_lines_totals(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_teams ON core_betting.betting_lines_totals(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_home_team ON core_betting.betting_lines_totals(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_away_team ON core_betting.betting_lines_totals(away_team);

-- Indexes for betting_splits
CREATE INDEX IF NOT EXISTS idx_betting_splits_game_datetime ON core_betting.betting_splits(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_splits_teams ON core_betting.betting_splits(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_splits_home_team ON core_betting.betting_splits(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_splits_away_team ON core_betting.betting_splits(away_team);

-- Indexes for line_movements
CREATE INDEX IF NOT EXISTS idx_line_movements_game_datetime ON core_betting.line_movements(game_datetime);
CREATE INDEX IF NOT EXISTS idx_line_movements_teams ON core_betting.line_movements(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_line_movements_home_team ON core_betting.line_movements(home_team);
CREATE INDEX IF NOT EXISTS idx_line_movements_away_team ON core_betting.line_movements(away_team);

-- Indexes for steam_moves
CREATE INDEX IF NOT EXISTS idx_steam_moves_game_datetime ON core_betting.steam_moves(game_datetime);
CREATE INDEX IF NOT EXISTS idx_steam_moves_teams ON core_betting.steam_moves(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_steam_moves_home_team ON core_betting.steam_moves(home_team);
CREATE INDEX IF NOT EXISTS idx_steam_moves_away_team ON core_betting.steam_moves(away_team);

-- ==============================================================================
-- CREATE TRIGGERS TO AUTOMATICALLY POPULATE NEW COLUMNS
-- ==============================================================================

-- Function to populate team and datetime info from games table
CREATE OR REPLACE FUNCTION populate_game_info()
RETURNS TRIGGER AS $$
BEGIN
    -- Get game information from games table
    SELECT 
        g.game_datetime,
        g.home_team,
        g.away_team
    INTO 
        NEW.game_datetime,
        NEW.home_team,
        NEW.away_team
    FROM core_betting.games g
    WHERE g.id = NEW.game_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for all betting tables
CREATE TRIGGER trigger_populate_game_info_moneyline
    BEFORE INSERT ON core_betting.betting_lines_moneyline
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_spreads
    BEFORE INSERT ON core_betting.betting_lines_spreads
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_totals
    BEFORE INSERT ON core_betting.betting_lines_totals
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_splits
    BEFORE INSERT ON core_betting.betting_splits
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_line_movements
    BEFORE INSERT ON core_betting.line_movements
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_steam_moves
    BEFORE INSERT ON core_betting.steam_moves
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

-- ==============================================================================
-- VERIFICATION QUERIES
-- ==============================================================================

-- Show sample data to verify the changes
SELECT 
    'betting_lines_totals' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM core_betting.betting_lines_totals

UNION ALL

SELECT 
    'betting_lines_moneyline' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM core_betting.betting_lines_moneyline

UNION ALL

SELECT 
    'betting_lines_spreads' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM core_betting.betting_lines_spreads;

-- Sample data to verify the changes
SELECT 
    'betting_lines_totals' as table_name,
    game_datetime,
    home_team,
    away_team,
    sportsbook,
    total_line,
    over_price,
    under_price
FROM core_betting.betting_lines_totals 
WHERE game_datetime IS NOT NULL
ORDER BY game_datetime DESC
LIMIT 5;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON COLUMN core_betting.betting_lines_moneyline.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_moneyline.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_moneyline.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN core_betting.betting_lines_spreads.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_spreads.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_spreads.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN core_betting.betting_lines_totals.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_totals.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_lines_totals.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN core_betting.betting_splits.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_splits.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.betting_splits.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN core_betting.line_movements.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.line_movements.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.line_movements.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN core_betting.steam_moves.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.steam_moves.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN core_betting.steam_moves.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)'; 