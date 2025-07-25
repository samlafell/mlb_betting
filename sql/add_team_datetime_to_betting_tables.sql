-- Add team and datetime columns to betting tables for easier identification
-- This script adds home_team, away_team, and game_datetime columns to make betting data more accessible

-- ==============================================================================
-- ADD COLUMNS TO CORE_BETTING TABLES
-- ==============================================================================

-- Add columns to betting_lines_moneyline table
ALTER TABLE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_lines_spreads table
ALTER TABLE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_lines_totals table
ALTER TABLE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to betting_splits table
ALTER TABLE curated.betting_splits 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to line_movements table
ALTER TABLE curated.line_movements 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- Add columns to steam_moves table
ALTER TABLE curated.steam_moves 
ADD COLUMN IF NOT EXISTS game_datetime TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS home_team VARCHAR(5),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(5);

-- ==============================================================================
-- POPULATE THE NEW COLUMNS WITH EXISTING DATA
-- ==============================================================================

-- Update betting_lines_moneyline table
UPDATE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'.game_id = g.id
AND curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'.game_datetime IS NULL;

-- Update betting_lines_spreads table
UPDATE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's.game_id = g.id
AND curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's.game_datetime IS NULL;

-- Update betting_lines_totals table
UPDATE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'.game_id = g.id
AND curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'.game_datetime IS NULL;

-- Update betting_splits table
UPDATE curated.betting_splits 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.betting_splits.game_id = g.id
AND curated.betting_splits.game_datetime IS NULL;

-- Update line_movements table
UPDATE curated.line_movements 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.line_movements.game_id = g.id
AND curated.line_movements.game_datetime IS NULL;

-- Update steam_moves table
UPDATE curated.steam_moves 
SET 
    game_datetime = g.game_datetime,
    home_team = g.home_team,
    away_team = g.away_team
FROM curated.games_complete g
WHERE curated.steam_moves.game_id = g.id
AND curated.steam_moves.game_datetime IS NULL;

-- ==============================================================================
-- CREATE INDEXES FOR BETTER PERFORMANCE
-- ==============================================================================

-- Indexes for betting_lines_moneyline
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_game_datetime ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_teams ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_home_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_moneyline_away_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'(away_team);

-- Indexes for betting_lines_spreads
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_game_datetime ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_teams ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_home_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_spreads_away_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's(away_team);

-- Indexes for betting_lines_totals
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_game_datetime ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_teams ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_home_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_lines_totals_away_team ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'(away_team);

-- Indexes for betting_splits
CREATE INDEX IF NOT EXISTS idx_betting_splits_game_datetime ON curated.betting_splits(game_datetime);
CREATE INDEX IF NOT EXISTS idx_betting_splits_teams ON curated.betting_splits(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_betting_splits_home_team ON curated.betting_splits(home_team);
CREATE INDEX IF NOT EXISTS idx_betting_splits_away_team ON curated.betting_splits(away_team);

-- Indexes for line_movements
CREATE INDEX IF NOT EXISTS idx_line_movements_game_datetime ON curated.line_movements(game_datetime);
CREATE INDEX IF NOT EXISTS idx_line_movements_teams ON curated.line_movements(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_line_movements_home_team ON curated.line_movements(home_team);
CREATE INDEX IF NOT EXISTS idx_line_movements_away_team ON curated.line_movements(away_team);

-- Indexes for steam_moves
CREATE INDEX IF NOT EXISTS idx_steam_moves_game_datetime ON curated.steam_moves(game_datetime);
CREATE INDEX IF NOT EXISTS idx_steam_moves_teams ON curated.steam_moves(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_steam_moves_home_team ON curated.steam_moves(home_team);
CREATE INDEX IF NOT EXISTS idx_steam_moves_away_team ON curated.steam_moves(away_team);

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
    FROM curated.games_complete g
    WHERE g.id = NEW.game_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for all betting tables
CREATE TRIGGER trigger_populate_game_info_moneyline
    BEFORE INSERT ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_spreads
    BEFORE INSERT ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_totals
    BEFORE INSERT ON curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_splits
    BEFORE INSERT ON curated.betting_splits
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_line_movements
    BEFORE INSERT ON curated.line_movements
    FOR EACH ROW
    WHEN (NEW.game_datetime IS NULL OR NEW.home_team IS NULL OR NEW.away_team IS NULL)
    EXECUTE FUNCTION populate_game_info();

CREATE TRIGGER trigger_populate_game_info_steam_moves
    BEFORE INSERT ON curated.steam_moves
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
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'

UNION ALL

SELECT 
    'betting_lines_moneyline' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'

UNION ALL

SELECT 
    'betting_lines_spreads' as table_name,
    COUNT(*) as total_records,
    COUNT(game_datetime) as populated_datetime,
    COUNT(home_team) as populated_home_team,
    COUNT(away_team) as populated_away_team
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's;

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
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' 
WHERE game_datetime IS NOT NULL
ORDER BY game_datetime DESC
LIMIT 5;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN curated.betting_splits.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_splits.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.betting_splits.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN curated.line_movements.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.line_movements.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.line_movements.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)';

COMMENT ON COLUMN curated.steam_moves.game_datetime IS 'Game datetime for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.steam_moves.home_team IS 'Home team abbreviation for easier identification (denormalized from games table)';
COMMENT ON COLUMN curated.steam_moves.away_team IS 'Away team abbreviation for easier identification (denormalized from games table)'; 