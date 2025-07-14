-- Action Network Betting Line Movement Tracking Schema
-- 
-- This schema creates tables specifically for tracking Action Network betting data
-- with line movement history and automatic incremental updates.
-- 
-- Compatible with PostgreSQL 17

-- Create schema for Action Network betting data
CREATE SCHEMA IF NOT EXISTS action_network;

-- Table for Action Network sportsbooks reference
CREATE TABLE IF NOT EXISTS action_network.sportsbooks (
    id SERIAL PRIMARY KEY,
    book_id INTEGER UNIQUE NOT NULL, -- Action Network book ID (15, 30, 68, etc.)
    name VARCHAR(100) NOT NULL,
    display_name VARCHAR(100),
    abbreviation VARCHAR(10),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert common Action Network sportsbooks
INSERT INTO action_network.sportsbooks (book_id, name, display_name, abbreviation) 
VALUES 
    (15, 'DraftKings', 'DraftKings', 'DK'),
    (30, 'FanDuel', 'FanDuel', 'FD'),
    (68, 'BetMGM', 'BetMGM', 'MGM'),
    (69, 'Caesars', 'Caesars', 'CZR'),
    (71, 'PointsBet', 'PointsBet', 'PB'),
    (75, 'Circa', 'Circa', 'CIRCA')
ON CONFLICT (book_id) DO UPDATE SET
    name = EXCLUDED.name,
    display_name = EXCLUDED.display_name,
    abbreviation = EXCLUDED.abbreviation,
    updated_at = NOW();

-- Main table for Action Network betting lines with line movement history
CREATE TABLE IF NOT EXISTS action_network.betting_lines (
    id SERIAL PRIMARY KEY,
    
    -- Game identification
    game_id INTEGER NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Sportsbook information
    book_id INTEGER NOT NULL REFERENCES action_network.sportsbooks(book_id),
    sportsbook_name VARCHAR(100) NOT NULL,
    
    -- Market information
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    side VARCHAR(10) NOT NULL CHECK (side IN ('home', 'away', 'over', 'under')),
    period VARCHAR(20) NOT NULL CHECK (period IN ('pregame', 'live')),
    
    -- Line data
    odds_american INTEGER, -- American odds format (-110, +150, etc.)
    odds_decimal DECIMAL(8,4), -- Decimal odds (1.909, 2.50, etc.)
    line_value DECIMAL(5,2), -- Spread/total value (-1.5, 8.5, etc.)
    
    -- Betting splits data (from Action Network bet info)
    bet_tickets_count INTEGER,
    bet_tickets_percentage DECIMAL(5,2),
    bet_money_percentage DECIMAL(5,2),
    
    -- Line movement tracking
    line_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- When this line was active
    is_opening_line BOOLEAN DEFAULT FALSE,
    is_closing_line BOOLEAN DEFAULT FALSE,
    
    -- Movement indicators
    line_movement_direction VARCHAR(10) CHECK (line_movement_direction IN ('up', 'down', 'none')),
    odds_movement_direction VARCHAR(10) CHECK (odds_movement_direction IN ('up', 'down', 'none')),
    
    -- Sharp action detection
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move BOOLEAN DEFAULT FALSE,
    sharp_action VARCHAR(20),
    
    -- Raw data for debugging
    raw_market_data JSONB,
    
    -- Extraction metadata
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    history_url TEXT,
    
    -- Constraints
    UNIQUE(game_id, book_id, market_type, side, line_timestamp),
    
    -- Indexes for performance
    INDEX idx_betting_lines_game_id (game_id),
    INDEX idx_betting_lines_book_id (book_id),
    INDEX idx_betting_lines_market_type (market_type),
    INDEX idx_betting_lines_game_book_market (game_id, book_id, market_type),
    INDEX idx_betting_lines_teams (home_team, away_team),
    INDEX idx_betting_lines_game_datetime (game_datetime),
    INDEX idx_betting_lines_line_timestamp (line_timestamp),
    INDEX idx_betting_lines_period (period),
    INDEX idx_betting_lines_extracted_at (extracted_at),
    INDEX idx_betting_lines_opening_closing (is_opening_line, is_closing_line),
    INDEX idx_betting_lines_sharp_action (sharp_action, reverse_line_movement, steam_move)
);

-- Table for tracking the last extraction time per game to enable incremental updates
CREATE TABLE IF NOT EXISTS action_network.extraction_log (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Extraction tracking
    last_extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    total_extractions INTEGER NOT NULL DEFAULT 1,
    total_lines_extracted INTEGER NOT NULL DEFAULT 0,
    
    -- Status tracking
    extraction_status VARCHAR(20) NOT NULL DEFAULT 'success' CHECK (extraction_status IN ('success', 'failed', 'partial')),
    error_message TEXT,
    
    -- Metadata
    history_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(game_id),
    
    -- Indexes
    INDEX idx_extraction_log_game_id (game_id),
    INDEX idx_extraction_log_last_extracted (last_extracted_at),
    INDEX idx_extraction_log_status (extraction_status)
);

-- Table for line movement summaries (aggregated data for faster queries)
CREATE TABLE IF NOT EXISTS action_network.line_movement_summary (
    id SERIAL PRIMARY KEY,
    
    -- Game and market identification
    game_id INTEGER NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    book_id INTEGER NOT NULL REFERENCES action_network.sportsbooks(book_id),
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    side VARCHAR(10) NOT NULL CHECK (side IN ('home', 'away', 'over', 'under')),
    
    -- Opening line data
    opening_odds_american INTEGER,
    opening_odds_decimal DECIMAL(8,4),
    opening_line_value DECIMAL(5,2),
    opening_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- Closing line data
    closing_odds_american INTEGER,
    closing_odds_decimal DECIMAL(8,4),
    closing_line_value DECIMAL(5,2),
    closing_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- Movement statistics
    total_movements INTEGER NOT NULL DEFAULT 0,
    max_odds_american INTEGER,
    min_odds_american INTEGER,
    max_line_value DECIMAL(5,2),
    min_line_value DECIMAL(5,2),
    
    -- Sharp action summary
    reverse_line_movement_detected BOOLEAN DEFAULT FALSE,
    steam_moves_detected INTEGER DEFAULT 0,
    sharp_action_direction VARCHAR(20),
    
    -- Betting splits summary
    final_bet_tickets_percentage DECIMAL(5,2),
    final_bet_money_percentage DECIMAL(5,2),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(game_id, book_id, market_type, side),
    
    -- Indexes
    INDEX idx_line_movement_summary_game_id (game_id),
    INDEX idx_line_movement_summary_book_market (book_id, market_type),
    INDEX idx_line_movement_summary_game_book_market (game_id, book_id, market_type),
    INDEX idx_line_movement_summary_sharp_action (sharp_action_direction, reverse_line_movement_detected)
);

-- Function to automatically update line movement summary
CREATE OR REPLACE FUNCTION action_network.update_line_movement_summary()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert or update line movement summary
    INSERT INTO action_network.line_movement_summary (
        game_id, home_team, away_team, game_datetime, book_id, market_type, side,
        opening_odds_american, opening_odds_decimal, opening_line_value, opening_timestamp,
        closing_odds_american, closing_odds_decimal, closing_line_value, closing_timestamp,
        total_movements, max_odds_american, min_odds_american, max_line_value, min_line_value,
        reverse_line_movement_detected, steam_moves_detected, sharp_action_direction,
        final_bet_tickets_percentage, final_bet_money_percentage,
        created_at, updated_at
    )
    SELECT 
        NEW.game_id, NEW.home_team, NEW.away_team, NEW.game_datetime, NEW.book_id, NEW.market_type, NEW.side,
        
        -- Opening line data (earliest timestamp)
        (SELECT odds_american FROM action_network.betting_lines bl1 
         WHERE bl1.game_id = NEW.game_id AND bl1.book_id = NEW.book_id 
         AND bl1.market_type = NEW.market_type AND bl1.side = NEW.side 
         ORDER BY bl1.line_timestamp ASC LIMIT 1),
        (SELECT odds_decimal FROM action_network.betting_lines bl1 
         WHERE bl1.game_id = NEW.game_id AND bl1.book_id = NEW.book_id 
         AND bl1.market_type = NEW.market_type AND bl1.side = NEW.side 
         ORDER BY bl1.line_timestamp ASC LIMIT 1),
        (SELECT line_value FROM action_network.betting_lines bl1 
         WHERE bl1.game_id = NEW.game_id AND bl1.book_id = NEW.book_id 
         AND bl1.market_type = NEW.market_type AND bl1.side = NEW.side 
         ORDER BY bl1.line_timestamp ASC LIMIT 1),
        (SELECT line_timestamp FROM action_network.betting_lines bl1 
         WHERE bl1.game_id = NEW.game_id AND bl1.book_id = NEW.book_id 
         AND bl1.market_type = NEW.market_type AND bl1.side = NEW.side 
         ORDER BY bl1.line_timestamp ASC LIMIT 1),
        
        -- Closing line data (latest timestamp)
        (SELECT odds_american FROM action_network.betting_lines bl2 
         WHERE bl2.game_id = NEW.game_id AND bl2.book_id = NEW.book_id 
         AND bl2.market_type = NEW.market_type AND bl2.side = NEW.side 
         ORDER BY bl2.line_timestamp DESC LIMIT 1),
        (SELECT odds_decimal FROM action_network.betting_lines bl2 
         WHERE bl2.game_id = NEW.game_id AND bl2.book_id = NEW.book_id 
         AND bl2.market_type = NEW.market_type AND bl2.side = NEW.side 
         ORDER BY bl2.line_timestamp DESC LIMIT 1),
        (SELECT line_value FROM action_network.betting_lines bl2 
         WHERE bl2.game_id = NEW.game_id AND bl2.book_id = NEW.book_id 
         AND bl2.market_type = NEW.market_type AND bl2.side = NEW.side 
         ORDER BY bl2.line_timestamp DESC LIMIT 1),
        (SELECT line_timestamp FROM action_network.betting_lines bl2 
         WHERE bl2.game_id = NEW.game_id AND bl2.book_id = NEW.book_id 
         AND bl2.market_type = NEW.market_type AND bl2.side = NEW.side 
         ORDER BY bl2.line_timestamp DESC LIMIT 1),
        
        -- Movement statistics
        (SELECT COUNT(*) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        (SELECT MAX(odds_american) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        (SELECT MIN(odds_american) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        (SELECT MAX(line_value) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        (SELECT MIN(line_value) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        
        -- Sharp action summary
        (SELECT bool_or(reverse_line_movement) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side),
        (SELECT COUNT(*) FROM action_network.betting_lines bl3 
         WHERE bl3.game_id = NEW.game_id AND bl3.book_id = NEW.book_id 
         AND bl3.market_type = NEW.market_type AND bl3.side = NEW.side AND bl3.steam_move = true),
        NEW.sharp_action,
        
        -- Betting splits
        NEW.bet_tickets_percentage,
        NEW.bet_money_percentage,
        
        NOW(), NOW()
    
    ON CONFLICT (game_id, book_id, market_type, side) 
    DO UPDATE SET
        closing_odds_american = EXCLUDED.closing_odds_american,
        closing_odds_decimal = EXCLUDED.closing_odds_decimal,
        closing_line_value = EXCLUDED.closing_line_value,
        closing_timestamp = EXCLUDED.closing_timestamp,
        total_movements = EXCLUDED.total_movements,
        max_odds_american = EXCLUDED.max_odds_american,
        min_odds_american = EXCLUDED.min_odds_american,
        max_line_value = EXCLUDED.max_line_value,
        min_line_value = EXCLUDED.min_line_value,
        reverse_line_movement_detected = EXCLUDED.reverse_line_movement_detected,
        steam_moves_detected = EXCLUDED.steam_moves_detected,
        sharp_action_direction = EXCLUDED.sharp_action_direction,
        final_bet_tickets_percentage = EXCLUDED.final_bet_tickets_percentage,
        final_bet_money_percentage = EXCLUDED.final_bet_money_percentage,
        updated_at = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update line movement summary
CREATE TRIGGER trigger_update_line_movement_summary
    AFTER INSERT OR UPDATE ON action_network.betting_lines
    FOR EACH ROW
    EXECUTE FUNCTION action_network.update_line_movement_summary();

-- Function to get new lines since last extraction
CREATE OR REPLACE FUNCTION action_network.get_new_lines_since_last_extraction(
    p_game_id INTEGER,
    p_book_id INTEGER,
    p_market_type VARCHAR(20),
    p_last_extracted_at TIMESTAMP WITH TIME ZONE
) RETURNS TABLE (
    id INTEGER,
    side VARCHAR(10),
    odds_american INTEGER,
    odds_decimal DECIMAL(8,4),
    line_value DECIMAL(5,2),
    line_timestamp TIMESTAMP WITH TIME ZONE,
    bet_tickets_percentage DECIMAL(5,2),
    bet_money_percentage DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        bl.id,
        bl.side,
        bl.odds_american,
        bl.odds_decimal,
        bl.line_value,
        bl.line_timestamp,
        bl.bet_tickets_percentage,
        bl.bet_money_percentage
    FROM action_network.betting_lines bl
    WHERE bl.game_id = p_game_id
    AND bl.book_id = p_book_id
    AND bl.market_type = p_market_type
    AND bl.line_timestamp > p_last_extracted_at
    ORDER BY bl.line_timestamp DESC;
END;
$$ LANGUAGE plpgsql;

-- View for easy querying of line movements with sportsbook names
CREATE OR REPLACE VIEW action_network.betting_lines_with_books AS
SELECT 
    bl.*,
    sb.name as sportsbook_full_name,
    sb.display_name as sportsbook_display_name,
    sb.abbreviation as sportsbook_abbreviation
FROM action_network.betting_lines bl
JOIN action_network.sportsbooks sb ON bl.book_id = sb.book_id
ORDER BY bl.game_id, bl.book_id, bl.market_type, bl.side, bl.line_timestamp;

-- View for latest lines per game/book/market
CREATE OR REPLACE VIEW action_network.latest_betting_lines AS
SELECT DISTINCT ON (game_id, book_id, market_type, side)
    bl.*,
    sb.name as sportsbook_full_name,
    sb.display_name as sportsbook_display_name,
    sb.abbreviation as sportsbook_abbreviation
FROM action_network.betting_lines bl
JOIN action_network.sportsbooks sb ON bl.book_id = sb.book_id
ORDER BY bl.game_id, bl.book_id, bl.market_type, bl.side, bl.line_timestamp DESC;

-- Comments for documentation
COMMENT ON SCHEMA action_network IS 'Schema for Action Network betting data and line movement tracking';
COMMENT ON TABLE action_network.sportsbooks IS 'Reference table for Action Network sportsbooks';
COMMENT ON TABLE action_network.betting_lines IS 'Main table for Action Network betting lines with complete line movement history';
COMMENT ON TABLE action_network.extraction_log IS 'Log table for tracking extraction status and enabling incremental updates';
COMMENT ON TABLE action_network.line_movement_summary IS 'Aggregated line movement data for faster queries';
COMMENT ON VIEW action_network.betting_lines_with_books IS 'Betting lines joined with sportsbook information';
COMMENT ON VIEW action_network.latest_betting_lines IS 'Latest betting lines per game/book/market combination';

COMMENT ON COLUMN action_network.betting_lines.game_id IS 'Action Network game ID';
COMMENT ON COLUMN action_network.betting_lines.book_id IS 'Action Network sportsbook ID (15=DraftKings, 30=FanDuel, etc.)';
COMMENT ON COLUMN action_network.betting_lines.market_type IS 'Type of betting market (moneyline, spread, total)';
COMMENT ON COLUMN action_network.betting_lines.side IS 'Side of the bet (home, away, over, under)';
COMMENT ON COLUMN action_network.betting_lines.period IS 'When the line was active (pregame, live)';
COMMENT ON COLUMN action_network.betting_lines.line_timestamp IS 'When this specific line was active (from Action Network history)';
COMMENT ON COLUMN action_network.betting_lines.bet_tickets_percentage IS 'Percentage of tickets on this side';
COMMENT ON COLUMN action_network.betting_lines.bet_money_percentage IS 'Percentage of money on this side'; 