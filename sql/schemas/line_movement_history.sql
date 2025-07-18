-- Historical Line Movement Schema
-- Captures all historical odds changes from Action Network

-- Main line movement history table
CREATE TABLE IF NOT EXISTS core_betting.line_movement_history (
    id SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    sportsbook_id INTEGER NOT NULL REFERENCES core_betting.sportsbooks(id),
    
    -- Action Network Identifiers
    action_network_game_id INTEGER NOT NULL,
    action_network_book_id INTEGER NOT NULL,
    outcome_id BIGINT,
    market_id BIGINT,
    
    -- Market Information
    bet_type VARCHAR(20) NOT NULL CHECK (bet_type IN ('moneyline', 'spread', 'total')),
    side VARCHAR(10) NOT NULL CHECK (side IN ('home', 'away', 'over', 'under')),
    period VARCHAR(20) DEFAULT 'event',
    
    -- Line Values (at this point in time)
    odds INTEGER NOT NULL,
    line_value DECIMAL(4,1), -- For spreads and totals
    line_status VARCHAR(20) DEFAULT 'normal',
    
    -- Timing
    line_timestamp TIMESTAMPTZ NOT NULL, -- When this line was active (from updated_at)
    collection_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When we collected it
    
    -- Team Information (denormalized for querying)
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_datetime TIMESTAMPTZ NOT NULL,
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'ACTION_NETWORK',
    is_live BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(action_network_game_id, action_network_book_id, bet_type, side, line_timestamp)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_line_movement_game_book 
ON core_betting.line_movement_history(game_id, sportsbook_id);

CREATE INDEX IF NOT EXISTS idx_line_movement_timestamp 
ON core_betting.line_movement_history(line_timestamp);

CREATE INDEX IF NOT EXISTS idx_line_movement_game_date 
ON core_betting.line_movement_history(game_datetime);

CREATE INDEX IF NOT EXISTS idx_line_movement_bet_type 
ON core_betting.line_movement_history(bet_type, side);

-- Opening lines view (first recorded line for each market)
CREATE OR REPLACE VIEW core_betting.opening_lines AS
SELECT DISTINCT ON (game_id, sportsbook_id, bet_type, side)
    game_id,
    sportsbook_id,
    bet_type,
    side,
    odds as opening_odds,
    line_value as opening_line_value,
    line_timestamp as opening_timestamp,
    home_team,
    away_team
FROM core_betting.line_movement_history
ORDER BY game_id, sportsbook_id, bet_type, side, line_timestamp ASC;

-- Closing lines view (last recorded line for each market)
CREATE OR REPLACE VIEW core_betting.closing_lines AS
SELECT DISTINCT ON (game_id, sportsbook_id, bet_type, side)
    game_id,
    sportsbook_id,
    bet_type,
    side,
    odds as closing_odds,
    line_value as closing_line_value,
    line_timestamp as closing_timestamp,
    home_team,
    away_team
FROM core_betting.line_movement_history
ORDER BY game_id, sportsbook_id, bet_type, side, line_timestamp DESC;

-- Line movement summary view
CREATE OR REPLACE VIEW core_betting.line_movement_summary AS
SELECT 
    lmh.game_id,
    lmh.sportsbook_id,
    s.display_name as sportsbook_name,
    lmh.bet_type,
    lmh.side,
    lmh.home_team,
    lmh.away_team,
    ol.opening_odds,
    ol.opening_line_value,
    cl.closing_odds,
    cl.closing_line_value,
    (cl.closing_odds - ol.opening_odds) as odds_movement,
    (cl.closing_line_value - ol.opening_line_value) as line_movement,
    COUNT(lmh.id) as total_movements,
    ol.opening_timestamp,
    cl.closing_timestamp
FROM core_betting.line_movement_history lmh
JOIN core_betting.sportsbooks s ON lmh.sportsbook_id = s.id
JOIN core_betting.opening_lines ol ON (
    lmh.game_id = ol.game_id AND 
    lmh.sportsbook_id = ol.sportsbook_id AND 
    lmh.bet_type = ol.bet_type AND 
    lmh.side = ol.side
)
JOIN core_betting.closing_lines cl ON (
    lmh.game_id = cl.game_id AND 
    lmh.sportsbook_id = cl.sportsbook_id AND 
    lmh.bet_type = cl.bet_type AND 
    lmh.side = cl.side
)
GROUP BY 
    lmh.game_id, lmh.sportsbook_id, s.display_name, lmh.bet_type, lmh.side,
    lmh.home_team, lmh.away_team, ol.opening_odds, ol.opening_line_value,
    cl.closing_odds, cl.closing_line_value, ol.opening_timestamp, cl.closing_timestamp;