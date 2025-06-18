CREATE SCHEMA IF NOT EXISTS splits;

CREATE TABLE IF NOT EXISTS splits.raw_mlb_betting_splits (
    id BIGINT,
    game_id TEXT,
    home_team TEXT,
    away_team TEXT,
    game_datetime TIMESTAMP,
    split_type TEXT, -- 'Spread', 'Total', 'Moneyline'
    last_updated TIMESTAMP,
    source TEXT, -- 'SBD' for SportsBettingDime, 'VSIN' for other sources
    book TEXT, -- Specific sportsbook for VSIN (like 'DK', 'Circa'), NULL for SBD source
    
    -- Long format: home_or_over represents home team (Spread/Moneyline) or over (Total)
    home_or_over_bets INTEGER,
    home_or_over_bets_percentage DOUBLE,
    home_or_over_stake_percentage DOUBLE,
    
    -- Long format: away_or_under represents away team (Spread/Moneyline) or under (Total)
    away_or_under_bets INTEGER,
    away_or_under_bets_percentage DOUBLE,
    away_or_under_stake_percentage DOUBLE,
    
    -- Split-specific value (spread line, total line, or moneyline odds)
    split_value TEXT,
    
    -- Metadata
    sharp_action TEXT, -- Detected sharp action direction (if any)
    outcome TEXT, -- Outcome of the bet (win/loss/push)
    created_at TIMESTAMP,
    updated_at TIMESTAMP
); 