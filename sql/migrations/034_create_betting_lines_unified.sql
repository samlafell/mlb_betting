-- Migration: Create missing curated.betting_lines_unified table
-- Purpose: Create the unified betting lines table expected by ActionNetworkRepository
-- This table unifies betting lines data across all market types for Action Network
-- Date: 2025-08-12

-- ================================
-- Create curated.betting_lines_unified table
-- ================================

CREATE TABLE IF NOT EXISTS curated.betting_lines_unified (
    id SERIAL PRIMARY KEY,
    
    -- Game reference
    game_id VARCHAR(255) NOT NULL, -- Links to curated.games.game_id
    
    -- Sportsbook information
    sportsbook VARCHAR(100) NOT NULL,
    sportsbook_id INTEGER,
    source VARCHAR(50) NOT NULL DEFAULT 'action_network',
    
    -- Market type (moneyline, spread, total)
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    
    -- Moneyline fields
    home_ml INTEGER,
    away_ml INTEGER,
    
    -- Spread fields
    home_spread DECIMAL(5,2),
    away_spread DECIMAL(5,2),
    home_spread_price INTEGER,
    away_spread_price INTEGER,
    
    -- Total fields
    total_line DECIMAL(5,2),
    over_price INTEGER,
    under_price INTEGER,
    
    -- Data quality and reliability
    data_quality VARCHAR(10) DEFAULT 'HIGH' CHECK (data_quality IN ('HIGH', 'MEDIUM', 'LOW')),
    source_reliability_score DECIMAL(3,2) DEFAULT 1.0,
    
    -- Collection metadata
    collection_method VARCHAR(50) DEFAULT 'api',
    source_metadata JSONB,
    collection_batch_id VARCHAR(255),
    
    -- Timestamps
    odds_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT check_moneyline_data CHECK (
        CASE WHEN market_type = 'moneyline' 
        THEN (home_ml IS NOT NULL OR away_ml IS NOT NULL)
        ELSE TRUE END
    ),
    CONSTRAINT check_spread_data CHECK (
        CASE WHEN market_type = 'spread'
        THEN (home_spread IS NOT NULL AND away_spread IS NOT NULL 
              AND home_spread_price IS NOT NULL AND away_spread_price IS NOT NULL)
        ELSE TRUE END
    ),
    CONSTRAINT check_total_data CHECK (
        CASE WHEN market_type = 'total'
        THEN (total_line IS NOT NULL AND over_price IS NOT NULL AND under_price IS NOT NULL)
        ELSE TRUE END
    )
);

-- ================================
-- Create indexes for performance
-- ================================

CREATE INDEX IF NOT EXISTS idx_betting_lines_unified_game_id ON curated.betting_lines_unified(game_id);
CREATE INDEX IF NOT EXISTS idx_betting_lines_unified_market_type ON curated.betting_lines_unified(market_type);
CREATE INDEX IF NOT EXISTS idx_betting_lines_unified_sportsbook ON curated.betting_lines_unified(sportsbook);
CREATE INDEX IF NOT EXISTS idx_betting_lines_unified_odds_timestamp ON curated.betting_lines_unified(odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_betting_lines_unified_source ON curated.betting_lines_unified(source);

-- ================================
-- Insert sample data for testing
-- ================================

-- Insert sample unified betting lines for the games we created in migration 030
INSERT INTO curated.betting_lines_unified (
    game_id, sportsbook, sportsbook_id, source, market_type, 
    home_ml, away_ml, odds_timestamp
) VALUES 
    ('2025-08-01-NYY-BOS', 'DraftKings', 15, 'action_network', 'moneyline', -145, 125, '2025-08-01 18:00:00-04'),
    ('2025-08-01-NYY-BOS', 'FanDuel', 30, 'action_network', 'moneyline', -140, 120, '2025-08-01 18:00:00-04'),
    ('2025-08-01-LAD-SF', 'BetMGM', 68, 'action_network', 'moneyline', -165, 145, '2025-08-01 21:00:00-07')
ON CONFLICT DO NOTHING;

INSERT INTO curated.betting_lines_unified (
    game_id, sportsbook, sportsbook_id, source, market_type,
    home_spread, away_spread, home_spread_price, away_spread_price, odds_timestamp
) VALUES 
    ('2025-08-01-LAD-SF', 'BetMGM', 68, 'action_network', 'spread', -1.5, 1.5, -110, -110, '2025-08-01 21:00:00-07'),
    ('2025-08-02-HOU-TEX', 'Caesars', 69, 'action_network', 'spread', -1.0, 1.0, -115, -105, '2025-08-02 19:00:00-05')
ON CONFLICT DO NOTHING;

INSERT INTO curated.betting_lines_unified (
    game_id, sportsbook, sportsbook_id, source, market_type,
    total_line, over_price, under_price, odds_timestamp
) VALUES 
    ('2025-08-02-HOU-TEX', 'Caesars', 69, 'action_network', 'total', 8.5, -105, -115, '2025-08-02 19:00:00-05'),
    ('2025-08-01-NYY-BOS', 'DraftKings', 15, 'action_network', 'total', 9.0, -110, -110, '2025-08-01 18:00:00-04')
ON CONFLICT DO NOTHING;

-- ================================
-- Comments and documentation
-- ================================

COMMENT ON TABLE curated.betting_lines_unified IS 
'Unified betting lines table that combines moneyline, spread, and total markets in a single table structure expected by ActionNetworkRepository';

COMMENT ON COLUMN curated.betting_lines_unified.market_type IS 
'Type of betting market: moneyline, spread, or total';

COMMENT ON COLUMN curated.betting_lines_unified.source_reliability_score IS 
'Reliability score from 0.0 to 1.0 indicating data source quality';

COMMENT ON COLUMN curated.betting_lines_unified.collection_batch_id IS 
'Identifier for the batch collection run that created this record';