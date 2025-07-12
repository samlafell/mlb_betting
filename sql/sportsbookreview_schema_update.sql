-- SportsbookReview Data Integration Schema Update
-- This schema adds columns to the existing public.games table and creates betting tables

-- Create mlb_betting schema
CREATE SCHEMA IF NOT EXISTS mlb_betting;

-- ==============================================================================
-- UPDATE EXISTING PUBLIC.GAMES TABLE
-- ==============================================================================

-- Add the three ID columns as requested
ALTER TABLE public.games 
ADD COLUMN IF NOT EXISTS sportsbookreview_game_id VARCHAR(100) UNIQUE,
ADD COLUMN IF NOT EXISTS mlb_stats_api_game_id VARCHAR(20),
ADD COLUMN IF NOT EXISTS action_network_game_id INTEGER;

-- Add additional columns for SportsbookReview integration
ALTER TABLE public.games 
ADD COLUMN IF NOT EXISTS game_date DATE,
ADD COLUMN IF NOT EXISTS venue_name VARCHAR(200),
ADD COLUMN IF NOT EXISTS venue_id INTEGER,
ADD COLUMN IF NOT EXISTS season INTEGER,
ADD COLUMN IF NOT EXISTS season_type VARCHAR(20) DEFAULT 'regular',
ADD COLUMN IF NOT EXISTS game_type VARCHAR(20) DEFAULT 'regular',
ADD COLUMN IF NOT EXISTS weather_condition VARCHAR(20),
ADD COLUMN IF NOT EXISTS temperature INTEGER,
ADD COLUMN IF NOT EXISTS wind_speed INTEGER,
ADD COLUMN IF NOT EXISTS wind_direction VARCHAR(10),
ADD COLUMN IF NOT EXISTS humidity INTEGER,
ADD COLUMN IF NOT EXISTS data_quality VARCHAR(20) DEFAULT 'MEDIUM',
ADD COLUMN IF NOT EXISTS mlb_correlation_confidence DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS has_mlb_enrichment BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS winning_team VARCHAR(5);

-- Update the game_date column from game_datetime if it's not already set
UPDATE public.games 
SET game_date = DATE(game_datetime) 
WHERE game_date IS NULL AND game_datetime IS NOT NULL;

-- Add indexes for the new columns
CREATE INDEX IF NOT EXISTS idx_public_games_sbr_id ON public.games(sportsbookreview_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_mlb_id ON public.games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_action_id ON public.games(action_network_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_date ON public.games(game_date);
CREATE INDEX IF NOT EXISTS idx_public_games_season ON public.games(season, season_type);

-- ==============================================================================
-- BETTING DATA TABLES
-- ==============================================================================

-- Moneyline betting data
CREATE TABLE IF NOT EXISTS mlb_betting.moneyline (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    
    -- Moneyline odds
    home_ml INTEGER, -- American odds format (+150, -120, etc.)
    away_ml INTEGER,
    
    -- Line movement tracking
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    opening_home_ml INTEGER,
    opening_away_ml INTEGER,
    closing_home_ml INTEGER,
    closing_away_ml INTEGER,
    
    -- Betting splits data
    home_bets_count INTEGER,
    away_bets_count INTEGER,
    home_bets_percentage DECIMAL(5,2),
    away_bets_percentage DECIMAL(5,2),
    home_money_percentage DECIMAL(5,2),
    away_money_percentage DECIMAL(5,2),
    
    -- Sharp action indicators
    sharp_action VARCHAR(10), -- 'HOME', 'AWAY', 'NONE'
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move BOOLEAN DEFAULT FALSE,
    
    -- Outcome tracking
    winning_side VARCHAR(10), -- 'HOME', 'AWAY', 'PUSH'
    profit_loss DECIMAL(12,2),
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'SPORTSBOOKREVIEW',
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key constraint
    CONSTRAINT fk_moneyline_game FOREIGN KEY (game_id) REFERENCES public.games(id) ON DELETE CASCADE
);

-- Spreads table
CREATE TABLE IF NOT EXISTS mlb_betting.spreads (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    
    -- Spread data
    home_spread DECIMAL(3,1), -- e.g., -1.5, +1.5
    away_spread DECIMAL(3,1),
    home_spread_price INTEGER, -- American odds for the spread
    away_spread_price INTEGER,
    
    -- Line movement tracking
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    opening_home_spread DECIMAL(3,1),
    opening_away_spread DECIMAL(3,1),
    opening_home_spread_price INTEGER,
    opening_away_spread_price INTEGER,
    closing_home_spread DECIMAL(3,1),
    closing_away_spread DECIMAL(3,1),
    closing_home_spread_price INTEGER,
    closing_away_spread_price INTEGER,
    
    -- Betting splits data
    home_bets_count INTEGER,
    away_bets_count INTEGER,
    home_bets_percentage DECIMAL(5,2),
    away_bets_percentage DECIMAL(5,2),
    home_money_percentage DECIMAL(5,2),
    away_money_percentage DECIMAL(5,2),
    
    -- Sharp action indicators
    sharp_action VARCHAR(10), -- 'HOME', 'AWAY', 'NONE'
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move BOOLEAN DEFAULT FALSE,
    
    -- Outcome tracking
    winning_side VARCHAR(10), -- 'HOME', 'AWAY', 'PUSH'
    profit_loss DECIMAL(12,2),
    home_cover BOOLEAN,
    margin_of_victory INTEGER,
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'SPORTSBOOKREVIEW',
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key constraint
    CONSTRAINT fk_spreads_game FOREIGN KEY (game_id) REFERENCES public.games(id) ON DELETE CASCADE
);

-- Totals table
CREATE TABLE IF NOT EXISTS mlb_betting.totals (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    
    -- Total data
    total_line DECIMAL(3,1), -- e.g., 8.5, 9.0
    over_price INTEGER, -- American odds for over
    under_price INTEGER, -- American odds for under
    
    -- Line movement tracking
    odds_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    opening_total DECIMAL(3,1),
    opening_over_price INTEGER,
    opening_under_price INTEGER,
    closing_total DECIMAL(3,1),
    closing_over_price INTEGER,
    closing_under_price INTEGER,
    
    -- Betting splits data
    over_bets_count INTEGER,
    under_bets_count INTEGER,
    over_bets_percentage DECIMAL(5,2),
    under_bets_percentage DECIMAL(5,2),
    over_money_percentage DECIMAL(5,2),
    under_money_percentage DECIMAL(5,2),
    
    -- Sharp action indicators
    sharp_action VARCHAR(10), -- 'OVER', 'UNDER', 'NONE'
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move BOOLEAN DEFAULT FALSE,
    
    -- Outcome tracking
    winning_side VARCHAR(10), -- 'OVER', 'UNDER', 'PUSH'
    profit_loss DECIMAL(12,2),
    final_total INTEGER,
    went_over BOOLEAN,
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'SPORTSBOOKREVIEW',
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key constraint
    CONSTRAINT fk_totals_game FOREIGN KEY (game_id) REFERENCES public.games(id) ON DELETE CASCADE
);

-- ==============================================================================
-- INDEXES FOR BETTING TABLES
-- ==============================================================================

-- Moneyline indexes
CREATE INDEX IF NOT EXISTS idx_moneyline_game_id ON mlb_betting.moneyline(game_id);
CREATE INDEX IF NOT EXISTS idx_moneyline_sportsbook ON mlb_betting.moneyline(sportsbook);
CREATE INDEX IF NOT EXISTS idx_moneyline_timestamp ON mlb_betting.moneyline(odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_moneyline_sharp_action ON mlb_betting.moneyline(sharp_action);

-- Spreads indexes
CREATE INDEX IF NOT EXISTS idx_spreads_game_id ON mlb_betting.spreads(game_id);
CREATE INDEX IF NOT EXISTS idx_spreads_sportsbook ON mlb_betting.spreads(sportsbook);
CREATE INDEX IF NOT EXISTS idx_spreads_timestamp ON mlb_betting.spreads(odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_spreads_sharp_action ON mlb_betting.spreads(sharp_action);

-- Totals indexes
CREATE INDEX IF NOT EXISTS idx_totals_game_id ON mlb_betting.totals(game_id);
CREATE INDEX IF NOT EXISTS idx_totals_sportsbook ON mlb_betting.totals(sportsbook);
CREATE INDEX IF NOT EXISTS idx_totals_timestamp ON mlb_betting.totals(odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_totals_sharp_action ON mlb_betting.totals(sharp_action);

-- ==============================================================================
-- HELPER FUNCTIONS
-- ==============================================================================

-- Function to upsert game data (modified for existing table)
CREATE OR REPLACE FUNCTION public.upsert_sportsbookreview_game(
    p_sbr_game_id VARCHAR(100),
    p_mlb_game_id VARCHAR(20),
    p_home_team VARCHAR(5),
    p_away_team VARCHAR(5),
    p_game_date DATE,
    p_game_datetime TIMESTAMP WITH TIME ZONE,
    p_venue_name VARCHAR(200) DEFAULT NULL,
    p_venue_id INTEGER DEFAULT NULL,
    p_season INTEGER DEFAULT NULL,
    p_season_type VARCHAR(20) DEFAULT 'regular',
    p_game_type VARCHAR(20) DEFAULT 'regular',
    p_data_quality VARCHAR(20) DEFAULT 'medium'
)
RETURNS INTEGER AS $$
DECLARE
    game_row_id INTEGER;
BEGIN
    -- Try to find existing game by sportsbookreview_game_id
    SELECT id INTO game_row_id 
    FROM public.games 
    WHERE sportsbookreview_game_id = p_sbr_game_id;
    
    IF game_row_id IS NOT NULL THEN
        -- Update existing game
        UPDATE public.games SET
            mlb_stats_api_game_id = p_mlb_game_id,
            game_id = p_sbr_game_id,
            home_team = p_home_team,
            away_team = p_away_team,
            game_date = p_game_date,
            game_datetime = p_game_datetime,
            venue_name = p_venue_name,
            venue_id = p_venue_id,
            season = p_season,
            season_type = p_season_type,
            game_type = p_game_type,
            data_quality = p_data_quality,
            updated_at = NOW()
        WHERE id = game_row_id;
    ELSE
        -- Insert new game
        INSERT INTO public.games (
            sportsbookreview_game_id, mlb_stats_api_game_id, game_id,
            home_team, away_team, game_date, game_datetime, 
            venue_name, venue_id, season, season_type, game_type, data_quality,
            created_at, updated_at
        ) VALUES (
            p_sbr_game_id, p_mlb_game_id, p_sbr_game_id,  -- Use SBR game ID as game_id
            p_home_team, p_away_team, p_game_date, p_game_datetime,
            p_venue_name, p_venue_id, p_season, p_season_type, p_game_type, p_data_quality,
            NOW(), NOW()
        )
        RETURNING id INTO game_row_id;
    END IF;
    
    RETURN game_row_id;
END;
$$ LANGUAGE plpgsql;

-- Function to update game MLB enrichment data
CREATE OR REPLACE FUNCTION public.update_game_mlb_enrichment(
    p_game_id INTEGER,
    p_mlb_game_id VARCHAR(20),
    p_venue_name VARCHAR(200),
    p_venue_id INTEGER,
    p_weather_condition VARCHAR(20),
    p_temperature INTEGER,
    p_wind_speed INTEGER,
    p_wind_direction VARCHAR(10),
    p_humidity INTEGER,
    p_correlation_confidence DECIMAL(5,4)
)
RETURNS VOID AS $$
BEGIN
    UPDATE public.games SET
        mlb_stats_api_game_id = p_mlb_game_id,
        venue_name = p_venue_name,
        venue_id = p_venue_id,
        weather_condition = p_weather_condition,
        temperature = p_temperature,
        wind_speed = p_wind_speed,
        wind_direction = p_wind_direction,
        humidity = p_humidity,
        mlb_correlation_confidence = p_correlation_confidence,
        has_mlb_enrichment = TRUE,
        data_quality = CASE 
            WHEN p_correlation_confidence >= 0.9 THEN 'HIGH'
            WHEN p_correlation_confidence >= 0.7 THEN 'MEDIUM'
            ELSE 'LOW'
        END,
        updated_at = NOW()
    WHERE id = p_game_id;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- VIEWS FOR ANALYSIS
-- ==============================================================================

-- Comprehensive game view with all betting data
CREATE OR REPLACE VIEW public.games_with_sportsbookreview_data AS
SELECT 
    g.id,
    g.sportsbookreview_game_id,
    g.mlb_stats_api_game_id,
    g.action_network_game_id,
    g.game_id,
    g.home_team,
    g.away_team,
    g.game_date,
    g.game_datetime,
    g.status,
    g.venue_name,
    g.season,
    
    -- Moneyline data (latest)
    ml.home_ml,
    ml.away_ml,
    ml.sharp_action as ml_sharp_action,
    
    -- Spreads data (latest)
    s.home_spread,
    s.away_spread,
    s.home_spread_price,
    s.away_spread_price,
    s.sharp_action as spread_sharp_action,
    
    -- Totals data (latest)
    t.total_line,
    t.over_price,
    t.under_price,
    t.sharp_action as total_sharp_action,
    
    -- Game results
    g.home_score,
    g.away_score,
    g.winning_team,
    
    -- Data quality
    g.data_quality,
    g.has_mlb_enrichment,
    g.mlb_correlation_confidence

FROM public.games g
LEFT JOIN LATERAL (
    SELECT * FROM mlb_betting.moneyline 
    WHERE game_id = g.id 
    ORDER BY odds_timestamp DESC LIMIT 1
) ml ON true
LEFT JOIN LATERAL (
    SELECT * FROM mlb_betting.spreads 
    WHERE game_id = g.id 
    ORDER BY odds_timestamp DESC LIMIT 1
) s ON true
LEFT JOIN LATERAL (
    SELECT * FROM mlb_betting.totals 
    WHERE game_id = g.id 
    ORDER BY odds_timestamp DESC LIMIT 1
) t ON true
ORDER BY g.game_datetime DESC;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON SCHEMA mlb_betting IS 'Schema for MLB betting data from SportsbookReview.com';
COMMENT ON TABLE mlb_betting.moneyline IS 'Moneyline betting data and splits from SportsbookReview';
COMMENT ON TABLE mlb_betting.spreads IS 'Spread betting data and splits from SportsbookReview';
COMMENT ON TABLE mlb_betting.totals IS 'Total betting data and splits from SportsbookReview';

COMMENT ON COLUMN public.games.sportsbookreview_game_id IS 'Unique identifier from SportsbookReview.com';
COMMENT ON COLUMN public.games.mlb_stats_api_game_id IS 'Official MLB Stats API game ID (gamePk)';
COMMENT ON COLUMN public.games.action_network_game_id IS 'Action Network game ID (currently NULL)'; 