-- SportsbookReview Data Integration Schema Update (Simplified)
-- This schema adds columns to the existing public.games table and creates betting tables

-- Create mlb_betting schema
CREATE SCHEMA IF NOT EXISTS mlb_betting;

-- ==============================================================================
-- UPDATE EXISTING PUBLIC.GAMES TABLE
-- ==============================================================================

-- Add the three ID columns as requested
ALTER TABLE public.games 
ADD COLUMN IF NOT EXISTS sportsbookreview_game_id VARCHAR(100),
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
-- BETTING DATA TABLES (WITHOUT FOREIGN KEY CONSTRAINTS)
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
-- UTILITY FUNCTIONS
-- ==============================================================================

-- First, make sure the id column is auto-incrementing
CREATE SEQUENCE IF NOT EXISTS public.games_id_seq 
START WITH 1 
INCREMENT BY 1 
NO MINVALUE 
NO MAXVALUE 
CACHE 1;

-- Set the sequence as the default for the id column
ALTER TABLE public.games 
ALTER COLUMN id SET DEFAULT nextval('public.games_id_seq');

-- Function to upsert SportsbookReview game data
CREATE OR REPLACE FUNCTION public.upsert_sportsbookreview_game(
    p_sbr_game_id VARCHAR(100),
    p_mlb_game_id VARCHAR(20),
    p_home_team VARCHAR(5),
    p_away_team VARCHAR(5),
    p_game_date DATE,
    p_game_datetime TIMESTAMP WITH TIME ZONE,
    p_venue_name VARCHAR(200),
    p_venue_id INTEGER,
    p_season INTEGER,
    p_season_type VARCHAR(20),
    p_game_type VARCHAR(20),
    p_data_quality VARCHAR(20)
)
RETURNS INTEGER AS $$
DECLARE
    v_game_id INTEGER;
    v_generated_game_id TEXT;
BEGIN
    -- Generate a unique game_id if not provided
    v_generated_game_id := COALESCE(p_sbr_game_id, p_home_team || '-' || p_away_team || '-' || p_game_date::TEXT);
    
    -- Try to find existing game by SportsbookReview ID
    SELECT id INTO v_game_id 
    FROM public.games 
    WHERE sportsbookreview_game_id = p_sbr_game_id;
    
    IF v_game_id IS NULL THEN
        -- Insert new game with required fields
        INSERT INTO public.games (
            game_id, sportsbookreview_game_id, mlb_stats_api_game_id, 
            home_team, away_team, game_date, game_datetime,
            venue_name, venue_id, season, season_type, game_type, data_quality,
            created_at, updated_at
        ) VALUES (
            v_generated_game_id, p_sbr_game_id, p_mlb_game_id,
            p_home_team, p_away_team, p_game_date, p_game_datetime,
            p_venue_name, p_venue_id, p_season, p_season_type, p_game_type, p_data_quality,
            NOW(), NOW()
        ) RETURNING id INTO v_game_id;
    ELSE
        -- Update existing game
        UPDATE public.games SET
            mlb_stats_api_game_id = COALESCE(p_mlb_game_id, mlb_stats_api_game_id),
            venue_name = COALESCE(p_venue_name, venue_name),
            venue_id = COALESCE(p_venue_id, venue_id),
            season = COALESCE(p_season, season),
            season_type = COALESCE(p_season_type, season_type),
            game_type = COALESCE(p_game_type, game_type),
            data_quality = COALESCE(p_data_quality, data_quality),
            updated_at = NOW()
        WHERE id = v_game_id;
    END IF;
    
    RETURN v_game_id;
END;
$$ LANGUAGE plpgsql;

-- Function to update game with MLB enrichment data
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
        mlb_stats_api_game_id = COALESCE(p_mlb_game_id, mlb_stats_api_game_id),
        venue_name = COALESCE(p_venue_name, venue_name),
        venue_id = COALESCE(p_venue_id, venue_id),
        weather_condition = COALESCE(p_weather_condition, weather_condition),
        temperature = COALESCE(p_temperature, temperature),
        wind_speed = COALESCE(p_wind_speed, wind_speed),
        wind_direction = COALESCE(p_wind_direction, wind_direction),
        humidity = COALESCE(p_humidity, humidity),
        mlb_correlation_confidence = COALESCE(p_correlation_confidence, mlb_correlation_confidence),
        has_mlb_enrichment = TRUE,
        updated_at = NOW()
    WHERE id = p_game_id;
END;
$$ LANGUAGE plpgsql;

-- Create a view for games with SportsbookReview data
CREATE OR REPLACE VIEW public.games_with_sportsbookreview_data AS
SELECT 
    g.*,
    CASE 
        WHEN g.sportsbookreview_game_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_sportsbookreview_data,
    CASE 
        WHEN g.mlb_stats_api_game_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_mlb_stats_api_data,
    CASE 
        WHEN g.action_network_game_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_action_network_data
FROM public.games g
WHERE g.sportsbookreview_game_id IS NOT NULL
   OR g.mlb_stats_api_game_id IS NOT NULL
   OR g.action_network_game_id IS NOT NULL; 