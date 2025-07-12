-- SportsbookReview Data Integration Schema
-- This schema stores historical odds data from SportsbookReview.com
-- with MLB Stats API enrichment and Action Network integration

-- Create schemas
CREATE SCHEMA IF NOT EXISTS mlb_betting;
CREATE SCHEMA IF NOT EXISTS public;

-- ==============================================================================
-- MAIN GAMES TABLE WITH MULTIPLE ID COLUMNS
-- ==============================================================================

-- Main games table with all three ID systems
CREATE TABLE IF NOT EXISTS public.games (
    id SERIAL PRIMARY KEY,
    
    -- Three ID columns as requested
    sportsbookreview_game_id VARCHAR(100) UNIQUE,
    mlb_stats_api_game_id VARCHAR(20),
    action_network_game_id INTEGER,
    
    -- Core game information
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Game status and results
    game_status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(5),
    
    -- Venue information
    venue_name VARCHAR(200),
    venue_id INTEGER,
    
    -- Game context
    season INTEGER,
    season_type VARCHAR(20) DEFAULT 'regular',
    game_type VARCHAR(20) DEFAULT 'regular',
    
    -- Weather data (from MLB Stats API)
    weather_condition VARCHAR(20),
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction VARCHAR(10),
    humidity INTEGER,
    
    -- Data quality tracking
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    mlb_correlation_confidence DECIMAL(5,4),
    has_mlb_enrichment BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_public_games_sbr_id ON public.games(sportsbookreview_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_mlb_id ON public.games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_action_id ON public.games(action_network_game_id);
CREATE INDEX IF NOT EXISTS idx_public_games_date ON public.games(game_date);
CREATE INDEX IF NOT EXISTS idx_public_games_datetime ON public.games(game_datetime);
CREATE INDEX IF NOT EXISTS idx_public_games_teams ON public.games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_public_games_season ON public.games(season, season_type);

-- ==============================================================================
-- BETTING DATA TABLES
-- ==============================================================================

-- Base betting data table structure
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

-- Function to upsert game data
CREATE OR REPLACE FUNCTION public.upsert_game(
    p_sbr_game_id VARCHAR(100),
    p_mlb_game_id VARCHAR(20),
    p_action_game_id INTEGER,
    p_home_team VARCHAR(5),
    p_away_team VARCHAR(5),
    p_game_date DATE,
    p_game_datetime TIMESTAMP WITH TIME ZONE,
    p_game_status VARCHAR(20) DEFAULT 'scheduled',
    p_venue_name VARCHAR(200) DEFAULT NULL,
    p_season INTEGER DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    game_id INTEGER;
BEGIN
    INSERT INTO public.games (
        sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id,
        home_team, away_team, game_date, game_datetime, game_status, venue_name, season
    ) VALUES (
        p_sbr_game_id, p_mlb_game_id, p_action_game_id,
        p_home_team, p_away_team, p_game_date, p_game_datetime, p_game_status, p_venue_name, p_season
    )
    ON CONFLICT (sportsbookreview_game_id) DO UPDATE SET
        mlb_stats_api_game_id = EXCLUDED.mlb_stats_api_game_id,
        action_network_game_id = EXCLUDED.action_network_game_id,
        home_team = EXCLUDED.home_team,
        away_team = EXCLUDED.away_team,
        game_date = EXCLUDED.game_date,
        game_datetime = EXCLUDED.game_datetime,
        game_status = EXCLUDED.game_status,
        venue_name = EXCLUDED.venue_name,
        season = EXCLUDED.season,
        updated_at = NOW()
    RETURNING id INTO game_id;
    
    RETURN game_id;
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
CREATE OR REPLACE VIEW public.games_with_betting_data AS
SELECT 
    g.id,
    g.sportsbookreview_game_id,
    g.mlb_stats_api_game_id,
    g.action_network_game_id,
    g.home_team,
    g.away_team,
    g.game_date,
    g.game_datetime,
    g.game_status,
    g.venue_name,
    g.season,
    
    -- Moneyline data
    ml.home_ml,
    ml.away_ml,
    ml.sharp_action as ml_sharp_action,
    
    -- Spreads data
    s.home_spread,
    s.away_spread,
    s.home_spread_price,
    s.away_spread_price,
    s.sharp_action as spread_sharp_action,
    
    -- Totals data
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
LEFT JOIN mlb_betting.moneyline ml ON g.id = ml.game_id
LEFT JOIN mlb_betting.spreads s ON g.id = s.game_id
LEFT JOIN mlb_betting.totals t ON g.id = t.game_id
ORDER BY g.game_datetime DESC;

-- ==============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ==============================================================================

COMMENT ON SCHEMA mlb_betting IS 'Schema for MLB betting data from SportsbookReview.com';
COMMENT ON TABLE public.games IS 'Main games table with SportsbookReview, MLB Stats API, and Action Network IDs';
COMMENT ON TABLE mlb_betting.moneyline IS 'Moneyline betting data and splits from SportsbookReview';
COMMENT ON TABLE mlb_betting.spreads IS 'Spread betting data and splits from SportsbookReview';
COMMENT ON TABLE mlb_betting.totals IS 'Total betting data and splits from SportsbookReview';

COMMENT ON COLUMN public.games.sportsbookreview_game_id IS 'Unique identifier from SportsbookReview.com';
COMMENT ON COLUMN public.games.mlb_stats_api_game_id IS 'Official MLB Stats API game ID (gamePk)';
COMMENT ON COLUMN public.games.action_network_game_id IS 'Action Network game ID (currently NULL)'; 