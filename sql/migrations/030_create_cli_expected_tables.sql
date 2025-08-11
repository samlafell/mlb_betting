-- Migration: Create CLI-Expected Tables
-- Purpose: Create the tables that the CLI commands expect for proper functioning
-- Creates curated.games, curated.betting_lines, curated.ml_experiments, and curated.ml_predictions
-- Date: 2025-08-01

-- ================================
-- Create CURATED schema (if not exists)
-- ================================

CREATE SCHEMA IF NOT EXISTS curated;

-- ================================
-- Create curated.games table (expected by CLI)
-- ================================

CREATE TABLE IF NOT EXISTS curated.games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) UNIQUE NOT NULL,  -- External game ID for references
    
    -- Core game information
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_date DATE NOT NULL,
    game_time TIME,
    game_datetime TIMESTAMPTZ,
    
    -- Game status and results  
    game_status VARCHAR(50) DEFAULT 'scheduled',
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(100),
    
    -- Season information
    season INTEGER,
    season_type VARCHAR(20) DEFAULT 'regular',
    
    -- Venue information
    venue_name VARCHAR(200),
    
    -- Data quality
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- Create curated.betting_lines table (expected by CLI)  
-- ================================

CREATE TABLE IF NOT EXISTS curated.betting_lines (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL REFERENCES curated.games(game_id),
    
    -- Sportsbook information
    sportsbook_name VARCHAR(100) NOT NULL,
    sportsbook_id INTEGER,
    
    -- Market information
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    side VARCHAR(10) NOT NULL CHECK (side IN ('home', 'away', 'over', 'under')),
    
    -- Line data
    odds_american INTEGER,
    odds_decimal DECIMAL(8,4),
    line_value DECIMAL(5,2),
    
    -- Strategy recommendations (for CLI display)
    recommendation VARCHAR(50),
    confidence_score DECIMAL(3,2),
    expected_value DECIMAL(8,4),
    
    -- Timestamps
    line_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- Create curated.ml_experiments table (expected by CLI)
-- ================================

CREATE TABLE IF NOT EXISTS curated.ml_experiments (
    id SERIAL PRIMARY KEY,
    
    -- Model identification
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) DEFAULT 'v1.0',
    
    -- Model status
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'completed', 'failed', 'retired')),
    
    -- Performance metrics
    roi DECIMAL(8,4),
    accuracy DECIMAL(5,4),
    precision_score DECIMAL(5,4),
    recall_score DECIMAL(5,4), 
    f1_score DECIMAL(5,4),
    
    -- Prediction counts
    total_predictions INTEGER DEFAULT 0,
    correct_predictions INTEGER DEFAULT 0,
    
    -- Model metadata
    feature_importance JSONB,
    hyperparameters JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_prediction TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(model_name, model_version)
);

-- ================================
-- Create curated.ml_predictions table (expected by CLI)
-- ================================

CREATE TABLE IF NOT EXISTS curated.ml_predictions (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL REFERENCES curated.games(game_id),
    
    -- Model information
    model_name VARCHAR(100) NOT NULL,
    
    -- Prediction details
    prediction_type VARCHAR(50) NOT NULL,
    prediction_value VARCHAR(100),
    confidence_score DECIMAL(3,2),
    betting_recommendation VARCHAR(100),
    expected_value DECIMAL(8,4),
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- Create indexes for performance
-- ================================

-- Games table indexes
CREATE INDEX IF NOT EXISTS idx_curated_games_game_id ON curated.games(game_id);
CREATE INDEX IF NOT EXISTS idx_curated_games_date ON curated.games(game_date);
CREATE INDEX IF NOT EXISTS idx_curated_games_teams ON curated.games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_curated_games_status ON curated.games(game_status);

-- Betting lines indexes
CREATE INDEX IF NOT EXISTS idx_curated_betting_lines_game_id ON curated.betting_lines(game_id);
CREATE INDEX IF NOT EXISTS idx_curated_betting_lines_sportsbook ON curated.betting_lines(sportsbook_name);
CREATE INDEX IF NOT EXISTS idx_curated_betting_lines_market ON curated.betting_lines(market_type, side);
CREATE INDEX IF NOT EXISTS idx_curated_betting_lines_recommendation ON curated.betting_lines(recommendation);

-- ML experiments indexes
CREATE INDEX IF NOT EXISTS idx_curated_ml_experiments_name ON curated.ml_experiments(model_name);
CREATE INDEX IF NOT EXISTS idx_curated_ml_experiments_status ON curated.ml_experiments(status);
CREATE INDEX IF NOT EXISTS idx_curated_ml_experiments_roi ON curated.ml_experiments(roi DESC);

-- ML predictions indexes
CREATE INDEX IF NOT EXISTS idx_curated_ml_predictions_game_id ON curated.ml_predictions(game_id);
CREATE INDEX IF NOT EXISTS idx_curated_ml_predictions_model ON curated.ml_predictions(model_name);
CREATE INDEX IF NOT EXISTS idx_curated_ml_predictions_confidence ON curated.ml_predictions(confidence_score DESC);

-- ================================
-- Insert sample data for CLI testing
-- ================================

-- Insert some sample games
INSERT INTO curated.games (game_id, home_team, away_team, game_date, game_datetime, season)
VALUES 
    ('2025-08-01-NYY-BOS', 'New York Yankees', 'Boston Red Sox', '2025-08-01', '2025-08-01 19:10:00-04', 2025),
    ('2025-08-01-LAD-SF', 'Los Angeles Dodgers', 'San Francisco Giants', '2025-08-01', '2025-08-01 22:15:00-07', 2025),
    ('2025-08-02-HOU-TEX', 'Houston Astros', 'Texas Rangers', '2025-08-02', '2025-08-02 20:05:00-05', 2025)
ON CONFLICT (game_id) DO NOTHING;

-- Insert sample ML experiments
INSERT INTO curated.ml_experiments (model_name, model_version, status, roi, accuracy, total_predictions, correct_predictions)
VALUES 
    ('sharp_action_processor', 'v1.0', 'active', 0.052, 0.582, 245, 143),
    ('consensus_processor', 'v1.0', 'active', 0.031, 0.556, 189, 105),
    ('line_movement_processor', 'v1.2', 'active', 0.067, 0.601, 156, 94),
    ('hybrid_sharp_processor', 'v2.1', 'active', 0.089, 0.634, 98, 62)
ON CONFLICT (model_name, model_version) DO NOTHING;

-- Insert sample betting lines with recommendations
INSERT INTO curated.betting_lines (game_id, sportsbook_name, market_type, side, odds_american, recommendation, confidence_score, expected_value, line_timestamp)
VALUES 
    ('2025-08-01-NYY-BOS', 'DraftKings', 'moneyline', 'home', -145, 'PASS', 0.65, 0.023, '2025-08-01 18:00:00-04'),
    ('2025-08-01-NYY-BOS', 'FanDuel', 'moneyline', 'away', 125, 'BET', 0.78, 0.067, '2025-08-01 18:00:00-04'),
    ('2025-08-01-LAD-SF', 'BetMGM', 'spread', 'home', -110, 'BET', 0.72, 0.045, '2025-08-01 21:00:00-07'),
    ('2025-08-02-HOU-TEX', 'Caesars', 'total', 'over', -105, 'LEAN', 0.58, 0.018, '2025-08-02 19:00:00-05')
ON CONFLICT DO NOTHING;

-- Insert sample ML predictions
INSERT INTO curated.ml_predictions (game_id, model_name, prediction_type, prediction_value, confidence_score, betting_recommendation, expected_value)
VALUES 
    ('2025-08-01-NYY-BOS', 'sharp_action_processor', 'moneyline', 'away', 0.78, 'BET Boston Red Sox ML', 0.067),
    ('2025-08-01-LAD-SF', 'line_movement_processor', 'spread', 'home', 0.72, 'BET Los Angeles Dodgers -1.5', 0.045),
    ('2025-08-02-HOU-TEX', 'consensus_processor', 'total', 'over', 0.65, 'LEAN Over 8.5', 0.023)
ON CONFLICT DO NOTHING;

-- ================================
-- Comments and documentation
-- ================================

COMMENT ON TABLE curated.games IS 'Games table expected by CLI commands for predictions and analysis';
COMMENT ON TABLE curated.betting_lines IS 'Betting lines with strategy recommendations for CLI display';
COMMENT ON TABLE curated.ml_experiments IS 'ML model experiments and performance tracking for CLI commands';
COMMENT ON TABLE curated.ml_predictions IS 'ML model predictions for CLI prediction commands';

COMMENT ON COLUMN curated.games.game_id IS 'External game identifier used for cross-table references';
COMMENT ON COLUMN curated.betting_lines.recommendation IS 'Strategy recommendation: BET, PASS, LEAN';
COMMENT ON COLUMN curated.ml_experiments.roi IS 'Return on investment as decimal (0.05 = 5%)';
COMMENT ON COLUMN curated.ml_predictions.confidence_score IS 'Model confidence from 0.0 to 1.0';