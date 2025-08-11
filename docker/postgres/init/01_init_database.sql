-- MLB Betting Database Initialization Script
-- This script initializes the basic database structure for Docker container startup
-- Created for PostgreSQL 15+ with MLB betting system requirements

-- Create basic schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging; 
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS analysis;

-- Set default search path to include all schemas
ALTER DATABASE mlb_betting SET search_path TO public, raw_data, staging, curated, analysis;

-- Create basic extension for UUID generation (commonly needed)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create basic tables required by MLflow
-- MLflow will create its own tables, but we ensure they have a proper home

-- Basic teams reference table for consistency
CREATE TABLE IF NOT EXISTS staging.teams (
    id SERIAL PRIMARY KEY,
    team_abbreviation VARCHAR(10) UNIQUE NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    league VARCHAR(10),
    division VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Basic sportsbooks reference table
CREATE TABLE IF NOT EXISTS staging.sportsbooks (
    id SERIAL PRIMARY KEY,
    sportsbook_name VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert basic team data for MLB (essential for system operation)
INSERT INTO staging.teams (team_abbreviation, team_name, city, league, division) VALUES
    ('ARI', 'Arizona Diamondbacks', 'Arizona', 'NL', 'West'),
    ('ATL', 'Atlanta Braves', 'Atlanta', 'NL', 'East'),
    ('BAL', 'Baltimore Orioles', 'Baltimore', 'AL', 'East'),
    ('BOS', 'Boston Red Sox', 'Boston', 'AL', 'East'),
    ('CHC', 'Chicago Cubs', 'Chicago', 'NL', 'Central'),
    ('CWS', 'Chicago White Sox', 'Chicago', 'AL', 'Central'),
    ('CIN', 'Cincinnati Reds', 'Cincinnati', 'NL', 'Central'),
    ('CLE', 'Cleveland Guardians', 'Cleveland', 'AL', 'Central'),
    ('COL', 'Colorado Rockies', 'Colorado', 'NL', 'West'),
    ('DET', 'Detroit Tigers', 'Detroit', 'AL', 'Central'),
    ('HOU', 'Houston Astros', 'Houston', 'AL', 'West'),
    ('KC', 'Kansas City Royals', 'Kansas City', 'AL', 'Central'),
    ('LAA', 'Los Angeles Angels', 'Los Angeles', 'AL', 'West'),
    ('LAD', 'Los Angeles Dodgers', 'Los Angeles', 'NL', 'West'),
    ('MIA', 'Miami Marlins', 'Miami', 'NL', 'East'),
    ('MIL', 'Milwaukee Brewers', 'Milwaukee', 'NL', 'Central'),
    ('MIN', 'Minnesota Twins', 'Minnesota', 'AL', 'Central'),
    ('NYM', 'New York Mets', 'New York', 'NL', 'East'),
    ('NYY', 'New York Yankees', 'New York', 'AL', 'East'),
    ('OAK', 'Oakland Athletics', 'Oakland', 'AL', 'West'),
    ('PHI', 'Philadelphia Phillies', 'Philadelphia', 'NL', 'East'),
    ('PIT', 'Pittsburgh Pirates', 'Pittsburgh', 'NL', 'Central'),
    ('SD', 'San Diego Padres', 'San Diego', 'NL', 'West'),
    ('SF', 'San Francisco Giants', 'San Francisco', 'NL', 'West'),
    ('SEA', 'Seattle Mariners', 'Seattle', 'AL', 'West'),
    ('STL', 'St. Louis Cardinals', 'St. Louis', 'NL', 'Central'),
    ('TB', 'Tampa Bay Rays', 'Tampa Bay', 'AL', 'East'),
    ('TEX', 'Texas Rangers', 'Texas', 'AL', 'West'),
    ('TOR', 'Toronto Blue Jays', 'Toronto', 'AL', 'East'),
    ('WSH', 'Washington Nationals', 'Washington', 'NL', 'East')
ON CONFLICT (team_abbreviation) DO NOTHING;

-- Insert basic sportsbook data (essential for betting system)
INSERT INTO staging.sportsbooks (sportsbook_name, display_name) VALUES
    ('draftkings', 'DraftKings'),
    ('fanduel', 'FanDuel'),
    ('betmgm', 'BetMGM'),
    ('caesars', 'Caesars'),
    ('bet365', 'Bet365'),
    ('fanatics', 'Fanatics'),
    ('circa', 'Circa Sports'),
    ('westgate', 'Westgate'),
    ('pinnacle', 'Pinnacle')
ON CONFLICT (sportsbook_name) DO NOTHING;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_teams_abbreviation ON staging.teams(team_abbreviation);
CREATE INDEX IF NOT EXISTS idx_sportsbooks_name ON staging.sportsbooks(sportsbook_name);

-- Set permissions for database user
GRANT USAGE ON SCHEMA raw_data, staging, curated, analysis TO mlb_betting;
GRANT CREATE ON SCHEMA raw_data, staging, curated, analysis TO mlb_betting;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data, staging, curated, analysis TO mlb_betting;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw_data, staging, curated, analysis TO mlb_betting;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data, staging, curated, analysis 
    GRANT ALL PRIVILEGES ON TABLES TO mlb_betting;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data, staging, curated, analysis 
    GRANT ALL PRIVILEGES ON SEQUENCES TO mlb_betting;

-- Log successful initialization
\echo 'MLB Betting Database initialization completed successfully'