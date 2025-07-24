-- Migration 011: Create ML-Optimized CURATED Zone for Multi-Source Integration
-- Purpose: Implement comprehensive CURATED zone for ML model development
-- Integrates: Action Network, VSIN, SBD, MLB Stats API data sources
-- Date: 2025-07-24
-- Reference: docs/ml_model_development.md

-- ================================
-- Phase 1: Enhanced Games Master Table
-- ================================

-- Enhanced games table with cross-system identifiers and ML features container
CREATE TABLE IF NOT EXISTS curated.enhanced_games (
    id BIGSERIAL PRIMARY KEY,
    
    -- Cross-system identifiers for data correlation
    mlb_stats_api_game_id VARCHAR(50) UNIQUE, -- Authoritative MLB game ID
    action_network_game_id INTEGER, -- Action Network external game ID
    sbd_game_id VARCHAR(100), -- SBD game identifier
    vsin_game_key VARCHAR(100), -- VSIN correlation key
    
    -- Core game data (normalized team names)
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    home_team_full_name VARCHAR(100),
    away_team_full_name VARCHAR(100),
    
    -- Game scheduling (EST timezone)
    game_date DATE NOT NULL,
    game_time TIME,
    game_datetime TIMESTAMPTZ NOT NULL,
    season INTEGER NOT NULL CHECK (season >= 1876 AND season <= 2030),
    season_type VARCHAR(20) DEFAULT 'regular' CHECK (season_type IN ('regular', 'postseason', 'spring')),
    
    -- Game classification
    game_type VARCHAR(20) DEFAULT 'regular',
    game_number INTEGER DEFAULT 1 CHECK (game_number IN (1, 2)), -- Doubleheader support
    
    -- Venue information (from MLB Stats API)
    venue_name VARCHAR(200),
    venue_city VARCHAR(100),
    venue_state VARCHAR(50),
    venue_timezone VARCHAR(50),
    
    -- Weather data (impacts totals betting)
    temperature_fahrenheit INTEGER CHECK (temperature_fahrenheit BETWEEN -20 AND 120),
    wind_speed_mph INTEGER CHECK (wind_speed_mph BETWEEN 0 AND 100),
    wind_direction VARCHAR(10),
    humidity_pct INTEGER CHECK (humidity_pct BETWEEN 0 AND 100),
    weather_condition VARCHAR(50),
    
    -- Game outcomes (from MLB Stats API - authoritative)
    home_score INTEGER CHECK (home_score >= 0),
    away_score INTEGER CHECK (away_score >= 0),
    winning_team VARCHAR(10),
    game_status VARCHAR(20) DEFAULT 'scheduled' CHECK (game_status IN ('scheduled', 'live', 'final', 'postponed', 'cancelled', 'suspended')),
    total_runs INTEGER GENERATED ALWAYS AS (COALESCE(home_score, 0) + COALESCE(away_score, 0)) STORED,
    
    -- Starting pitcher information (for ML features)
    home_pitcher_name VARCHAR(100),
    away_pitcher_name VARCHAR(100),
    home_pitcher_era DECIMAL(4,2),
    away_pitcher_era DECIMAL(4,2),
    home_pitcher_throws CHAR(1) CHECK (home_pitcher_throws IN ('L', 'R')),
    away_pitcher_throws CHAR(1) CHECK (away_pitcher_throws IN ('L', 'R')),
    pitcher_handedness_matchup VARCHAR(10) GENERATED ALWAYS AS (
        CASE 
            WHEN home_pitcher_throws IS NOT NULL AND away_pitcher_throws IS NOT NULL 
            THEN away_pitcher_throws || ' vs ' || home_pitcher_throws
            ELSE NULL
        END
    ) STORED,
    
    -- ML Features Container (JSONB for flexibility and performance)
    feature_data JSONB DEFAULT '{}', -- Computed features for ML pipeline
    ml_metadata JSONB DEFAULT '{}', -- ML-specific metadata (model versions, etc.)
    
    -- Data quality and correlation confidence
    data_quality_score DECIMAL(3,2) DEFAULT 1.0 CHECK (data_quality_score BETWEEN 0 AND 1),
    mlb_correlation_confidence DECIMAL(3,2) DEFAULT 1.0 CHECK (mlb_correlation_confidence BETWEEN 0 AND 1),
    source_coverage_score DECIMAL(3,2) DEFAULT 0.0, -- How many sources contributed data
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT different_teams CHECK (home_team != away_team),
    CONSTRAINT valid_winner CHECK (winning_team IS NULL OR winning_team IN (home_team, away_team)),
    CONSTRAINT completed_game_has_scores CHECK (
        (game_status = 'final' AND home_score IS NOT NULL AND away_score IS NOT NULL) OR
        (game_status != 'final')
    )
);

-- ================================
-- Phase 2: Unified Betting Splits (Multi-Source)
-- ================================

-- Unified betting splits combining VSIN, SBD, and Action Network data
CREATE TABLE IF NOT EXISTS curated.unified_betting_splits (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Source identification and attribution
    data_source VARCHAR(20) NOT NULL CHECK (data_source IN ('vsin', 'sbd', 'action_network')),
    sportsbook_name VARCHAR(50) NOT NULL,
    sportsbook_id INTEGER, -- References core_betting.sportsbooks when available
    sportsbook_external_id VARCHAR(50), -- Source-specific sportsbook ID
    
    -- Market identification
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    
    -- Moneyline and spread betting splits
    bet_percentage_home DECIMAL(5,2) CHECK (bet_percentage_home BETWEEN 0 AND 100),
    bet_percentage_away DECIMAL(5,2) CHECK (bet_percentage_away BETWEEN 0 AND 100),
    money_percentage_home DECIMAL(5,2) CHECK (money_percentage_home BETWEEN 0 AND 100),
    money_percentage_away DECIMAL(5,2) CHECK (money_percentage_away BETWEEN 0 AND 100),
    
    -- Totals betting splits
    bet_percentage_over DECIMAL(5,2) CHECK (bet_percentage_over BETWEEN 0 AND 100),
    bet_percentage_under DECIMAL(5,2) CHECK (bet_percentage_under BETWEEN 0 AND 100),
    money_percentage_over DECIMAL(5,2) CHECK (money_percentage_over BETWEEN 0 AND 100),
    money_percentage_under DECIMAL(5,2) CHECK (money_percentage_under BETWEEN 0 AND 100),
    
    -- Sharp action indicators (key ML features)
    sharp_action_direction VARCHAR(10) CHECK (sharp_action_direction IN ('home', 'away', 'over', 'under', 'none')),
    sharp_action_strength VARCHAR(10) CHECK (sharp_action_strength IN ('weak', 'moderate', 'strong')),
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    
    -- Current odds for context
    current_home_ml INTEGER,
    current_away_ml INTEGER,
    current_spread_home DECIMAL(4,1),
    current_spread_away DECIMAL(4,1),
    current_total_line DECIMAL(4,1),
    current_over_odds INTEGER,
    current_under_odds INTEGER,
    
    -- Temporal data with ML cutoff enforcement
    collected_at TIMESTAMPTZ NOT NULL,
    game_start_time TIMESTAMPTZ NOT NULL,
    minutes_before_game INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (game_start_time - collected_at)) / 60
    ) STORED,
    
    -- Data quality metrics
    data_completeness_score DECIMAL(3,2) DEFAULT 0.0,
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints for ML data leakage prevention
    CONSTRAINT valid_ml_cutoff CHECK (minutes_before_game >= 60), -- Enforce 60-minute cutoff
    CONSTRAINT valid_home_splits CHECK (
        (bet_percentage_home IS NULL AND money_percentage_home IS NULL) OR
        (bet_percentage_home + bet_percentage_away <= 102 AND -- Allow for small rounding errors
         money_percentage_home + money_percentage_away <= 102)
    ),
    CONSTRAINT valid_total_splits CHECK (
        (bet_percentage_over IS NULL AND money_percentage_over IS NULL) OR
        (bet_percentage_over + bet_percentage_under <= 102 AND
         money_percentage_over + money_percentage_under <= 102)
    )
);

-- ================================
-- Phase 3: ML Temporal Features Table
-- ================================

-- Temporal features with strict 60-minute cutoff for data leakage prevention
CREATE TABLE IF NOT EXISTS curated.ml_temporal_features (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Critical temporal constraint
    feature_cutoff_time TIMESTAMPTZ NOT NULL, -- Exactly 60min before first pitch
    game_start_time TIMESTAMPTZ NOT NULL,
    minutes_before_game INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (game_start_time - feature_cutoff_time)) / 60
    ) STORED,
    
    -- Line movement features (from Action Network historical data)
    line_movement_velocity_60min DECIMAL(10,4), -- Rate of odds changes in final hour
    opening_to_current_ml_home DECIMAL(10,2), -- Home ML movement from open
    opening_to_current_ml_away DECIMAL(10,2), -- Away ML movement from open
    opening_to_current_spread_home DECIMAL(4,1), -- Spread movement
    opening_to_current_total DECIMAL(4,1), -- Total line movement
    
    -- Line movement patterns
    ml_movement_direction VARCHAR(10), -- 'toward_home', 'toward_away', 'stable'
    spread_movement_direction VARCHAR(10),
    total_movement_direction VARCHAR(10), -- 'toward_over', 'toward_under', 'stable'
    movement_consistency_score DECIMAL(3,2), -- Cross-book movement consistency
    
    -- Sharp action synthesis (from VSIN + SBD)
    sharp_action_intensity_60min DECIMAL(5,2), -- Aggregated sharp action strength
    reverse_line_movement_signals INTEGER DEFAULT 0, -- Count of RLM instances
    steam_move_count INTEGER DEFAULT 0, -- Cross-book simultaneous moves
    
    -- Public vs sharp divergence (key ML feature)
    money_vs_bet_divergence_home DECIMAL(5,2), -- Money% - Bet% for home team
    money_vs_bet_divergence_away DECIMAL(5,2),
    money_vs_bet_divergence_over DECIMAL(5,2),
    money_vs_bet_divergence_under DECIMAL(5,2),
    
    -- Cross-sportsbook consensus features
    cross_sbook_consensus_60min DECIMAL(5,2), -- Agreement across sportsbooks
    sportsbook_variance_ml DECIMAL(8,2), -- Variance in ML odds
    sportsbook_variance_spread DECIMAL(4,2), -- Variance in spread
    sportsbook_variance_total DECIMAL(4,2), -- Variance in total
    participating_sportsbooks INTEGER, -- Number of books with data
    
    -- Public sentiment shift
    public_sentiment_shift_60min DECIMAL(5,2), -- Change in public betting direction
    
    -- Source-specific features
    dk_money_vs_bet_gap DECIMAL(5,2), -- VSIN DraftKings specific
    circa_money_vs_bet_gap DECIMAL(5,2), -- VSIN Circa specific
    
    -- Feature versioning and metadata
    feature_version VARCHAR(20) NOT NULL DEFAULT 'v1.0',
    feature_hash VARCHAR(64), -- For caching and deduplication
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Critical constraint for ML data leakage prevention
    CONSTRAINT enforce_ml_cutoff CHECK (minutes_before_game >= 60),
    CONSTRAINT unique_game_cutoff UNIQUE (game_id, feature_cutoff_time, feature_version)
);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE curated.enhanced_games IS 'ML-optimized games master table with cross-system identifiers and feature containers. Integrates Action Network, VSIN, SBD, and MLB Stats API data.';
COMMENT ON COLUMN curated.enhanced_games.feature_data IS 'JSONB container for computed ML features, optimized for querying and indexing';
COMMENT ON COLUMN curated.enhanced_games.ml_metadata IS 'ML-specific metadata including model versions, experiment tracking, and pipeline information';
COMMENT ON COLUMN curated.enhanced_games.source_coverage_score IS 'Data quality metric indicating how many data sources contributed information (0.0-1.0)';

COMMENT ON TABLE curated.unified_betting_splits IS 'Multi-source betting splits combining VSIN (DK/Circa), SBD (9+ sportsbooks), and Action Network data with ML cutoff enforcement';
COMMENT ON COLUMN curated.unified_betting_splits.minutes_before_game IS 'Calculated minutes before game start - must be >= 60 for ML feature usage';
COMMENT ON COLUMN curated.unified_betting_splits.sharp_action_direction IS 'Direction of sharp money movement detected across sources';

COMMENT ON TABLE curated.ml_temporal_features IS 'Temporal features with strict 60-minute cutoff for ML data leakage prevention. Combines line movement, sharp action, and consensus data.';
COMMENT ON COLUMN curated.ml_temporal_features.feature_cutoff_time IS 'Exact timestamp when features were captured - must be >= 60 minutes before game start';
COMMENT ON COLUMN curated.ml_temporal_features.sharp_action_intensity_60min IS 'Aggregated sharp action strength from multiple sources (VSIN + SBD)';