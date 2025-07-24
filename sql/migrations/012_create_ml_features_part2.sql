-- Migration 012: ML Features Part 2 - Market Structure and Team Performance
-- Purpose: Complete ML feature tables for comprehensive model development
-- Continues: Migration 011 (ML CURATED zone foundation)
-- Date: 2025-07-24

-- ================================
-- Phase 4: ML Market Structure Features
-- ================================

-- Market structure and efficiency features
CREATE TABLE IF NOT EXISTS curated.ml_market_features (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Market efficiency metrics (from Action Network historical analysis)
    closing_line_efficiency DECIMAL(5,4), -- Historical accuracy of final pre-game lines
    market_liquidity_score DECIMAL(5,2), -- Volume and depth indicators
    line_stability_score DECIMAL(3,2), -- How stable lines were leading up to game
    
    -- Steam move detection (synchronized cross-book movements)
    steam_move_indicators INTEGER DEFAULT 0, -- Count of detected steam moves
    steam_move_magnitude DECIMAL(6,2), -- Average magnitude of steam moves
    largest_steam_move DECIMAL(6,2), -- Biggest single move
    steam_move_sportsbooks TEXT[], -- Books that participated in steam moves
    
    -- Cross-source arbitrage opportunities
    max_ml_arbitrage_opportunity DECIMAL(5,2), -- Best ML arbitrage found
    max_spread_arbitrage_opportunity DECIMAL(5,2), -- Best spread arbitrage
    max_total_arbitrage_opportunity DECIMAL(5,2), -- Best total arbitrage
    arbitrage_duration_minutes INTEGER, -- How long arbitrage opportunities lasted
    
    -- Sportsbook coverage and consensus
    participating_sportsbooks TEXT[] NOT NULL DEFAULT '{}', -- All books with data
    sportsbook_count INTEGER GENERATED ALWAYS AS (array_length(participating_sportsbooks, 1)) STORED,
    sportsbook_consensus_strength DECIMAL(3,2), -- Agreement level across books
    
    -- Market depth indicators
    best_ml_spread INTEGER, -- Difference between best home/away ML odds
    best_total_spread INTEGER, -- Difference between best over/under odds
    odds_efficiency_score DECIMAL(3,2), -- How efficient odds pricing appears
    
    -- Line movement patterns
    total_line_movements INTEGER DEFAULT 0, -- Count of significant line changes
    average_movement_magnitude DECIMAL(6,2), -- Average size of movements
    movement_frequency DECIMAL(6,2), -- Movements per hour
    late_movement_indicator BOOLEAN DEFAULT FALSE, -- Significant moves in final hour
    
    -- Sharp vs public indicators
    sharp_public_divergence_ml DECIMAL(5,2), -- Difference in ML preferences
    sharp_public_divergence_spread DECIMAL(5,2),
    sharp_public_divergence_total DECIMAL(5,2),
    
    -- Market microstructure
    bid_ask_spread_estimate DECIMAL(6,2), -- Estimated transaction costs
    market_maker_vs_flow DECIMAL(3,2), -- Market maker advantage indicator
    
    -- Feature metadata
    feature_version VARCHAR(20) NOT NULL DEFAULT 'v1.0',
    calculation_timestamp TIMESTAMPTZ NOT NULL,
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_game_market_features UNIQUE (game_id, feature_version)
);

-- ================================
-- Phase 5: ML Team Performance Features
-- ================================

-- Team performance features with MLB Stats API enrichment
CREATE TABLE IF NOT EXISTS curated.ml_team_features (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Recent form metrics (weighted by recency)
    home_recent_form_weighted DECIMAL(5,3), -- Last 10 games with decay weighting
    away_recent_form_weighted DECIMAL(5,3),
    home_last_5_record VARCHAR(10), -- e.g., "3-2"
    away_last_5_record VARCHAR(10),
    home_last_10_record VARCHAR(10),
    away_last_10_record VARCHAR(10),
    
    -- Head-to-head historical performance
    h2h_home_wins_last_10 INTEGER DEFAULT 0, -- H2H wins for home team
    h2h_away_wins_last_10 INTEGER DEFAULT 0,
    h2h_total_games INTEGER DEFAULT 0, -- Total H2H games in sample
    h2h_avg_total_runs DECIMAL(4,1), -- Average total runs in H2H matchups
    h2h_home_advantage DECIMAL(4,3), -- Home field advantage in this matchup
    
    -- Season performance metrics
    home_season_record VARCHAR(10), -- e.g., "45-32"
    away_season_record VARCHAR(10),
    home_win_pct DECIMAL(4,3),
    away_win_pct DECIMAL(4,3),
    home_runs_per_game DECIMAL(4,2),
    away_runs_per_game DECIMAL(4,2),
    home_runs_allowed_per_game DECIMAL(4,2),
    away_runs_allowed_per_game DECIMAL(4,2),
    
    -- Pitcher-specific features (enhanced from MLB Stats API)
    home_pitcher_season_era DECIMAL(4,2),
    away_pitcher_season_era DECIMAL(4,2),
    home_pitcher_whip DECIMAL(4,3), -- Walks + Hits per Inning Pitched
    away_pitcher_whip DECIMAL(4,3),
    home_pitcher_k9 DECIMAL(4,2), -- Strikeouts per 9 innings
    away_pitcher_k9 DECIMAL(4,2),
    home_pitcher_hr9 DECIMAL(4,2), -- Home runs per 9 innings
    away_pitcher_hr9 DECIMAL(4,2),
    
    -- Pitcher vs opposing team history
    home_pitcher_vs_opponent_era DECIMAL(4,2), -- ERA against this specific opponent
    away_pitcher_vs_opponent_era DECIMAL(4,2),
    home_pitcher_opponent_games INTEGER DEFAULT 0, -- Games against this opponent
    away_pitcher_opponent_games INTEGER DEFAULT 0,
    
    -- Bullpen factors (affects late-game betting)
    home_bullpen_era DECIMAL(4,2),
    away_bullpen_era DECIMAL(4,2),
    home_bullpen_recent_usage DECIMAL(4,2), -- Innings pitched last 3 days
    away_bullpen_recent_usage DECIMAL(4,2),
    home_bullpen_fatigue_score DECIMAL(3,2), -- 0-1 fatigue indicator
    away_bullpen_fatigue_score DECIMAL(3,2),
    
    -- Venue-specific performance
    home_field_advantage_factor DECIMAL(4,3), -- Historical home field advantage
    venue_total_factor DECIMAL(4,3), -- Venue impact on over/under
    venue_home_team_factor DECIMAL(4,3), -- How well home team plays at venue
    venue_away_team_factor DECIMAL(4,3), -- How well away team plays at this venue
    
    -- Weather impact factors
    temperature_impact_total DECIMAL(4,3), -- Expected impact on total runs
    wind_impact_total DECIMAL(4,3), -- Wind impact on over/under
    weather_advantage_home DECIMAL(4,3), -- Weather favor for home team
    weather_advantage_away DECIMAL(4,3), -- Weather favor for away team
    
    -- Rest and travel factors
    home_days_rest INTEGER DEFAULT 0, -- Days since last game
    away_days_rest INTEGER DEFAULT 0,
    away_travel_distance INTEGER, -- Miles traveled for away team
    away_timezone_change INTEGER, -- Hours of timezone change
    
    -- Lineup and injury factors
    home_key_players_out INTEGER DEFAULT 0, -- Count of key injured players
    away_key_players_out INTEGER DEFAULT 0,
    home_lineup_strength DECIMAL(3,2), -- Estimated lineup strength (0-1)
    away_lineup_strength DECIMAL(3,2),
    
    -- Situational factors
    home_motivation_factor DECIMAL(3,2), -- Playoff race, rivalry, etc.
    away_motivation_factor DECIMAL(3,2),
    game_importance_score DECIMAL(3,2), -- Overall game importance
    
    -- Feature metadata
    feature_version VARCHAR(20) NOT NULL DEFAULT 'v1.0',
    mlb_api_last_updated TIMESTAMPTZ, -- When MLB data was last refreshed
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_game_team_features UNIQUE (game_id, feature_version)
);

-- ================================
-- Phase 6: ML Feature Vectors (Consolidated)
-- ================================

-- Consolidated feature vectors for ML model input
CREATE TABLE IF NOT EXISTS curated.ml_feature_vectors (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.enhanced_games(id) ON DELETE CASCADE,
    
    -- Feature engineering metadata
    feature_cutoff_time TIMESTAMPTZ NOT NULL, -- 60min before first pitch
    feature_version VARCHAR(20) NOT NULL DEFAULT 'v1.0',
    feature_hash VARCHAR(64) UNIQUE, -- SHA-256 hash for caching and deduplication
    
    -- Consolidated features (JSONB for flexibility while maintaining performance)
    temporal_features JSONB NOT NULL DEFAULT '{}', -- From ml_temporal_features
    market_features JSONB NOT NULL DEFAULT '{}', -- From ml_market_features  
    team_features JSONB NOT NULL DEFAULT '{}', -- From ml_team_features
    betting_splits_features JSONB NOT NULL DEFAULT '{}', -- From unified_betting_splits
    
    -- Additional computed features
    derived_features JSONB DEFAULT '{}', -- Features computed from base features
    interaction_features JSONB DEFAULT '{}', -- Feature interactions and combinations
    
    -- Data quality and completeness metrics
    feature_completeness_score DECIMAL(3,2) NOT NULL DEFAULT 0.0, -- 0-1 completeness
    data_source_coverage INTEGER NOT NULL DEFAULT 0, -- How many sources contributed
    missing_feature_count INTEGER DEFAULT 0,
    total_feature_count INTEGER DEFAULT 0,
    
    -- Source attribution
    action_network_data BOOLEAN DEFAULT FALSE,
    vsin_data BOOLEAN DEFAULT FALSE,
    sbd_data BOOLEAN DEFAULT FALSE,
    mlb_stats_api_data BOOLEAN DEFAULT FALSE,
    
    -- ML pipeline metadata
    normalization_applied BOOLEAN DEFAULT FALSE,
    scaling_method VARCHAR(20), -- 'standard', 'minmax', 'robust', etc.
    feature_selection_applied BOOLEAN DEFAULT FALSE,
    dimensionality_reduction VARCHAR(20), -- 'pca', 'lda', etc.
    
    -- Temporal tracking
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_completeness CHECK (feature_completeness_score BETWEEN 0 AND 1),
    CONSTRAINT valid_coverage CHECK (data_source_coverage BETWEEN 0 AND 4),
    CONSTRAINT unique_game_feature_vector UNIQUE (game_id, feature_version, feature_cutoff_time)
);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE curated.ml_market_features IS 'Market structure and efficiency features for ML models. Includes arbitrage opportunities, steam moves, and sportsbook consensus metrics.';
COMMENT ON COLUMN curated.ml_market_features.steam_move_indicators IS 'Count of synchronized cross-sportsbook line movements indicating sharp action';
COMMENT ON COLUMN curated.ml_market_features.closing_line_efficiency IS 'Historical accuracy of final pre-game lines as proxy for market efficiency';

COMMENT ON TABLE curated.ml_team_features IS 'Team performance features enriched with MLB Stats API data. Includes pitcher matchups, bullpen factors, and venue-specific performance.';
COMMENT ON COLUMN curated.ml_team_features.h2h_home_advantage IS 'Home field advantage specific to this team matchup based on historical data';
COMMENT ON COLUMN curated.ml_team_features.bullpen_fatigue_score IS 'Bullpen fatigue indicator (0-1) based on recent usage patterns';

COMMENT ON TABLE curated.ml_feature_vectors IS 'Consolidated feature vectors for ML model input. Combines all feature types with data quality metrics and source attribution.';
COMMENT ON COLUMN curated.ml_feature_vectors.feature_hash IS 'SHA-256 hash of feature vector for caching, deduplication, and version control';
COMMENT ON COLUMN curated.ml_feature_vectors.data_source_coverage IS 'Number of data sources that contributed to this feature vector (0-4)';