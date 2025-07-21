-- Migration: Create RAW/STAGING/CURATED Pipeline Zones
-- Purpose: Implement three-tier data pipeline architecture
-- Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
-- Created: 2025-07-21

-- ================================
-- Phase 1: Create Pipeline Schemas
-- ================================

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

-- Add comments to schemas
COMMENT ON SCHEMA raw_data IS 'RAW Zone - Stores unprocessed data exactly as received from external sources';
COMMENT ON SCHEMA staging IS 'STAGING Zone - Cleaned, normalized, and validated data ready for analysis';
COMMENT ON SCHEMA curated IS 'CURATED Zone - Feature-enriched, analysis-ready datasets with ML features';

-- ================================
-- RAW Zone Tables
-- ================================

-- Core Betting Lines Data (Raw from External Sources)
CREATE TABLE raw_data.betting_lines_raw (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    source VARCHAR(50) NOT NULL, -- 'action_network', 'sbd', 'vsin', etc.
    game_external_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    bet_type VARCHAR(50), -- 'spread', 'total', 'moneyline'
    line_value DECIMAL(10,3), -- spread value or total points
    odds_american INTEGER, -- American odds format (-110, +150, etc.)
    odds_decimal DECIMAL(10,6), -- Decimal odds format
    team_type VARCHAR(20), -- 'home', 'away', 'over', 'under'
    raw_data JSONB, -- Complete raw JSON from source
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw Moneylines from All Sources
CREATE TABLE raw_data.moneylines_raw (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    source VARCHAR(50) NOT NULL,
    game_external_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_name VARCHAR(100),
    away_team_name VARCHAR(100),
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw Spreads from All Sources  
CREATE TABLE raw_data.spreads_raw (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    source VARCHAR(50) NOT NULL,
    game_external_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    spread_value DECIMAL(4,1),
    spread_odds INTEGER,
    favorite_team VARCHAR(100),
    underdog_team VARCHAR(100),
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw Totals from All Sources
CREATE TABLE raw_data.totals_raw (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    source VARCHAR(50) NOT NULL,
    game_external_id VARCHAR(255),
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    total_points DECIMAL(4,1),
    over_odds INTEGER,
    under_odds INTEGER,
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw Line Movement History
CREATE TABLE raw_data.line_movements_raw (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    source VARCHAR(50) NOT NULL,
    game_external_id VARCHAR(255),
    bet_type VARCHAR(50),
    sportsbook_name VARCHAR(100),
    previous_value DECIMAL(10,3),
    new_value DECIMAL(10,3),
    previous_odds INTEGER,
    new_odds INTEGER,
    movement_time TIMESTAMPTZ,
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw Data by Source (Source-Specific Tables)
CREATE TABLE raw_data.action_network_games (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE,
    raw_response JSONB NOT NULL,
    endpoint_url TEXT,
    response_status INTEGER,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw_data.action_network_odds (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255),
    sportsbook_key VARCHAR(100),
    raw_odds JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw_data.sbd_betting_splits (
    id BIGSERIAL PRIMARY KEY,
    external_matchup_id VARCHAR(255),
    raw_response JSONB NOT NULL,
    api_endpoint TEXT,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw_data.vsin_data (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    data_type VARCHAR(100), -- 'game', 'odds', 'splits', etc.
    raw_response JSONB NOT NULL,
    source_feed VARCHAR(100),
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw_data.mlb_stats_api (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255),
    api_endpoint TEXT,
    raw_response JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    game_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Collection Audit Trail
CREATE TABLE raw_data.collection_log (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    collection_type VARCHAR(100),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'in_progress', -- 'in_progress', 'completed', 'failed'
    records_collected INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE raw_data.source_health (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'healthy', 'degraded', 'down'
    response_time_ms INTEGER,
    last_successful_collection TIMESTAMPTZ,
    last_error TEXT,
    error_count_24h INTEGER DEFAULT 0,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- STAGING Zone Tables
-- ================================

-- Processed Betting Lines (Cleaned from Raw)
CREATE TABLE staging.moneylines (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES raw_data.moneylines_raw(id),
    game_id INTEGER, -- Will reference staging.games
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    home_odds INTEGER,
    away_odds INTEGER,
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    validation_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'validated', 'rejected'
    validation_errors JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.spreads (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES raw_data.spreads_raw(id),
    game_id INTEGER,
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    spread_value DECIMAL(4,1),
    spread_odds INTEGER,
    favorite_team_normalized VARCHAR(100),
    underdog_team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2),
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.totals (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES raw_data.totals_raw(id),
    game_id INTEGER,
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100),
    total_points DECIMAL(4,1),
    over_odds INTEGER,
    under_odds INTEGER,
    data_quality_score DECIMAL(3,2),
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unified cleaned betting lines
CREATE TABLE staging.betting_lines (
    id BIGSERIAL PRIMARY KEY,
    raw_betting_lines_id BIGINT REFERENCES raw_data.betting_lines_raw(id),
    game_id INTEGER,
    sportsbook_id INTEGER,
    bet_type VARCHAR(50),
    line_value DECIMAL(10,3),
    odds_american INTEGER,
    team_type VARCHAR(20),
    team_normalized VARCHAR(100),
    data_quality_score DECIMAL(3,2),
    validation_status VARCHAR(20) DEFAULT 'pending',
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Processed movement history with patterns
CREATE TABLE staging.line_movements (
    id BIGSERIAL PRIMARY KEY,
    raw_movement_id BIGINT REFERENCES raw_data.line_movements_raw(id),
    game_id INTEGER,
    bet_type VARCHAR(50),
    sportsbook_name VARCHAR(100),
    previous_value DECIMAL(10,3),
    new_value DECIMAL(10,3),
    movement_size DECIMAL(10,3), -- Calculated movement size
    movement_direction VARCHAR(10), -- 'up', 'down', 'neutral'
    movement_significance VARCHAR(20), -- 'minor', 'moderate', 'significant', 'steam'
    movement_time TIMESTAMPTZ,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Normalized game data
CREATE TABLE staging.games (
    id BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    game_date DATE,
    game_time TIME,
    game_datetime TIMESTAMPTZ,
    season INTEGER,
    week INTEGER,
    venue VARCHAR(200),
    weather_conditions JSONB,
    data_quality_score DECIMAL(3,2),
    validation_status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Standardized team data
CREATE TABLE staging.teams (
    id SERIAL PRIMARY KEY,
    team_name_normalized VARCHAR(100) UNIQUE,
    team_abbreviation VARCHAR(10),
    team_city VARCHAR(100),
    division VARCHAR(50),
    league VARCHAR(20),
    alternate_names JSONB, -- Array of alternate spellings/names
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Normalized split data
CREATE TABLE staging.betting_splits (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES staging.games(id),
    sportsbook_name VARCHAR(100),
    bet_type VARCHAR(50),
    public_bet_percentage DECIMAL(5,2),
    public_money_percentage DECIMAL(5,2),
    sharp_bet_percentage DECIMAL(5,2),
    total_bets INTEGER,
    total_handle DECIMAL(15,2),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Processed sharp action indicators
CREATE TABLE staging.sharp_action_signals (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES staging.games(id),
    bet_type VARCHAR(50),
    signal_type VARCHAR(100), -- 'reverse_line_movement', 'steam_move', 'consensus_flip', etc.
    signal_strength DECIMAL(3,2), -- 0.00 to 1.00
    confidence_score DECIMAL(3,2),
    trigger_conditions JSONB,
    detected_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Quality control tables
CREATE TABLE staging.data_quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    record_id BIGINT,
    quality_score DECIMAL(3,2),
    completeness_score DECIMAL(3,2),
    accuracy_score DECIMAL(3,2),
    consistency_score DECIMAL(3,2),
    quality_issues JSONB,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.validation_results (
    id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    source_record_id BIGINT,
    validation_type VARCHAR(100),
    status VARCHAR(20), -- 'passed', 'failed', 'warning'
    error_message TEXT,
    validation_data JSONB,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- CURATED Zone Tables
-- ================================

-- Final Betting Lines (Analysis-Ready)
CREATE TABLE curated.moneylines (
    id BIGSERIAL PRIMARY KEY,
    staging_id BIGINT REFERENCES staging.moneylines(id),
    game_id INTEGER,
    sportsbook_name VARCHAR(100),
    home_odds INTEGER,
    away_odds INTEGER,
    implied_probability_home DECIMAL(5,4),
    implied_probability_away DECIMAL(5,4),
    no_vig_probability_home DECIMAL(5,4),
    no_vig_probability_away DECIMAL(5,4),
    expected_value_home DECIMAL(8,4),
    expected_value_away DECIMAL(8,4),
    market_efficiency_score DECIMAL(3,2),
    sharp_action_indicator BOOLEAN DEFAULT FALSE,
    consensus_deviation DECIMAL(5,2),
    historical_performance JSONB,
    feature_vector JSONB, -- ML features
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.spreads (
    id BIGSERIAL PRIMARY KEY,
    staging_id BIGINT REFERENCES staging.spreads(id),
    game_id INTEGER,
    sportsbook_name VARCHAR(100),
    spread_value DECIMAL(4,1),
    spread_odds INTEGER,
    implied_probability DECIMAL(5,4),
    expected_value DECIMAL(8,4),
    line_movement_trend VARCHAR(20),
    sharp_action_indicator BOOLEAN DEFAULT FALSE,
    reverse_line_movement BOOLEAN DEFAULT FALSE,
    steam_move_detected BOOLEAN DEFAULT FALSE,
    public_fade_opportunity DECIMAL(3,2),
    feature_vector JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.totals (
    id BIGSERIAL PRIMARY KEY,
    staging_id BIGINT REFERENCES staging.totals(id),
    game_id INTEGER,
    sportsbook_name VARCHAR(100),
    total_points DECIMAL(4,1),
    over_odds INTEGER,
    under_odds INTEGER,
    implied_probability_over DECIMAL(5,4),
    implied_probability_under DECIMAL(5,4),
    expected_value_over DECIMAL(8,4),
    expected_value_under DECIMAL(8,4),
    weather_impact_score DECIMAL(3,2),
    pitcher_impact_score DECIMAL(3,2),
    sharp_action_indicator BOOLEAN DEFAULT FALSE,
    feature_vector JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Complete betting lines with all features
CREATE TABLE curated.betting_lines_enhanced (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER,
    bet_type VARCHAR(50),
    sportsbook_name VARCHAR(100),
    primary_line DECIMAL(10,3),
    primary_odds INTEGER,
    market_consensus DECIMAL(10,3),
    sharp_consensus DECIMAL(10,3),
    public_percentage DECIMAL(5,2),
    sharp_percentage DECIMAL(5,2),
    line_movement_pattern JSONB,
    profitability_indicators JSONB,
    risk_metrics JSONB,
    confidence_score DECIMAL(3,2),
    recommendation VARCHAR(20), -- 'bet', 'fade', 'avoid', 'monitor'
    feature_vector JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Analysis-ready data
CREATE TABLE curated.enhanced_games (
    id BIGSERIAL PRIMARY KEY,
    staging_game_id INTEGER REFERENCES staging.games(id),
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    game_datetime TIMESTAMPTZ,
    season INTEGER,
    venue VARCHAR(200),
    weather_features JSONB,
    team_form_features JSONB,
    head_to_head_features JSONB,
    pitcher_features JSONB,
    historical_trends JSONB,
    market_features JSONB,
    sharp_action_summary JSONB,
    public_sentiment JSONB,
    injury_impact_score DECIMAL(3,2),
    motivation_factors JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.betting_analysis (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    analysis_type VARCHAR(100), -- 'sharp_action', 'rlm', 'steam', 'consensus'
    bet_type VARCHAR(50),
    recommendation VARCHAR(20),
    confidence_score DECIMAL(3,2),
    expected_roi DECIMAL(8,4),
    risk_level VARCHAR(20),
    key_factors JSONB,
    supporting_evidence JSONB,
    historical_similar_games JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.movement_analysis (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    bet_type VARCHAR(50),
    movement_pattern VARCHAR(100), -- 'steam', 'rlm', 'drift', 'volatility'
    magnitude DECIMAL(8,4),
    velocity DECIMAL(8,4),
    acceleration DECIMAL(8,4),
    market_impact_score DECIMAL(3,2),
    sharp_money_indicator DECIMAL(3,2),
    public_money_indicator DECIMAL(3,2),
    time_series_features JSONB,
    anomaly_score DECIMAL(3,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ML Features
CREATE TABLE curated.feature_vectors (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    bet_type VARCHAR(50),
    feature_set_version VARCHAR(20),
    features JSONB NOT NULL, -- Complete feature vector for ML models
    target_variable DECIMAL(10,6), -- For supervised learning
    feature_importance JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.prediction_inputs (
    id BIGSERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    model_name VARCHAR(100),
    input_features JSONB,
    prediction_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Strategy and Performance Tables
CREATE TABLE curated.strategy_results (
    id BIGSERIAL PRIMARY KEY,
    strategy_name VARCHAR(100),
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    bet_type VARCHAR(50),
    recommendation VARCHAR(20),
    confidence_score DECIMAL(3,2),
    stake_percentage DECIMAL(5,2),
    expected_return DECIMAL(8,4),
    actual_return DECIMAL(8,4),
    result_status VARCHAR(20), -- 'pending', 'win', 'loss', 'push'
    strategy_parameters JSONB,
    execution_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curated.profitability_metrics (
    id BIGSERIAL PRIMARY KEY,
    strategy_name VARCHAR(100),
    time_period VARCHAR(50), -- 'daily', 'weekly', 'monthly', 'season'
    period_start DATE,
    period_end DATE,
    total_bets INTEGER,
    winning_bets INTEGER,
    losing_bets INTEGER,
    push_bets INTEGER,
    win_rate DECIMAL(5,4),
    total_roi DECIMAL(8,4),
    avg_odds DECIMAL(8,2),
    total_stake DECIMAL(12,2),
    total_return DECIMAL(12,2),
    profit_loss DECIMAL(12,2),
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ================================
-- Indexes for Performance
-- ================================

-- RAW Zone Indexes
CREATE INDEX idx_betting_lines_raw_source_collected ON raw_data.betting_lines_raw(source, collected_at);
CREATE INDEX idx_betting_lines_raw_game_date ON raw_data.betting_lines_raw(game_date);
CREATE INDEX idx_betting_lines_raw_external_id ON raw_data.betting_lines_raw(external_id);

CREATE INDEX idx_moneylines_raw_source_collected ON raw_data.moneylines_raw(source, collected_at);
CREATE INDEX idx_spreads_raw_source_collected ON raw_data.spreads_raw(source, collected_at);
CREATE INDEX idx_totals_raw_source_collected ON raw_data.totals_raw(source, collected_at);

CREATE INDEX idx_collection_log_source_start ON raw_data.collection_log(source, start_time);
CREATE INDEX idx_source_health_source_checked ON raw_data.source_health(source, checked_at);

-- STAGING Zone Indexes
CREATE INDEX idx_staging_moneylines_game_id ON staging.moneylines(game_id);
CREATE INDEX idx_staging_spreads_game_id ON staging.spreads(game_id);
CREATE INDEX idx_staging_totals_game_id ON staging.totals(game_id);
CREATE INDEX idx_staging_games_date ON staging.games(game_date);
CREATE INDEX idx_staging_teams_normalized ON staging.teams(team_name_normalized);

-- CURATED Zone Indexes
CREATE INDEX idx_curated_moneylines_game_id ON curated.moneylines(game_id);
CREATE INDEX idx_curated_spreads_game_id ON curated.spreads(game_id);
CREATE INDEX idx_curated_totals_game_id ON curated.totals(game_id);
CREATE INDEX idx_enhanced_games_datetime ON curated.enhanced_games(game_datetime);
CREATE INDEX idx_feature_vectors_game_bet_type ON curated.feature_vectors(game_id, bet_type);
CREATE INDEX idx_strategy_results_strategy_game ON curated.strategy_results(strategy_name, game_id);

-- ================================
-- Add Pipeline Tracking
-- ================================

CREATE TABLE public.pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    execution_id UUID DEFAULT gen_random_uuid(),
    pipeline_stage VARCHAR(20) NOT NULL, -- 'raw', 'staging', 'curated'
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    records_processed INTEGER DEFAULT 0,
    records_successful INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'running', -- 'running', 'completed', 'failed'
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipeline_log_execution_stage ON public.pipeline_execution_log(execution_id, pipeline_stage);
CREATE INDEX idx_pipeline_log_start_time ON public.pipeline_execution_log(start_time);

-- ================================
-- Grant Permissions
-- ================================

-- Grant usage on schemas
GRANT USAGE ON SCHEMA raw_data TO PUBLIC;
GRANT USAGE ON SCHEMA staging TO PUBLIC;
GRANT USAGE ON SCHEMA curated TO PUBLIC;

-- Grant permissions on tables (adjust based on your user/role setup)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw_data TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA curated TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.pipeline_execution_log TO PUBLIC;

-- Grant permissions on sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw_data TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curated TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE public.pipeline_execution_log_id_seq TO PUBLIC;