-- =============================================================================
-- CONSOLIDATED MIGRATION: Utility and Support Tables
-- =============================================================================
-- Purpose: Consolidated utility tables and support infrastructure
-- Replaces: 008, 015, 016, 017, 018, 019, 020_mappings, 030, 034
-- Date: 2025-08-12 (Consolidation)  
-- Includes: Game mappings, pipeline logs, monitoring, curated tables
-- =============================================================================

-- Ensure required schemas exist
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- GAME ID MAPPINGS AND CROSS-SYSTEM INTEGRATION
-- =============================================================================

-- Game ID Mappings Dimension Table
CREATE TABLE IF NOT EXISTS curated.game_id_mappings (
    id BIGSERIAL PRIMARY KEY,
    
    -- Primary Game Information
    canonical_game_id VARCHAR(255) UNIQUE NOT NULL, -- Our canonical game ID
    game_date DATE NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    
    -- External System IDs
    action_network_game_id VARCHAR(255),
    mlb_stats_api_game_id VARCHAR(50),
    vsin_game_id VARCHAR(255),
    sbd_game_id VARCHAR(255),
    
    -- Game Metadata
    game_start_time TIMESTAMPTZ,
    venue_name VARCHAR(200),
    game_status VARCHAR(50) DEFAULT 'scheduled', -- scheduled, in_progress, final, postponed
    season INTEGER,
    game_type VARCHAR(10) DEFAULT 'R', -- R (regular), P (playoffs), etc.
    
    -- Data Quality and Lineage
    mapping_confidence NUMERIC(4,3) DEFAULT 1.0, -- Confidence in ID mappings
    last_verified TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    verification_source VARCHAR(100),
    
    -- Audit Trail
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_teams_mappings CHECK (home_team != away_team),
    CONSTRAINT valid_confidence CHECK (mapping_confidence >= 0.0 AND mapping_confidence <= 1.0),
    CONSTRAINT valid_game_status CHECK (game_status IN ('scheduled', 'in_progress', 'final', 'postponed', 'cancelled')),
    
    -- Indexes for cross-system lookups
    INDEX idx_game_mappings_action_network (action_network_game_id),
    INDEX idx_game_mappings_mlb_stats (mlb_stats_api_game_id),
    INDEX idx_game_mappings_game_date (game_date),
    INDEX idx_game_mappings_teams (home_team, away_team),
    INDEX idx_game_mappings_start_time (game_start_time),
    INDEX idx_game_mappings_confidence (mapping_confidence),
    INDEX idx_game_mappings_updated (updated_at)
);

-- =============================================================================
-- CURATED ZONE CORE TABLES  
-- =============================================================================

-- Unified Games Table
CREATE TABLE IF NOT EXISTS curated.games (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game Identification
    canonical_game_id VARCHAR(255) UNIQUE NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    action_network_game_id VARCHAR(255),
    
    -- Game Details
    game_date DATE NOT NULL,
    game_start_time TIMESTAMPTZ,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    
    -- Game Outcome
    game_status VARCHAR(50) DEFAULT 'scheduled',
    home_score INTEGER,
    away_score INTEGER,
    winning_team VARCHAR(10),
    
    -- Game Context
    venue_name VARCHAR(200),
    season INTEGER,
    game_type VARCHAR(10) DEFAULT 'R',
    weather_conditions JSONB,
    
    -- Data Quality
    data_completeness_score NUMERIC(4,3) DEFAULT 1.0,
    last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_teams_games CHECK (home_team != away_team),
    CONSTRAINT valid_scores CHECK ((home_score IS NULL AND away_score IS NULL) OR (home_score IS NOT NULL AND away_score IS NOT NULL)),
    CONSTRAINT valid_winner CHECK (winning_team IS NULL OR winning_team IN (home_team, away_team)),
    
    INDEX idx_games_canonical_id (canonical_game_id),
    INDEX idx_games_mlb_stats_id (mlb_stats_api_game_id),
    INDEX idx_games_action_network_id (action_network_game_id),
    INDEX idx_games_date (game_date),
    INDEX idx_games_teams (home_team, away_team),
    INDEX idx_games_status (game_status),
    INDEX idx_games_start_time (game_start_time)
);

-- Unified Betting Lines Table
CREATE TABLE IF NOT EXISTS curated.betting_lines_unified (
    id BIGSERIAL PRIMARY KEY,
    
    -- Game Reference
    game_id VARCHAR(255) NOT NULL,
    canonical_game_id VARCHAR(255),
    
    -- Sportsbook Information
    sportsbook VARCHAR(100) NOT NULL,
    
    -- Market Information
    market_type VARCHAR(20) NOT NULL, -- moneyline, spread, total
    
    -- Betting Lines
    home_ml INTEGER,
    away_ml INTEGER,
    home_spread NUMERIC(4,2),
    away_spread NUMERIC(4,2),
    total_line NUMERIC(4,2),
    over_odds INTEGER,
    under_odds INTEGER,
    
    -- Betting Percentages
    home_bet_percentage NUMERIC(5,2),
    away_bet_percentage NUMERIC(5,2),
    home_money_percentage NUMERIC(5,2),
    away_money_percentage NUMERIC(5,2),
    over_bet_percentage NUMERIC(5,2),
    under_bet_percentage NUMERIC(5,2),
    over_money_percentage NUMERIC(5,2),
    under_money_percentage NUMERIC(5,2),
    
    -- Data Quality and Metadata
    data_quality VARCHAR(20) DEFAULT 'HIGH', -- HIGH, MEDIUM, LOW
    source_reliability_score NUMERIC(4,3) DEFAULT 1.0,
    odds_timestamp TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_market_type_unified CHECK (market_type IN ('moneyline', 'spread', 'total')),
    CONSTRAINT valid_data_quality CHECK (data_quality IN ('HIGH', 'MEDIUM', 'LOW')),
    CONSTRAINT valid_percentages CHECK (
        (home_bet_percentage IS NULL OR (home_bet_percentage >= 0 AND home_bet_percentage <= 100)) AND
        (away_bet_percentage IS NULL OR (away_bet_percentage >= 0 AND away_bet_percentage <= 100))
    ),
    
    INDEX idx_betting_lines_unified_game_id (game_id),
    INDEX idx_betting_lines_unified_canonical_id (canonical_game_id),
    INDEX idx_betting_lines_unified_sportsbook (sportsbook),
    INDEX idx_betting_lines_unified_market_type (market_type),
    INDEX idx_betting_lines_unified_quality (data_quality),
    INDEX idx_betting_lines_unified_timestamp (odds_timestamp)
);

-- =============================================================================
-- PIPELINE AND MONITORING INFRASTRUCTURE
-- =============================================================================

-- Pipeline Execution Log
CREATE TABLE IF NOT EXISTS public.pipeline_execution_log (
    execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(200) NOT NULL,
    zone VARCHAR(50), -- raw, staging, curated, analytics
    mode VARCHAR(50), -- full, incremental, test
    source VARCHAR(100), -- all, action_network, vsin, etc.
    
    -- Execution Details
    status VARCHAR(20) DEFAULT 'running', -- running, completed, failed, cancelled
    records_processed INTEGER DEFAULT 0,
    records_successful INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    
    -- Timing Information
    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    
    -- Error Information
    error_message TEXT,
    error_details JSONB,
    
    -- Metadata
    execution_context JSONB, -- Additional execution parameters and context
    
    CONSTRAINT valid_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT valid_records CHECK (records_processed >= 0 AND records_successful >= 0 AND records_failed >= 0),
    CONSTRAINT valid_record_sum CHECK (records_successful + records_failed <= records_processed),
    
    INDEX idx_pipeline_log_pipeline_name (pipeline_name),
    INDEX idx_pipeline_log_zone (zone),
    INDEX idx_pipeline_log_status (status),
    INDEX idx_pipeline_log_started_at (started_at),
    INDEX idx_pipeline_log_duration (duration_seconds)
);

-- Data Quality Metrics
CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    
    -- Metric Details
    table_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL, -- completeness, accuracy, consistency, timeliness
    metric_value NUMERIC(10,4) NOT NULL,
    
    -- Quality Thresholds
    warning_threshold NUMERIC(10,4),
    critical_threshold NUMERIC(10,4),
    metric_status VARCHAR(20) DEFAULT 'ok', -- ok, warning, critical
    
    -- Measurement Details
    measurement_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    sample_size INTEGER,
    measurement_method VARCHAR(100),
    
    -- Context
    pipeline_execution_id UUID REFERENCES public.pipeline_execution_log(execution_id),
    additional_metadata JSONB,
    
    CONSTRAINT valid_metric_status CHECK (metric_status IN ('ok', 'warning', 'critical')),
    
    INDEX idx_data_quality_table (schema_name, table_name),
    INDEX idx_data_quality_metric (metric_name),
    INDEX idx_data_quality_timestamp (measurement_timestamp),
    INDEX idx_data_quality_status (metric_status),
    INDEX idx_data_quality_pipeline (pipeline_execution_id)
);

-- System Health Checks
CREATE TABLE IF NOT EXISTS monitoring.system_health_checks (
    id BIGSERIAL PRIMARY KEY,
    
    -- Health Check Details
    component_name VARCHAR(100) NOT NULL, -- database, api, collector, etc.
    check_name VARCHAR(100) NOT NULL, -- connection, performance, availability
    check_status VARCHAR(20) NOT NULL, -- healthy, degraded, unhealthy
    
    -- Check Results
    response_time_ms INTEGER,
    check_message TEXT,
    check_details JSONB,
    
    -- Timing
    check_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    next_check_due TIMESTAMPTZ,
    
    CONSTRAINT valid_check_status CHECK (check_status IN ('healthy', 'degraded', 'unhealthy')),
    
    INDEX idx_health_checks_component (component_name),
    INDEX idx_health_checks_status (check_status),
    INDEX idx_health_checks_timestamp (check_timestamp),
    INDEX idx_health_checks_next_due (next_check_due)
);

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Function to resolve game ID across systems
CREATE OR REPLACE FUNCTION curated.resolve_game_id(
    p_action_network_id VARCHAR DEFAULT NULL,
    p_mlb_stats_id VARCHAR DEFAULT NULL,
    p_home_team VARCHAR DEFAULT NULL,
    p_away_team VARCHAR DEFAULT NULL,
    p_game_date DATE DEFAULT NULL
)
RETURNS VARCHAR AS $$
DECLARE
    resolved_id VARCHAR;
BEGIN
    -- Try exact ID match first
    IF p_action_network_id IS NOT NULL THEN
        SELECT canonical_game_id INTO resolved_id
        FROM curated.game_id_mappings
        WHERE action_network_game_id = p_action_network_id;
        
        IF resolved_id IS NOT NULL THEN
            RETURN resolved_id;
        END IF;
    END IF;
    
    IF p_mlb_stats_id IS NOT NULL THEN
        SELECT canonical_game_id INTO resolved_id
        FROM curated.game_id_mappings
        WHERE mlb_stats_api_game_id = p_mlb_stats_id;
        
        IF resolved_id IS NOT NULL THEN
            RETURN resolved_id;
        END IF;
    END IF;
    
    -- Try team and date match
    IF p_home_team IS NOT NULL AND p_away_team IS NOT NULL AND p_game_date IS NOT NULL THEN
        SELECT canonical_game_id INTO resolved_id
        FROM curated.game_id_mappings
        WHERE home_team = p_home_team 
        AND away_team = p_away_team 
        AND game_date = p_game_date;
        
        IF resolved_id IS NOT NULL THEN
            RETURN resolved_id;
        END IF;
    END IF;
    
    -- No match found
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to validate betting percentage data
CREATE OR REPLACE FUNCTION staging.validate_betting_percentage_data()
RETURNS TABLE(
    validation_check VARCHAR,
    passed_count INTEGER,
    failed_count INTEGER,
    failure_rate NUMERIC
) AS $$
BEGIN
    -- Check if betting percentages sum to approximately 100%
    RETURN QUERY
    WITH percentage_validation AS (
        SELECT 
            CASE 
                WHEN ABS((COALESCE(home_bet_percentage, 0) + COALESCE(away_bet_percentage, 0)) - 100) <= 5 
                THEN 'passed' 
                ELSE 'failed' 
            END as bet_percentage_check,
            CASE 
                WHEN ABS((COALESCE(home_money_percentage, 0) + COALESCE(away_money_percentage, 0)) - 100) <= 5 
                THEN 'passed' 
                ELSE 'failed' 
            END as money_percentage_check
        FROM curated.betting_lines_unified
        WHERE home_bet_percentage IS NOT NULL OR away_bet_percentage IS NOT NULL
    )
    SELECT 
        'bet_percentage_totals'::VARCHAR,
        SUM(CASE WHEN bet_percentage_check = 'passed' THEN 1 ELSE 0 END)::INTEGER,
        SUM(CASE WHEN bet_percentage_check = 'failed' THEN 1 ELSE 0 END)::INTEGER,
        ROUND(
            SUM(CASE WHEN bet_percentage_check = 'failed' THEN 1 ELSE 0 END)::NUMERIC / 
            COUNT(*)::NUMERIC * 100, 2
        )
    FROM percentage_validation
    
    UNION ALL
    
    SELECT 
        'money_percentage_totals'::VARCHAR,
        SUM(CASE WHEN money_percentage_check = 'passed' THEN 1 ELSE 0 END)::INTEGER,
        SUM(CASE WHEN money_percentage_check = 'failed' THEN 1 ELSE 0 END)::INTEGER,
        ROUND(
            SUM(CASE WHEN money_percentage_check = 'failed' THEN 1 ELSE 0 END)::NUMERIC / 
            COUNT(*)::NUMERIC * 100, 2
        )
    FROM percentage_validation;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON TABLE curated.game_id_mappings IS 'Cross-system game ID mappings for unified game identification';
COMMENT ON TABLE curated.games IS 'Unified games table with complete game information and outcomes';
COMMENT ON TABLE curated.betting_lines_unified IS 'Unified betting lines across all sportsbooks and market types';

COMMENT ON TABLE public.pipeline_execution_log IS 'Comprehensive pipeline execution tracking for all data processing operations';
COMMENT ON TABLE monitoring.data_quality_metrics IS 'Data quality monitoring with configurable thresholds and alerting';
COMMENT ON TABLE monitoring.system_health_checks IS 'System component health monitoring and status tracking';

COMMENT ON FUNCTION curated.resolve_game_id IS 'Cross-system game ID resolution using multiple identification strategies';
COMMENT ON FUNCTION staging.validate_betting_percentage_data IS 'Validation function for betting percentage data integrity checks';