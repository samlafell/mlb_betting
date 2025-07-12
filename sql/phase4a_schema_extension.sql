-- ==================================================================================
-- MLB Sharp Betting System - Phase 4A: Schema Extension for Remaining Data
-- ==================================================================================
-- 
-- This script extends the existing 4 consolidated schemas with additional tables
-- needed to accommodate data from the remaining 6 legacy schemas:
-- - public, action, splits, tracking, validation, backtesting
--
-- PHASE 4A: NON-DESTRUCTIVE - Creates new tables only, no data migration yet
-- 
-- Target Schema Structure:
-- ✅ raw_data     - External data ingestion and raw storage  
-- ✅ core_betting - Clean betting data and core business entities
-- ✅ analytics    - Derived analytics and strategy outputs
-- ✅ operational  - System operations, monitoring, and validation
-- ==================================================================================

-- Enable logging
DO $$ 
BEGIN
    RAISE NOTICE 'Starting Phase 4A: Schema Extension for Remaining Data Consolidation';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- EXTEND RAW_DATA SCHEMA - External Data Sources
-- ==================================================================================

-- SportsbookReview raw HTML data (from public.sbr_raw_html)
CREATE TABLE IF NOT EXISTS raw_data.sbr_raw_html (
    id SERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    html_content TEXT NOT NULL,
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- SportsbookReview parsed game data (from public.sbr_parsed_games)  
CREATE TABLE IF NOT EXISTS raw_data.sbr_parsed_games (
    id SERIAL PRIMARY KEY,
    raw_html_id INTEGER NOT NULL,
    game_data JSONB NOT NULL,
    parsed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key to raw HTML
    CONSTRAINT fk_sbr_parsed_games_raw_html 
        FOREIGN KEY (raw_html_id) REFERENCES raw_data.sbr_raw_html(id)
);

-- ==================================================================================
-- EXTEND CORE_BETTING SCHEMA - Business Entities
-- ==================================================================================

-- Enhanced game outcomes table (from public.game_outcomes)
-- Note: This may already exist, so we'll use CREATE IF NOT EXISTS
CREATE TABLE IF NOT EXISTS core_betting.game_outcomes (
    game_id TEXT PRIMARY KEY,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITHOUT TIME ZONE,
    home_pitcher TEXT,
    away_pitcher TEXT,
    weather_conditions TEXT,
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction TEXT,
    game_status VARCHAR(50) DEFAULT 'completed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Action Network game mappings (from action.fact_games)
CREATE TABLE IF NOT EXISTS core_betting.action_network_games (
    id SERIAL PRIMARY KEY,
    id_action INTEGER NOT NULL UNIQUE,
    id_mlbstatsapi INTEGER,
    dim_home_team_actionid INTEGER NOT NULL,
    dim_away_team_actionid INTEGER NOT NULL,
    game_date DATE,
    game_time TIME,
    game_datetime TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Index for lookups
    CONSTRAINT idx_action_network_games_action_id UNIQUE (id_action)
);

-- Enhanced Action Network game data with team details (from action.games_with_teams)
CREATE TABLE IF NOT EXISTS core_betting.action_network_games_enhanced (
    id SERIAL PRIMARY KEY,
    id_action INTEGER,
    id_mlbstatsapi INTEGER,
    game_date DATE,
    game_time TIME,
    game_datetime TIMESTAMP WITHOUT TIME ZONE,
    home_team_name TEXT,
    away_team_name TEXT,
    home_team_short TEXT,
    away_team_short TEXT,
    home_team_actionid INTEGER,
    away_team_actionid INTEGER,
    venue_name TEXT,
    venue_city TEXT,
    venue_state TEXT,
    weather_conditions TEXT,
    temperature INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key to main action network games
    CONSTRAINT fk_action_enhanced_main_game 
        FOREIGN KEY (id_action) REFERENCES core_betting.action_network_games(id_action)
);

-- Supplementary games data (from splits.games)
CREATE TABLE IF NOT EXISTS core_betting.supplementary_games (
    id VARCHAR(255) PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL,
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    game_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    source VARCHAR(100) DEFAULT 'splits',
    status VARCHAR(50) DEFAULT 'active',
    home_pitcher VARCHAR(255),
    away_pitcher VARCHAR(255),
    venue VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==================================================================================
-- EXTEND ANALYTICS SCHEMA - Derived Analytics
-- ==================================================================================

-- Sharp action indicators (from splits.sharp_actions)
CREATE TABLE IF NOT EXISTS analytics.sharp_action_indicators (
    id VARCHAR(255) PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL,
    split_type VARCHAR(100) NOT NULL,
    direction VARCHAR(50) NOT NULL,
    overall_confidence VARCHAR(50) NOT NULL,
    signal_strength DECIMAL(5,2),
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source VARCHAR(100) DEFAULT 'splits_analysis',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==================================================================================
-- EXTEND OPERATIONAL SCHEMA - System Operations & Monitoring
-- ==================================================================================

-- Migration tracking logs (from public.migration_log)
CREATE TABLE IF NOT EXISTS operational.migration_log (
    id SERIAL PRIMARY KEY,
    migration_phase VARCHAR(20) NOT NULL,
    table_source VARCHAR(100) NOT NULL,
    table_destination VARCHAR(100) NOT NULL,
    records_migrated INTEGER DEFAULT 0,
    migration_started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    migration_completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started' CHECK (status IN ('started', 'completed', 'failed')),
    error_message TEXT,
    notes TEXT
);

-- Phase 2B migration logs (from public.migration_log_phase2b)
CREATE TABLE IF NOT EXISTS operational.migration_log_phase2b (
    id SERIAL PRIMARY KEY,
    migration_step VARCHAR(100) NOT NULL,
    table_name VARCHAR(100),
    records_processed INTEGER,
    records_migrated INTEGER,
    execution_time_seconds DECIMAL(10,3),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started',
    error_message TEXT,
    notes TEXT
);

-- Active strategy management (from tracking.active_high_roi_strategies)
CREATE TABLE IF NOT EXISTS operational.active_strategies (
    strategy_id VARCHAR(255) PRIMARY KEY,
    source_book_type VARCHAR(100) NOT NULL,
    split_type VARCHAR(100) NOT NULL,
    strategy_variant VARCHAR(100) NOT NULL,
    total_bets INTEGER NOT NULL DEFAULT 0,
    roi_per_100_unit DECIMAL(10,2),
    win_rate DECIMAL(5,2),
    confidence_level VARCHAR(50),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy configurations (consolidating from tracking.active_strategy_configs and backtesting.strategy_configurations)
CREATE TABLE IF NOT EXISTS operational.strategy_configurations (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(255) NOT NULL,
    strategy_name VARCHAR(255),
    source_book_type VARCHAR(100),
    split_type VARCHAR(100),
    configuration JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    win_rate DECIMAL(5,2),
    roi_per_100_unit DECIMAL(10,2),
    confidence_threshold DECIMAL(5,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Unique constraint on strategy_id
    CONSTRAINT unique_strategy_config UNIQUE (strategy_id)
);

-- Strategy configuration history (from backtesting.strategy_config_history)
CREATE TABLE IF NOT EXISTS operational.strategy_config_history (
    id SERIAL PRIMARY KEY,
    configuration_version VARCHAR(255) NOT NULL,
    config_data JSONB,
    snapshot_reason VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    strategy_count INTEGER,
    enabled_strategy_count INTEGER,
    total_roi DECIMAL(10,2),
    avg_win_rate DECIMAL(5,2),
    performance_score DECIMAL(10,2)
);

-- Strategy configuration cache (from backtesting.strategy_config_cache)
CREATE TABLE IF NOT EXISTS operational.strategy_config_cache (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    strategy_type VARCHAR(100) NOT NULL,
    config_json TEXT NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    cache_key VARCHAR(255) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy integration log (from tracking.strategy_integration_log)
CREATE TABLE IF NOT EXISTS operational.strategy_integration_log (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    roi_per_100_unit DECIMAL(10,2),
    total_bets INTEGER,
    win_rate DECIMAL(5,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Pre-game recommendations (from tracking.pre_game_recommendations)
CREATE TABLE IF NOT EXISTS operational.pre_game_recommendations (
    recommendation_id VARCHAR(255) PRIMARY KEY,
    game_pk INTEGER NOT NULL,
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    game_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    bet_type VARCHAR(100) NOT NULL,
    recommendation VARCHAR(100) NOT NULL,
    confidence_level VARCHAR(50),
    roi_projection DECIMAL(10,2),
    strategy_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy validation records (from validation.strategy_records)
CREATE TABLE IF NOT EXISTS operational.strategy_validation_records (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255) NOT NULL,
    validation_date DATE NOT NULL,
    roi_per_100 DECIMAL(10,2),
    win_rate DECIMAL(5,2),
    total_bets INTEGER,
    validation_status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Threshold configurations (from backtesting.threshold_configurations)
CREATE TABLE IF NOT EXISTS operational.threshold_configurations (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    strategy_type VARCHAR(100) NOT NULL,
    high_confidence_threshold DECIMAL(5,2),
    moderate_confidence_threshold DECIMAL(5,2),
    low_confidence_threshold DECIMAL(5,2),
    roi_threshold DECIMAL(10,2),
    win_rate_threshold DECIMAL(5,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orchestrator update triggers (from backtesting.orchestrator_update_triggers) 
-- Note: This may already exist, using CREATE IF NOT EXISTS
CREATE TABLE IF NOT EXISTS operational.orchestrator_update_triggers (
    id SERIAL PRIMARY KEY,
    trigger_type VARCHAR(100) NOT NULL,
    trigger_reason TEXT,
    previous_configuration_version VARCHAR(255),
    configuration_version VARCHAR(255),
    triggered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Recent configuration changes (from backtesting.recent_config_changes)
CREATE TABLE IF NOT EXISTS operational.recent_config_changes (
    id SERIAL PRIMARY KEY,
    configuration_version VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    snapshot_reason VARCHAR(255),
    total_strategies INTEGER,
    enabled_strategies INTEGER,
    disabled_strategies INTEGER,
    avg_roi DECIMAL(10,2),
    total_roi DECIMAL(10,2),
    change_summary TEXT
);

-- Strategy alerts (from backtesting.strategy_alerts)
CREATE TABLE IF NOT EXISTS operational.strategy_alerts (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255) NOT NULL,
    alert_level VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    current_roi DECIMAL(10,2),
    threshold_roi DECIMAL(10,2),
    current_win_rate DECIMAL(5,2),
    threshold_win_rate DECIMAL(5,2),
    alert_triggered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    alert_resolved_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy lifecycle events (from backtesting.strategy_lifecycle_events)
CREATE TABLE IF NOT EXISTS operational.strategy_lifecycle_events (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_reason TEXT,
    previous_status VARCHAR(50),
    new_status VARCHAR(50),
    metadata JSONB,
    event_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Alignment analysis (from backtesting.alignment_analysis)
CREATE TABLE IF NOT EXISTS operational.alignment_analysis (
    id SERIAL PRIMARY KEY,
    alignment_date TIMESTAMP WITH TIME ZONE,
    backtesting_results JSONB,
    live_results JSONB,
    alignment_score DECIMAL(5,2),
    variance_analysis JSONB,
    recommendations TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Threshold recommendations (from backtesting.threshold_recommendations)
CREATE TABLE IF NOT EXISTS operational.threshold_recommendations (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255) NOT NULL,
    recommended_threshold DECIMAL(5,2) NOT NULL,
    confidence_level VARCHAR(50) NOT NULL,
    justification TEXT,
    current_performance DECIMAL(5,2),
    projected_improvement DECIMAL(5,2),
    recommendation_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Standardization audit log (from backtesting.standardization_audit_log)
CREATE TABLE IF NOT EXISTS operational.standardization_audit_log (
    id SERIAL PRIMARY KEY,
    standardization_date TIMESTAMP WITH TIME ZONE,
    original_strategy_name TEXT,
    new_strategy_name TEXT,
    original_source_book_type TEXT,
    new_source_book_type TEXT,
    changes_made JSONB,
    audit_notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy performance backup (from backtesting.strategy_performance_backup)
CREATE TABLE IF NOT EXISTS operational.strategy_performance_backup (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255),
    source_book_type VARCHAR(100),
    split_type VARCHAR(100),
    backtest_date DATE,
    roi_per_100_unit DECIMAL(10,2),
    win_rate DECIMAL(5,2),
    total_bets INTEGER,
    backup_reason VARCHAR(255),
    backup_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==================================================================================
-- CREATE INDEXES FOR PERFORMANCE
-- ==================================================================================

-- Raw data indexes
CREATE INDEX IF NOT EXISTS idx_sbr_raw_html_scraped_at ON raw_data.sbr_raw_html(scraped_at);
CREATE INDEX IF NOT EXISTS idx_sbr_raw_html_status ON raw_data.sbr_raw_html(status);
CREATE INDEX IF NOT EXISTS idx_sbr_parsed_games_parsed_at ON raw_data.sbr_parsed_games(parsed_at);
CREATE INDEX IF NOT EXISTS idx_sbr_parsed_games_status ON raw_data.sbr_parsed_games(status);

-- Core betting indexes
CREATE INDEX IF NOT EXISTS idx_game_outcomes_game_date ON core_betting.game_outcomes(game_date);
CREATE INDEX IF NOT EXISTS idx_game_outcomes_teams ON core_betting.game_outcomes(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_action_network_games_date ON core_betting.action_network_games(game_date);
CREATE INDEX IF NOT EXISTS idx_supplementary_games_datetime ON core_betting.supplementary_games(game_datetime);
CREATE INDEX IF NOT EXISTS idx_supplementary_games_teams ON core_betting.supplementary_games(home_team, away_team);

-- Analytics indexes
CREATE INDEX IF NOT EXISTS idx_sharp_action_game_id ON analytics.sharp_action_indicators(game_id);
CREATE INDEX IF NOT EXISTS idx_sharp_action_detected_at ON analytics.sharp_action_indicators(detected_at);

-- Operational indexes
CREATE INDEX IF NOT EXISTS idx_active_strategies_roi ON operational.active_strategies(roi_per_100_unit);
CREATE INDEX IF NOT EXISTS idx_active_strategies_updated ON operational.active_strategies(last_updated);
CREATE INDEX IF NOT EXISTS idx_strategy_configs_enabled ON operational.strategy_configurations(enabled);
CREATE INDEX IF NOT EXISTS idx_strategy_integration_log_timestamp ON operational.strategy_integration_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_datetime ON operational.pre_game_recommendations(game_datetime);
CREATE INDEX IF NOT EXISTS idx_strategy_alerts_status ON operational.strategy_alerts(status);
CREATE INDEX IF NOT EXISTS idx_strategy_lifecycle_events_timestamp ON operational.strategy_lifecycle_events(event_timestamp);

-- ==================================================================================
-- COMPLETION LOG
-- ==================================================================================

DO $$ 
BEGIN
    RAISE NOTICE 'Phase 4A Schema Extension completed successfully!';
    RAISE NOTICE 'Extended schemas: raw_data, core_betting, analytics, operational';
    RAISE NOTICE 'New tables created: 27 tables total';
    RAISE NOTICE 'Ready for Phase 4B: Data Migration';
    RAISE NOTICE 'Completion timestamp: %', NOW();
END $$; 