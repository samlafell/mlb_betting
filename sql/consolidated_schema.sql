-- MLB Sharp Betting System - Consolidated Schema Structure
-- This schema consolidation reduces complexity from 9+ schemas to 4 logical schemas
-- Created: $(date '+%Y-%m-%d')
-- Migration from: public, splits, mlb_betting, backtesting, tracking, timing_analysis, clean, action schemas

-- ==============================================================================
-- 1. RAW_DATA SCHEMA - All external data ingestion and raw storage
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS raw_data;

-- Raw HTML/API responses from SportsbookReview
CREATE TABLE IF NOT EXISTS raw_data.sbr_raw_html (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL,
    response_html TEXT NOT NULL,
    response_headers JSONB,
    scrape_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status_code INTEGER NOT NULL DEFAULT 200,
    page_type VARCHAR(50), -- 'moneyline', 'spreads', 'totals'
    date_scraped DATE NOT NULL,
    processing_status VARCHAR(20) DEFAULT 'pending' CHECK (processing_status IN ('pending', 'processed', 'failed', 'skipped')),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Parsed games from SportsbookReview (intermediate processing)
CREATE TABLE IF NOT EXISTS raw_data.sbr_parsed_games (
    id SERIAL PRIMARY KEY,
    raw_html_id INTEGER REFERENCES raw_data.sbr_raw_html(id),
    game_date DATE NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_time VARCHAR(20),
    parsed_data JSONB NOT NULL, -- Raw parsed betting data
    parsing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    validation_errors JSONB DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Raw MLB betting splits data (from VSIN, SBD, and other sources)
CREATE TABLE IF NOT EXISTS raw_data.raw_mlb_betting_splits (
    id BIGINT PRIMARY KEY,
    game_id TEXT,
    home_team TEXT,
    away_team TEXT,
    game_datetime TIMESTAMP,
    split_type TEXT, -- 'Spread', 'Total', 'Moneyline'
    last_updated TIMESTAMP,
    source TEXT, -- 'SBD' for SportsBettingDime, 'VSIN' for other sources
    book TEXT, -- Specific sportsbook for VSIN (like 'DK', 'Circa'), NULL for SBD source
    
    -- Long format: home_or_over represents home team (Spread/Moneyline) or over (Total)
    home_or_over_bets INTEGER,
    home_or_over_bets_percentage DOUBLE PRECISION,
    home_or_over_stake_percentage DOUBLE PRECISION,
    
    -- Long format: away_or_under represents away team (Spread/Moneyline) or under (Total)
    away_or_under_bets INTEGER,
    away_or_under_bets_percentage DOUBLE PRECISION,
    away_or_under_stake_percentage DOUBLE PRECISION,
    
    -- Split-specific value (spread line, total line, or moneyline odds)
    split_value TEXT,
    
    -- Metadata
    sharp_action TEXT, -- Detected sharp action direction (if any)
    outcome TEXT, -- Outcome of the bet (win/loss/push)
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Raw MLB API responses
CREATE TABLE IF NOT EXISTS raw_data.mlb_api_responses (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(200) NOT NULL,
    request_params JSONB,
    response_data JSONB NOT NULL,
    response_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    cache_expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Raw Odds API responses  
CREATE TABLE IF NOT EXISTS raw_data.odds_api_responses (
    id SERIAL PRIMARY KEY,
    sport VARCHAR(20) NOT NULL,
    endpoint VARCHAR(200) NOT NULL,
    response_data JSONB NOT NULL,
    response_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    api_calls_used INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- VSIN raw data
CREATE TABLE IF NOT EXISTS raw_data.vsin_raw_data (
    id SERIAL PRIMARY KEY,
    source_url VARCHAR(500),
    raw_content TEXT NOT NULL,
    content_type VARCHAR(50) DEFAULT 'html',
    scrape_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processing_status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Parsing status and metadata
CREATE TABLE IF NOT EXISTS raw_data.parsing_status_logs (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(50) NOT NULL,
    source_record_id INTEGER NOT NULL,
    parser_type VARCHAR(50) NOT NULL,
    parsing_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    parsing_completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed', 'failed')),
    error_message TEXT,
    records_processed INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for raw_data schema
CREATE INDEX IF NOT EXISTS idx_raw_data_sbr_html_timestamp ON raw_data.sbr_raw_html(scrape_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_sbr_html_status ON raw_data.sbr_raw_html(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_data_sbr_parsed_date ON raw_data.sbr_parsed_games(game_date);
CREATE INDEX IF NOT EXISTS idx_raw_data_splits_game_datetime ON raw_data.raw_mlb_betting_splits(game_datetime);
CREATE INDEX IF NOT EXISTS idx_raw_data_splits_source ON raw_data.raw_mlb_betting_splits(source, book);
CREATE INDEX IF NOT EXISTS idx_raw_data_mlb_api_endpoint ON raw_data.mlb_api_responses(endpoint, response_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_parsing_status ON raw_data.parsing_status_logs(source_table, status);

-- ==============================================================================
-- 2. CORE_BETTING SCHEMA - Clean, processed betting data and core business entities
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS core_betting;

-- Unified games table with all ID systems and enriched data
CREATE TABLE IF NOT EXISTS core_betting.games (
    id SERIAL PRIMARY KEY,
    
    -- Multiple ID columns for cross-system integration
    sportsbookreview_game_id VARCHAR(100) UNIQUE,
    mlb_stats_api_game_id VARCHAR(20),
    action_network_game_id INTEGER,
    vsin_game_id VARCHAR(50),
    
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

-- Game outcomes for backtesting and analysis
CREATE TABLE IF NOT EXISTS core_betting.game_outcomes (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    
    -- Betting outcomes
    home_win BOOLEAN NOT NULL,
    over BOOLEAN NOT NULL,
    home_cover_spread BOOLEAN DEFAULT NULL,
    
    -- Additional context
    total_line DOUBLE PRECISION DEFAULT NULL,
    home_spread_line DOUBLE PRECISION DEFAULT NULL,
    game_date TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(game_id)
);

-- Teams reference data
CREATE TABLE IF NOT EXISTS core_betting.teams (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL UNIQUE, -- Action Network ID
    full_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(50) NOT NULL,
    short_name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    abbr VARCHAR(5) NOT NULL,
    logo VARCHAR(500),
    primary_color CHAR(6),
    secondary_color CHAR(6),
    conference_type VARCHAR(2) CHECK (conference_type IN ('AL', 'NL')),
    division_type VARCHAR(10) CHECK (division_type IN ('EAST', 'CENTRAL', 'WEST')),
    url_slug VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Sportsbooks reference data
CREATE TABLE IF NOT EXISTS core_betting.sportsbooks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100),
    abbreviation VARCHAR(10),
    is_active BOOLEAN DEFAULT true,
    supports_live_betting BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Betting lines - Moneyline
CREATE TABLE IF NOT EXISTS core_betting.betting_lines_moneyline (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    sportsbook_id INTEGER REFERENCES core_betting.sportsbooks(id),
    sportsbook VARCHAR(50) NOT NULL, -- Keep for backward compatibility
    
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

-- Betting lines - Spreads
CREATE TABLE IF NOT EXISTS core_betting.betting_lines_spreads (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    sportsbook_id INTEGER REFERENCES core_betting.sportsbooks(id),
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

-- Betting lines - Totals
CREATE TABLE IF NOT EXISTS core_betting.betting_lines_totals (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    sportsbook_id INTEGER REFERENCES core_betting.sportsbooks(id),
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
    total_score INTEGER,
    
    -- Metadata
    source VARCHAR(20) DEFAULT 'SPORTSBOOKREVIEW',
    data_quality VARCHAR(20) DEFAULT 'MEDIUM',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Line movements tracking
CREATE TABLE IF NOT EXISTS core_betting.line_movements (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    bet_type VARCHAR(20) NOT NULL CHECK (bet_type IN ('moneyline', 'spread', 'total')),
    sportsbook VARCHAR(50) NOT NULL,
    movement_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    old_value VARCHAR(20),
    new_value VARCHAR(20),
    movement_direction VARCHAR(10) CHECK (movement_direction IN ('up', 'down', 'none')),
    movement_size DECIMAL(5,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Steam moves detection
CREATE TABLE IF NOT EXISTS core_betting.steam_moves (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    bet_type VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    movement_pattern VARCHAR(100),
    confidence_score DECIMAL(3,2),
    sportsbooks_affected JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Betting splits aggregated data
CREATE TABLE IF NOT EXISTS core_betting.betting_splits (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    split_type VARCHAR(20) NOT NULL CHECK (split_type IN ('moneyline', 'spread', 'total')),
    source VARCHAR(50) NOT NULL,
    book VARCHAR(50),
    
    -- Public vs Sharp breakdown
    home_or_over_bets INTEGER,
    home_or_over_bets_percentage DECIMAL(5,2),
    home_or_over_stake_percentage DECIMAL(5,2),
    away_or_under_bets INTEGER,
    away_or_under_bets_percentage DECIMAL(5,2),
    away_or_under_stake_percentage DECIMAL(5,2),
    
    -- Sharp action detection
    sharp_action VARCHAR(20),
    sharp_confidence DECIMAL(3,2),
    
    -- Metadata
    split_value VARCHAR(20),
    last_updated TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sharp action indicators
CREATE TABLE IF NOT EXISTS core_betting.sharp_action_indicators (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    bet_type VARCHAR(20) NOT NULL,
    indicator_type VARCHAR(50) NOT NULL, -- 'reverse_line_movement', 'steam_move', 'money_disparity'
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    confidence DECIMAL(3,2) NOT NULL,
    supporting_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for core_betting schema
CREATE INDEX IF NOT EXISTS idx_core_betting_games_date ON core_betting.games(game_date);
CREATE INDEX IF NOT EXISTS idx_core_betting_games_teams ON core_betting.games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_core_betting_games_mlb_id ON core_betting.games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_core_betting_outcomes_game ON core_betting.game_outcomes(game_id);
CREATE INDEX IF NOT EXISTS idx_core_betting_ml_game_timestamp ON core_betting.betting_lines_moneyline(game_id, odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_core_betting_spreads_game_timestamp ON core_betting.betting_lines_spreads(game_id, odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_core_betting_totals_game_timestamp ON core_betting.betting_lines_totals(game_id, odds_timestamp);
CREATE INDEX IF NOT EXISTS idx_core_betting_splits_game_type ON core_betting.betting_splits(game_id, split_type);
CREATE INDEX IF NOT EXISTS idx_core_betting_sharp_indicators ON core_betting.sharp_action_indicators(game_id, indicator_type);

-- ==============================================================================
-- 3. ANALYTICS SCHEMA - Derived analytics, signals, and strategy outputs
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Strategy signals and recommendations
CREATE TABLE IF NOT EXISTS analytics.strategy_signals (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    strategy_name VARCHAR(100) NOT NULL,
    signal_type VARCHAR(50) NOT NULL, -- 'sharp_action', 'line_movement', 'splits_disparity'
    signal_strength DECIMAL(5,3) NOT NULL CHECK (signal_strength >= 0 AND signal_strength <= 1),
    confidence_level VARCHAR(20) NOT NULL CHECK (confidence_level IN ('LOW', 'MODERATE', 'HIGH')),
    recommended_action VARCHAR(100),
    signal_data JSONB,
    generated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy recommendations (clean consolidated data)
CREATE TABLE IF NOT EXISTS analytics.betting_recommendations (
    id VARCHAR PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    home_team VARCHAR(5) NOT NULL,
    away_team VARCHAR(5) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    source VARCHAR(50) NOT NULL,
    book VARCHAR(50),
    recommended_side VARCHAR(20) NOT NULL,
    line_value VARCHAR(20),
    confidence_score DECIMAL(5,3) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    differential DECIMAL(8,2) NOT NULL,
    stake_percentage DECIMAL(5,2),
    bet_percentage DECIMAL(5,2),
    minutes_before_game INTEGER NOT NULL,
    signal_strength VARCHAR(20) NOT NULL,
    consensus_boost DECIMAL(5,3) DEFAULT 0.0,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Timing analysis results
CREATE TABLE IF NOT EXISTS analytics.timing_analysis_results (
    id SERIAL PRIMARY KEY,
    analysis_name VARCHAR(200) NOT NULL,
    timing_bucket VARCHAR(10) NOT NULL CHECK (timing_bucket IN ('0-2h', '2-6h', '6-24h', '24h+')),
    source VARCHAR(20),
    book VARCHAR(50),
    split_type VARCHAR(20),
    strategy_name VARCHAR(100),
    
    -- Analysis period
    analysis_start_date DATE NOT NULL,
    analysis_end_date DATE NOT NULL,
    
    -- Performance metrics
    total_bets INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    pushes INTEGER NOT NULL DEFAULT 0,
    win_rate DECIMAL(6,3),
    roi_percentage DECIMAL(8,3),
    total_profit_loss DECIMAL(12,2),
    
    -- Confidence and metadata
    confidence_level VARCHAR(20),
    sample_size_adequate BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Cross-market analysis
CREATE TABLE IF NOT EXISTS analytics.cross_market_analysis (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    analysis_type VARCHAR(50) NOT NULL, -- 'correlation', 'arbitrage', 'steam_detection'
    markets_compared JSONB NOT NULL, -- Array of market types compared
    analysis_results JSONB NOT NULL,
    opportunity_score DECIMAL(5,3),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Confidence scores tracking
CREATE TABLE IF NOT EXISTS analytics.confidence_scores (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES core_betting.games(id),
    strategy_name VARCHAR(100) NOT NULL,
    confidence_type VARCHAR(50) NOT NULL, -- 'overall', 'timing', 'historical'
    score DECIMAL(5,3) NOT NULL CHECK (score >= 0 AND score <= 1),
    factors JSONB, -- Contributing factors to confidence
    calculated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ROI calculations and tracking
CREATE TABLE IF NOT EXISTS analytics.roi_calculations (
    id SERIAL PRIMARY KEY,
    calculation_period_start DATE NOT NULL,
    calculation_period_end DATE NOT NULL,
    strategy_name VARCHAR(100),
    bet_type VARCHAR(20),
    total_bets INTEGER NOT NULL,
    total_wagered DECIMAL(12,2) NOT NULL,
    total_profit_loss DECIMAL(12,2) NOT NULL,
    roi_percentage DECIMAL(8,3) NOT NULL,
    win_rate DECIMAL(6,3),
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    calculated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Performance metrics aggregated
CREATE TABLE IF NOT EXISTS analytics.performance_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_category VARCHAR(50) NOT NULL, -- 'strategy', 'timing', 'market', 'overall'
    period_type VARCHAR(20) NOT NULL CHECK (period_type IN ('daily', 'weekly', 'monthly', 'seasonal')),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    metric_value DECIMAL(12,4) NOT NULL,
    context_data JSONB,
    calculated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for analytics schema
CREATE INDEX IF NOT EXISTS idx_analytics_signals_game_strategy ON analytics.strategy_signals(game_id, strategy_name);
CREATE INDEX IF NOT EXISTS idx_analytics_recommendations_game ON analytics.betting_recommendations(game_id);
CREATE INDEX IF NOT EXISTS idx_analytics_timing_bucket ON analytics.timing_analysis_results(timing_bucket, analysis_start_date);
CREATE INDEX IF NOT EXISTS idx_analytics_cross_market_game ON analytics.cross_market_analysis(game_id, analysis_type);
CREATE INDEX IF NOT EXISTS idx_analytics_confidence_game ON analytics.confidence_scores(game_id, strategy_name);
CREATE INDEX IF NOT EXISTS idx_analytics_roi_period ON analytics.roi_calculations(calculation_period_start, calculation_period_end);

-- ==============================================================================
-- 4. OPERATIONAL SCHEMA - System operations, monitoring, and validation
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS operational;

-- Strategy performance tracking (from backtesting)
CREATE TABLE IF NOT EXISTS operational.strategy_performance (
    id VARCHAR PRIMARY KEY,
    backtest_date DATE NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    source_book_type VARCHAR(50),
    split_type VARCHAR(50),
    
    -- Performance Metrics
    total_bets INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    win_rate DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    roi_per_100 DECIMAL(10,2) NOT NULL DEFAULT 0.0,
    
    -- Statistical Metrics
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    confidence_interval_lower DECIMAL(8,4),
    confidence_interval_upper DECIMAL(8,4),
    sample_size_adequate BOOLEAN,
    statistical_significance BOOLEAN,
    p_value DECIMAL(8,6),
    
    -- Trend Analysis
    seven_day_win_rate DECIMAL(8,4),
    thirty_day_win_rate DECIMAL(8,4),
    trend_direction VARCHAR(20), -- 'improving', 'declining', 'stable'
    
    -- Risk Metrics
    consecutive_losses INTEGER DEFAULT 0,
    volatility DECIMAL(8,4) DEFAULT 0.0,
    kelly_criterion DECIMAL(8,4) DEFAULT 0.0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT strategy_performance_unique_key UNIQUE (strategy_name, source_book_type, split_type, backtest_date)
);

-- Pre-game recommendations tracking
CREATE TABLE IF NOT EXISTS operational.pre_game_recommendations (
    recommendation_id VARCHAR PRIMARY KEY,
    game_pk VARCHAR NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    recommendation TEXT NOT NULL,
    bet_type VARCHAR NOT NULL CHECK (bet_type IN ('moneyline', 'spread', 'total')),
    confidence_level VARCHAR NOT NULL CHECK (confidence_level IN ('LOW', 'MODERATE', 'HIGH')),
    signal_source VARCHAR NOT NULL,
    signal_strength DECIMAL(8,4) DEFAULT 0.0,
    recommended_at TIMESTAMP WITH TIME ZONE NOT NULL,
    email_sent BOOLEAN DEFAULT TRUE,
    
    -- Outcome tracking (filled in after game completion)
    game_completed BOOLEAN DEFAULT FALSE,
    bet_won BOOLEAN DEFAULT NULL,
    actual_outcome TEXT DEFAULT NULL,
    profit_loss DECIMAL(12,2) DEFAULT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Strategy source tracking
    strategy_source_id VARCHAR(200),
    auto_integrated BOOLEAN DEFAULT false
);

-- System health monitoring
CREATE TABLE IF NOT EXISTS operational.system_health_checks (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_category VARCHAR(50) NOT NULL, -- 'database', 'api', 'scraping', 'processing'
    status VARCHAR(20) NOT NULL CHECK (status IN ('healthy', 'warning', 'critical', 'unknown')),
    response_time_ms INTEGER,
    error_message TEXT,
    check_details JSONB,
    checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Data quality metrics and validation results
CREATE TABLE IF NOT EXISTS operational.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    quality_check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'timeliness'
    total_records INTEGER NOT NULL,
    passed_records INTEGER NOT NULL,
    failed_records INTEGER NOT NULL,
    quality_score DECIMAL(5,3) NOT NULL, -- 0.0 to 1.0
    check_details JSONB,
    checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Pipeline execution logs
CREATE TABLE IF NOT EXISTS operational.pipeline_execution_logs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('started', 'running', 'completed', 'failed', 'cancelled')),
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    execution_details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Alert configurations and tracking
CREATE TABLE IF NOT EXISTS operational.alert_configurations (
    id SERIAL PRIMARY KEY,
    alert_name VARCHAR(100) NOT NULL UNIQUE,
    alert_type VARCHAR(50) NOT NULL, -- 'threshold', 'anomaly', 'system', 'data_quality'
    trigger_conditions JSONB NOT NULL,
    notification_channels JSONB NOT NULL, -- email, slack, etc.
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Alert history
CREATE TABLE IF NOT EXISTS operational.alert_history (
    id SERIAL PRIMARY KEY,
    alert_config_id INTEGER NOT NULL REFERENCES operational.alert_configurations(id),
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    alert_level VARCHAR(20) NOT NULL CHECK (alert_level IN ('info', 'warning', 'error', 'critical')),
    message TEXT NOT NULL,
    trigger_data JSONB,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(100),
    resolution_notes TEXT
);

-- Recommendation tracking for ROI validation
CREATE TABLE IF NOT EXISTS operational.recommendation_tracking (
    id SERIAL PRIMARY KEY,
    recommendation_id VARCHAR NOT NULL,
    source_table VARCHAR(50) NOT NULL,
    tracking_started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    game_completed_at TIMESTAMP WITH TIME ZONE,
    outcome_determined_at TIMESTAMP WITH TIME ZONE,
    tracking_status VARCHAR(20) DEFAULT 'active' CHECK (tracking_status IN ('active', 'completed', 'expired')),
    final_result VARCHAR(20), -- 'win', 'loss', 'push'
    profit_loss DECIMAL(12,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Backtesting configurations and history
CREATE TABLE IF NOT EXISTS operational.backtesting_configurations (
    id SERIAL PRIMARY KEY,
    config_name VARCHAR(100) NOT NULL,
    strategy_parameters JSONB NOT NULL,
    data_range_start DATE NOT NULL,
    data_range_end DATE NOT NULL,
    created_by VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy orchestrator update triggers
CREATE TABLE IF NOT EXISTS operational.orchestrator_update_triggers (
    id SERIAL PRIMARY KEY,
    trigger_type VARCHAR(50) NOT NULL, -- 'threshold_change', 'performance_alert', 'manual'
    strategy_name VARCHAR(100) NOT NULL,
    trigger_data JSONB,
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processed', 'failed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for operational schema
CREATE INDEX IF NOT EXISTS idx_operational_strategy_performance_name ON operational.strategy_performance(strategy_name, backtest_date DESC);
CREATE INDEX IF NOT EXISTS idx_operational_recommendations_game ON operational.pre_game_recommendations(game_pk, game_datetime);
CREATE INDEX IF NOT EXISTS idx_operational_health_checks_category ON operational.system_health_checks(check_category, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_operational_quality_metrics_table ON operational.data_quality_metrics(table_name, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_operational_pipeline_logs_name ON operational.pipeline_execution_logs(pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_operational_alerts_triggered ON operational.alert_history(triggered_at DESC, alert_level);

-- ==============================================================================
-- VIEWS AND FUNCTIONS FOR COMPATIBILITY AND CONVENIENCE
-- ==============================================================================

-- Compatibility view for current betting performance summary
CREATE OR REPLACE VIEW operational.betting_performance_summary AS
SELECT 
    DATE(game_datetime) as game_date,
    COUNT(*) as total_recommendations,
    COUNT(CASE WHEN game_completed THEN 1 END) as completed_games,
    COUNT(CASE WHEN bet_won = true THEN 1 END) as wins,
    COUNT(CASE WHEN bet_won = false THEN 1 END) as losses,
    COUNT(CASE WHEN bet_won IS NULL AND game_completed THEN 1 END) as pushes,
    SUM(COALESCE(profit_loss, 0)) as total_profit_loss,
    AVG(COALESCE(profit_loss, 0)) as avg_profit_loss,
    CASE 
        WHEN COUNT(CASE WHEN game_completed THEN 1 END) > 0 
        THEN COUNT(CASE WHEN bet_won = true THEN 1 END)::DECIMAL / COUNT(CASE WHEN game_completed THEN 1 END) * 100
        ELSE 0 
    END as win_percentage
FROM operational.pre_game_recommendations
WHERE game_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(game_datetime)
ORDER BY game_date DESC;

-- Current active strategies view
CREATE OR REPLACE VIEW operational.active_strategies AS
SELECT 
    strategy_name,
    source_book_type,
    split_type,
    COUNT(*) as total_backtests,
    AVG(win_rate) as avg_win_rate,
    AVG(roi_per_100) as avg_roi_per_100,
    SUM(total_bets) as total_bets_all_time,
    AVG(sharpe_ratio) as avg_sharpe_ratio,
    MAX(updated_at) as last_analyzed,
    CASE 
        WHEN AVG(win_rate) > 0.65 THEN 'EXCELLENT'
        WHEN AVG(win_rate) > 0.58 THEN 'GOOD' 
        WHEN AVG(win_rate) > 0.52 THEN 'PROFITABLE'
        ELSE 'UNPROFITABLE'
    END as performance_grade
FROM operational.strategy_performance 
WHERE backtest_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY strategy_name, source_book_type, split_type
HAVING SUM(total_bets) >= 5
ORDER BY avg_roi_per_100 DESC;

-- Function to update recommendation outcomes
CREATE OR REPLACE FUNCTION operational.update_recommendation_outcomes()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    rec RECORD;
    outcome RECORD;
BEGIN
    -- Loop through incomplete recommendations
    FOR rec IN 
        SELECT r.recommendation_id, r.game_pk, r.home_team, r.away_team, 
               r.recommendation, r.bet_type, r.game_datetime
        FROM operational.pre_game_recommendations r
        WHERE r.game_completed = FALSE
          AND r.game_datetime < NOW() - INTERVAL '3 hours'
    LOOP
        -- Try to find game outcome
        SELECT g.home_score, g.away_score, g.home_win, g.over, g.home_cover_spread
        INTO outcome
        FROM core_betting.game_outcomes g
        JOIN core_betting.games gm ON g.game_id = gm.id
        WHERE gm.mlb_stats_api_game_id = rec.game_pk
           OR (gm.home_team = rec.home_team 
               AND gm.away_team = rec.away_team 
               AND DATE(gm.game_date) = DATE(rec.game_datetime));
        
        IF FOUND THEN
            -- Update the recommendation with outcome
            UPDATE operational.pre_game_recommendations
            SET 
                game_completed = TRUE,
                -- Determine bet_won based on recommendation logic
                bet_won = CASE 
                    WHEN rec.bet_type = 'moneyline' AND rec.recommendation LIKE '%' || rec.home_team || '%' THEN outcome.home_win
                    WHEN rec.bet_type = 'moneyline' AND rec.recommendation LIKE '%' || rec.away_team || '%' THEN NOT outcome.home_win
                    WHEN rec.bet_type = 'total' AND rec.recommendation LIKE '%OVER%' THEN outcome.over
                    WHEN rec.bet_type = 'total' AND rec.recommendation LIKE '%UNDER%' THEN NOT outcome.over
                    WHEN rec.bet_type = 'spread' AND outcome.home_cover_spread IS NOT NULL THEN
                        CASE 
                            WHEN rec.recommendation LIKE '%' || rec.home_team || '%' THEN outcome.home_cover_spread
                            WHEN rec.recommendation LIKE '%' || rec.away_team || '%' THEN NOT outcome.home_cover_spread
                            ELSE NULL
                        END
                    ELSE NULL
                END,
                actual_outcome = format('%s won %s-%s', 
                    CASE WHEN outcome.home_win THEN rec.home_team ELSE rec.away_team END,
                    outcome.home_score, outcome.away_score),
                updated_at = NOW()
            WHERE recommendation_id = rec.recommendation_id;
            
            updated_count := updated_count + 1;
        END IF;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- SCHEMA PERMISSIONS (Example structure)
-- ==============================================================================

-- Grant permissions (uncomment and modify as needed)
-- GRANT USAGE ON SCHEMA raw_data TO data_collectors;
-- GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw_data TO data_collectors;

-- GRANT USAGE ON SCHEMA core_betting TO betting_processors, analytics_users;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core_betting TO betting_processors;
-- GRANT SELECT ON ALL TABLES IN SCHEMA core_betting TO analytics_users;

-- GRANT USAGE ON SCHEMA analytics TO strategy_processors, analytics_users;
-- GRANT ALL ON ALL TABLES IN SCHEMA analytics TO strategy_processors;
-- GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_users;

-- GRANT USAGE ON SCHEMA operational TO system_administrators, monitoring_users;
-- GRANT ALL ON ALL TABLES IN SCHEMA operational TO system_administrators;
-- GRANT SELECT ON ALL TABLES IN SCHEMA operational TO monitoring_users;

-- Comments for documentation
COMMENT ON SCHEMA raw_data IS 'All external data ingestion and raw storage - HTML responses, API data, parsing status';
COMMENT ON SCHEMA core_betting IS 'Clean, processed betting data and core business entities - games, teams, betting lines, outcomes';
COMMENT ON SCHEMA analytics IS 'Derived analytics, signals, and strategy outputs - recommendations, timing analysis, performance metrics';
COMMENT ON SCHEMA operational IS 'System operations, monitoring, and validation - backtesting, health checks, alerts, tracking'; 