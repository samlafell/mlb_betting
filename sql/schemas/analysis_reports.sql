-- Analysis Reports Schema
-- Stores pipeline analysis results instead of JSON files
-- Compatible with three-tier pipeline architecture (RAW → STAGING → CURATED)

-- Main analysis reports table (CURATED zone)
CREATE TABLE IF NOT EXISTS curated.analysis_reports (
    id SERIAL PRIMARY KEY,
    
    -- Report metadata
    report_type VARCHAR(50) NOT NULL CHECK (report_type IN ('pipeline', 'movement_analysis', 'opportunities')),
    analysis_timestamp TIMESTAMPTZ NOT NULL,
    pipeline_run_id VARCHAR(100), -- Links related reports from same pipeline run
    
    -- Summary metrics
    total_games_analyzed INTEGER DEFAULT 0,
    games_with_rlm INTEGER DEFAULT 0,
    games_with_steam_moves INTEGER DEFAULT 0,
    games_with_arbitrage INTEGER DEFAULT 0,
    total_movements INTEGER DEFAULT 0,
    
    -- Execution metadata
    execution_time_seconds DECIMAL(10,3),
    data_source VARCHAR(50) DEFAULT 'action_network',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- RLM (Reverse Line Movement) opportunities (CURATED zone)
CREATE TABLE IF NOT EXISTS curated.rlm_opportunities (
    id SERIAL PRIMARY KEY,
    
    -- Links to analysis report
    analysis_report_id INTEGER NOT NULL REFERENCES curated.analysis_reports(id) ON DELETE CASCADE,
    
    -- Game information
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    action_network_game_id INTEGER NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_datetime TIMESTAMPTZ NOT NULL,
    
    -- Market details
    sportsbook_id INTEGER REFERENCES core_betting.sportsbooks(id),
    action_network_book_id INTEGER NOT NULL,
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    side VARCHAR(10) NOT NULL CHECK (side IN ('home', 'away', 'over', 'under')),
    
    -- RLM metrics
    line_direction VARCHAR(10) NOT NULL CHECK (line_direction IN ('up', 'down', 'stable')),
    public_betting_direction VARCHAR(10) NOT NULL CHECK (public_betting_direction IN ('up', 'down', 'stable')),
    public_percentage DECIMAL(5,2), -- Public betting percentage
    line_movement_amount DECIMAL(10,2),
    rlm_strength VARCHAR(10) NOT NULL CHECK (rlm_strength IN ('weak', 'moderate', 'strong')),
    
    -- Timing
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Steam moves (cross-book consensus moves) (CURATED zone)
CREATE TABLE IF NOT EXISTS curated.steam_moves (
    id SERIAL PRIMARY KEY,
    
    -- Links to analysis report
    analysis_report_id INTEGER NOT NULL REFERENCES curated.analysis_reports(id) ON DELETE CASCADE,
    
    -- Game information
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    action_network_game_id INTEGER NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_datetime TIMESTAMPTZ NOT NULL,
    
    -- Movement details
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    consensus_direction VARCHAR(10) NOT NULL CHECK (consensus_direction IN ('up', 'down', 'stable')),
    consensus_strength VARCHAR(10) NOT NULL CHECK (consensus_strength IN ('weak', 'moderate', 'strong')),
    participating_books TEXT[], -- Array of sportsbook IDs
    divergent_books TEXT[], -- Books that moved opposite
    average_movement DECIMAL(10,2),
    
    -- Timing
    movement_timestamp TIMESTAMPTZ NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Arbitrage opportunities (CURATED zone)
CREATE TABLE IF NOT EXISTS curated.arbitrage_opportunities (
    id SERIAL PRIMARY KEY,
    
    -- Links to analysis report
    analysis_report_id INTEGER NOT NULL REFERENCES curated.analysis_reports(id) ON DELETE CASCADE,
    
    -- Game information
    game_id INTEGER REFERENCES curated.enhanced_games(id),
    action_network_game_id INTEGER NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_datetime TIMESTAMPTZ NOT NULL,
    
    -- Arbitrage details
    market_type VARCHAR(20) NOT NULL CHECK (market_type IN ('moneyline', 'spread', 'total')),
    book_a_id INTEGER REFERENCES core_betting.sportsbooks(id),
    book_b_id INTEGER REFERENCES core_betting.sportsbooks(id),
    book_a_odds INTEGER NOT NULL,
    book_b_odds INTEGER NOT NULL,
    odds_discrepancy INTEGER NOT NULL, -- Absolute difference
    potential_profit_percentage DECIMAL(5,2),
    
    -- Timing
    detected_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ, -- Estimated expiration
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Pipeline execution logs (CURATED zone)
CREATE TABLE IF NOT EXISTS curated.pipeline_runs (
    id SERIAL PRIMARY KEY,
    
    -- Execution metadata
    run_id VARCHAR(100) UNIQUE NOT NULL,
    command_type VARCHAR(50) NOT NULL, -- 'pipeline', 'collect', 'analyze'
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    
    -- Configuration
    date_target VARCHAR(20), -- 'today', 'tomorrow', specific date
    max_games INTEGER,
    skip_history BOOLEAN DEFAULT FALSE,
    analyze_only BOOLEAN DEFAULT FALSE,
    
    -- Execution results
    games_extracted INTEGER DEFAULT 0,
    games_analyzed INTEGER DEFAULT 0,
    total_movements INTEGER DEFAULT 0,
    total_opportunities INTEGER DEFAULT 0,
    
    -- Performance
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    execution_time_seconds DECIMAL(10,3),
    
    -- Error handling
    error_message TEXT,
    warnings TEXT[],
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_analysis_reports_timestamp 
ON curated.analysis_reports(analysis_timestamp);

CREATE INDEX IF NOT EXISTS idx_analysis_reports_pipeline_run 
ON curated.analysis_reports(pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_rlm_opportunities_game_date 
ON curated.rlm_opportunities(game_datetime);

CREATE INDEX IF NOT EXISTS idx_rlm_opportunities_strength 
ON curated.rlm_opportunities(rlm_strength);

CREATE INDEX IF NOT EXISTS idx_steam_moves_game_date 
ON curated.steam_moves(game_datetime);

CREATE INDEX IF NOT EXISTS idx_steam_moves_strength 
ON curated.steam_moves(consensus_strength);

CREATE INDEX IF NOT EXISTS idx_arbitrage_opportunities_game_date 
ON curated.arbitrage_opportunities(game_datetime);

CREATE INDEX IF NOT EXISTS idx_arbitrage_opportunities_profit 
ON curated.arbitrage_opportunities(potential_profit_percentage);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status 
ON curated.pipeline_runs(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time 
ON curated.pipeline_runs(start_time);

-- Views for easy querying
CREATE OR REPLACE VIEW curated.latest_opportunities AS
SELECT 
    'rlm' as opportunity_type,
    r.home_team,
    r.away_team,
    r.game_datetime,
    r.market_type,
    r.rlm_strength as strength,
    r.detected_at,
    ar.analysis_timestamp,
    NULL::DECIMAL(5,2) as profit_potential
FROM curated.rlm_opportunities r
JOIN curated.analysis_reports ar ON r.analysis_report_id = ar.id
WHERE ar.analysis_timestamp > NOW() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'steam' as opportunity_type,
    s.home_team,
    s.away_team,
    s.game_datetime,
    s.market_type,
    s.consensus_strength as strength,
    s.detected_at,
    ar.analysis_timestamp,
    NULL::DECIMAL(5,2) as profit_potential
FROM curated.steam_moves s
JOIN curated.analysis_reports ar ON s.analysis_report_id = ar.id
WHERE ar.analysis_timestamp > NOW() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'arbitrage' as opportunity_type,
    a.home_team,
    a.away_team,
    a.game_datetime,
    a.market_type,
    'profitable' as strength,
    a.detected_at,
    ar.analysis_timestamp,
    a.potential_profit_percentage as profit_potential
FROM curated.arbitrage_opportunities a
JOIN curated.analysis_reports ar ON a.analysis_report_id = ar.id
WHERE ar.analysis_timestamp > NOW() - INTERVAL '24 hours'
ORDER BY detected_at DESC;

-- Daily opportunities summary view
CREATE OR REPLACE VIEW curated.daily_opportunities_summary AS
SELECT 
    DATE(ar.analysis_timestamp) as analysis_date,
    ar.report_type,
    COUNT(r.id) as rlm_count,
    COUNT(s.id) as steam_count,
    COUNT(a.id) as arbitrage_count,
    ar.total_games_analyzed,
    AVG(ar.execution_time_seconds) as avg_execution_time
FROM curated.analysis_reports ar
LEFT JOIN curated.rlm_opportunities r ON ar.id = r.analysis_report_id
LEFT JOIN curated.steam_moves s ON ar.id = s.analysis_report_id
LEFT JOIN curated.arbitrage_opportunities a ON ar.id = a.analysis_report_id
WHERE ar.analysis_timestamp > NOW() - INTERVAL '7 days'
GROUP BY DATE(ar.analysis_timestamp), ar.report_type, ar.total_games_analyzed
ORDER BY analysis_date DESC;