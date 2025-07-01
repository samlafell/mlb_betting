CREATE SCHEMA IF NOT EXISTS splits;

CREATE TABLE IF NOT EXISTS splits.raw_mlb_betting_splits (
    id BIGINT,
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
    home_or_over_bets_percentage DOUBLE,
    home_or_over_stake_percentage DOUBLE,
    
    -- Long format: away_or_under represents away team (Spread/Moneyline) or under (Total)
    away_or_under_bets INTEGER,
    away_or_under_bets_percentage DOUBLE,
    away_or_under_stake_percentage DOUBLE,
    
    -- Split-specific value (spread line, total line, or moneyline odds)
    split_value TEXT,
    
    -- Metadata
    sharp_action TEXT, -- Detected sharp action direction (if any)
    outcome TEXT, -- Outcome of the bet (win/loss/push)
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Backtesting Schema for Adaptive Configuration System
CREATE SCHEMA IF NOT EXISTS backtesting;

-- Strategy Performance Tracking
CREATE TABLE IF NOT EXISTS backtesting.strategy_performance (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    source_book_type VARCHAR(50),
    split_type VARCHAR(50),
    backtest_date DATE NOT NULL,
    win_rate DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    roi_per_100 DECIMAL(10,2) NOT NULL DEFAULT 0.0,
    total_bets INTEGER NOT NULL DEFAULT 0,
    total_profit_loss DECIMAL(12,2) NOT NULL DEFAULT 0.0,
    sharpe_ratio DECIMAL(8,4) DEFAULT NULL,
    max_drawdown DECIMAL(8,4) DEFAULT NULL,
    kelly_criterion DECIMAL(8,4) DEFAULT NULL,
    confidence_level VARCHAR(20) NOT NULL DEFAULT 'LOW',
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    wins INTEGER,
    CONSTRAINT strategy_performance_unique_key UNIQUE (strategy_name, source_book_type, split_type, backtest_date)
);

-- Indexes for performance queries
CREATE INDEX IF NOT EXISTS idx_strategy_performance_name_date 
ON backtesting.strategy_performance(strategy_name, backtest_date DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_performance_updated 
ON backtesting.strategy_performance(last_updated DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_performance_roi 
ON backtesting.strategy_performance(roi_per_100 DESC);

-- Threshold Recommendations
CREATE TABLE IF NOT EXISTS backtesting.threshold_recommendations (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    recommended_threshold DECIMAL(6,2) NOT NULL,
    confidence_level VARCHAR(20) NOT NULL,
    justification TEXT,
    requires_human_approval BOOLEAN NOT NULL DEFAULT true,
    approved_by VARCHAR(100) DEFAULT NULL,
    approved_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for threshold queries
CREATE INDEX IF NOT EXISTS idx_threshold_recommendations_strategy 
ON backtesting.threshold_recommendations(strategy_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_threshold_recommendations_active 
ON backtesting.threshold_recommendations(is_active, requires_human_approval);

-- Strategy Configuration Cache (for performance)
CREATE TABLE IF NOT EXISTS backtesting.strategy_config_cache (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    strategy_type VARCHAR(50) NOT NULL,
    config_json TEXT NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (NOW() + INTERVAL '15 minutes'),
    UNIQUE(source, strategy_type)
);

-- Index for config cache
CREATE INDEX IF NOT EXISTS idx_strategy_config_cache_lookup 
ON backtesting.strategy_config_cache(source, strategy_type, expires_at);

-- Historical Performance View for Quick Analysis
CREATE OR REPLACE VIEW backtesting.strategy_performance_summary AS
SELECT 
    strategy_name,
    source_book_type,
    split_type,
    COUNT(*) as total_backtests,
    AVG(win_rate) as avg_win_rate,
    AVG(roi_per_100) as avg_roi_per_100,
    SUM(total_bets) as total_bets_all_time,
    AVG(sharpe_ratio) as avg_sharpe_ratio,
    AVG(max_drawdown) as avg_max_drawdown,
    MAX(last_updated) as last_analyzed,
    -- Performance grade
    CASE 
        WHEN AVG(win_rate) > 0.65 THEN 'EXCELLENT'
        WHEN AVG(win_rate) > 0.58 THEN 'GOOD' 
        WHEN AVG(win_rate) > 0.52 THEN 'PROFITABLE'
        ELSE 'UNPROFITABLE'
    END as performance_grade,
    -- Activity status
    CASE 
        WHEN MAX(last_updated) > NOW() - INTERVAL '7 days' THEN 'ACTIVE'
        WHEN MAX(last_updated) > NOW() - INTERVAL '30 days' THEN 'INACTIVE'
        ELSE 'STALE'
    END as activity_status
FROM backtesting.strategy_performance 
WHERE backtest_date >= CURRENT_DATE - INTERVAL '90 days'  -- Last 90 days
GROUP BY strategy_name, source_book_type, split_type
HAVING SUM(total_bets) >= 5  -- Minimum sample size
ORDER BY avg_roi_per_100 DESC;

-- Current Active Strategies View
CREATE OR REPLACE VIEW backtesting.active_strategies AS
SELECT 
    s.*,
    t.recommended_threshold,
    t.confidence_level as threshold_confidence,
    t.requires_human_approval
FROM backtesting.strategy_performance_summary s
LEFT JOIN (
    SELECT DISTINCT ON (strategy_name) 
        strategy_name,
        recommended_threshold,
        confidence_level,
        requires_human_approval
    FROM backtesting.threshold_recommendations 
    WHERE is_active = true
    ORDER BY strategy_name, created_at DESC
) t ON s.strategy_name = t.strategy_name
WHERE s.performance_grade IN ('EXCELLENT', 'GOOD', 'PROFITABLE')
  AND s.activity_status = 'ACTIVE'
ORDER BY s.avg_roi_per_100 DESC;

-- Function to cleanup old performance data
CREATE OR REPLACE FUNCTION backtesting.cleanup_old_performance_data()
RETURNS void AS $$
BEGIN
    -- Delete performance data older than 6 months
    DELETE FROM backtesting.strategy_performance 
    WHERE backtest_date < CURRENT_DATE - INTERVAL '6 months';
    
    -- Delete old threshold recommendations (keep last 30 days)
    DELETE FROM backtesting.threshold_recommendations 
    WHERE created_at < NOW() - INTERVAL '30 days'
      AND requires_human_approval = false;
      
    -- Clear expired config cache
    DELETE FROM backtesting.strategy_config_cache 
    WHERE expires_at < NOW();
    
    -- Log cleanup
    RAISE NOTICE 'Cleaned up old backtesting data at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Auto-approve high-confidence threshold recommendations
CREATE OR REPLACE FUNCTION backtesting.auto_approve_thresholds()
RETURNS void AS $$
BEGIN
    UPDATE backtesting.threshold_recommendations
    SET 
        approved_by = 'SYSTEM_AUTO_APPROVAL',
        approved_at = NOW(),
        requires_human_approval = false
    WHERE requires_human_approval = true
      AND confidence_level = 'HIGH'
      AND created_at > NOW() - INTERVAL '24 hours'
      AND approved_at IS NULL;
      
    -- Log auto-approvals
    GET DIAGNOSTICS var_count = ROW_COUNT;
    IF var_count > 0 THEN
        RAISE NOTICE 'Auto-approved % high-confidence threshold recommendations', var_count;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON SCHEMA backtesting IS 'Adaptive betting strategy configuration and performance tracking';
COMMENT ON TABLE backtesting.strategy_performance IS 'Historical performance metrics for all betting strategies';
COMMENT ON TABLE backtesting.threshold_recommendations IS 'AI-generated threshold recommendations based on performance';
COMMENT ON TABLE backtesting.strategy_config_cache IS 'Performance cache for strategy configurations';
COMMENT ON VIEW backtesting.strategy_performance_summary IS 'Aggregated performance metrics by strategy';
COMMENT ON VIEW backtesting.active_strategies IS 'Currently active and profitable strategies with thresholds'; 