-- Strategy Configuration History Schema
-- Tracks configuration changes over time for debugging and auditing

-- Strategy configuration snapshots table
CREATE TABLE IF NOT EXISTS backtesting.strategy_config_history (
    id SERIAL PRIMARY KEY,
    config_version VARCHAR(50) NOT NULL,
    config_data JSONB NOT NULL,
    snapshot_reason VARCHAR(100) DEFAULT 'SCHEDULED_UPDATE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Configuration metadata
    total_strategies INTEGER NOT NULL,
    enabled_strategies INTEGER NOT NULL,
    avg_roi DECIMAL(8,2),
    avg_win_rate DECIMAL(6,4),
    
    -- Performance indicators
    performance_summary JSONB,
    strategy_breakdown JSONB,
    
    UNIQUE(config_version)
);

-- Strategy lifecycle events table
CREATE TABLE IF NOT EXISTS backtesting.strategy_lifecycle_events (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL, -- PROMOTED, DEMOTED, AUTO_DISABLED, PROBATION, etc.
    event_reason VARCHAR(200),
    previous_status VARCHAR(50),
    new_status VARCHAR(50),
    roi_at_event DECIMAL(8,2),
    win_rate_at_event DECIMAL(6,4),
    sample_size_at_event INTEGER,
    configuration_version VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for strategy lifecycle events
CREATE INDEX IF NOT EXISTS idx_strategy_lifecycle 
ON backtesting.strategy_lifecycle_events(strategy_name, created_at);

CREATE INDEX IF NOT EXISTS idx_event_type 
ON backtesting.strategy_lifecycle_events(event_type);

CREATE INDEX IF NOT EXISTS idx_config_version_lifecycle 
ON backtesting.strategy_lifecycle_events(configuration_version);

-- Note: betting_signals table modifications would go here if the table existed
-- ALTER TABLE betting_signals ADD COLUMN IF NOT EXISTS configuration_version VARCHAR(50);
-- ALTER TABLE betting_signals ADD COLUMN IF NOT EXISTS strategy_config_snapshot JSONB;

-- Configuration performance tracking
CREATE TABLE IF NOT EXISTS backtesting.configuration_performance (
    id SERIAL PRIMARY KEY,
    configuration_version VARCHAR(50) NOT NULL,
    signals_generated INTEGER DEFAULT 0,
    recommendations_made INTEGER DEFAULT 0,
    bets_placed INTEGER DEFAULT 0,
    bets_won INTEGER DEFAULT 0,
    total_profit_loss DECIMAL(10,2) DEFAULT 0.0,
    avg_confidence_score DECIMAL(4,2),
    period_start TIMESTAMP WITH TIME ZONE,
    period_end TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for configuration performance
CREATE INDEX IF NOT EXISTS idx_config_performance 
ON backtesting.configuration_performance(configuration_version);

CREATE INDEX IF NOT EXISTS idx_period_performance 
ON backtesting.configuration_performance(period_start, period_end);

-- Strategy alerts table
CREATE TABLE IF NOT EXISTS backtesting.strategy_alerts (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    alert_level VARCHAR(20) NOT NULL, -- INFO, WARNING, CRITICAL
    message TEXT NOT NULL,
    current_roi DECIMAL(8,2),
    current_win_rate DECIMAL(6,4),
    sample_size INTEGER,
    performance_trend VARCHAR(20),
    recommended_action TEXT,
    configuration_version VARCHAR(50),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for strategy alerts
CREATE INDEX IF NOT EXISTS idx_strategy_alerts 
ON backtesting.strategy_alerts(strategy_name, created_at);

CREATE INDEX IF NOT EXISTS idx_alert_level 
ON backtesting.strategy_alerts(alert_level);

CREATE INDEX IF NOT EXISTS idx_acknowledged 
ON backtesting.strategy_alerts(acknowledged);

CREATE INDEX IF NOT EXISTS idx_config_version_alerts 
ON backtesting.strategy_alerts(configuration_version);

-- Orchestrator Update Triggers Table
CREATE TABLE IF NOT EXISTS backtesting.orchestrator_update_triggers (
    id SERIAL PRIMARY KEY,
    trigger_type VARCHAR(50) NOT NULL, -- 'SCHEDULED', 'PERFORMANCE_CHANGE', 'MANUAL'
    trigger_reason TEXT,
    old_config_version VARCHAR(50),
    new_config_version VARCHAR(50),
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Trigger details
    changes_made JSONB,
    strategies_affected INTEGER DEFAULT 0,
    performance_impact JSONB,
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED', -- 'PENDING', 'COMPLETED', 'FAILED'
    error_message TEXT
);

-- Index for orchestrator triggers
CREATE INDEX IF NOT EXISTS idx_orchestrator_triggers_type_time 
ON backtesting.orchestrator_update_triggers(trigger_type, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_orchestrator_triggers_config 
ON backtesting.orchestrator_update_triggers(new_config_version);

-- Views for easy querying

-- Recent configuration changes
CREATE OR REPLACE VIEW backtesting.recent_config_changes AS
SELECT 
    h.config_version,
    h.created_at,
    h.snapshot_reason,
    h.total_strategies,
    h.enabled_strategies,
    h.avg_roi,
    h.avg_win_rate,
    t.trigger_type,
    t.trigger_reason,
    t.strategies_affected,
    LAG(h.enabled_strategies) OVER (ORDER BY h.created_at) as prev_enabled_strategies,
    LAG(h.avg_roi) OVER (ORDER BY h.created_at) as prev_avg_roi
FROM backtesting.strategy_config_history h
LEFT JOIN backtesting.orchestrator_update_triggers t 
    ON h.config_version = t.new_config_version
ORDER BY h.created_at DESC
LIMIT 20;

-- Strategy performance summary with lifecycle
CREATE OR REPLACE VIEW backtesting.strategy_status_summary AS
SELECT 
    sp.strategy_name,
    sp.win_rate,
    sp.roi_per_100,
    sp.total_bets,
    sch.strategy_status,
    sch.is_enabled,
    sch.confidence_multiplier,
    sch.configuration_version,
    sch.created_at as last_config_update,
    sle.event_type as last_lifecycle_event,
    sle.created_at as last_lifecycle_event_date
FROM backtesting.strategy_performance sp
JOIN backtesting.strategy_config_history sch ON sp.strategy_name = sch.strategy_name
LEFT JOIN LATERAL (
    SELECT event_type, created_at 
    FROM backtesting.strategy_lifecycle_events 
    WHERE strategy_name = sp.strategy_name 
    ORDER BY created_at DESC 
    LIMIT 1
) sle ON true
WHERE sch.created_at = (
    SELECT MAX(created_at) 
    FROM backtesting.strategy_config_history sch2 
    WHERE sch2.strategy_name = sp.strategy_name
)
AND sp.created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY sp.roi_per_100 DESC;

-- Critical alerts requiring attention
CREATE OR REPLACE VIEW backtesting.critical_alerts_unacknowledged AS
SELECT 
    sa.id,
    sa.strategy_name,
    sa.alert_level,
    sa.message,
    sa.current_roi,
    sa.current_win_rate,
    sa.recommended_action,
    sa.created_at,
    EXTRACT(HOURS FROM (CURRENT_TIMESTAMP - sa.created_at)) as hours_since_alert
FROM backtesting.strategy_alerts sa
WHERE sa.acknowledged = FALSE
AND sa.alert_level IN ('WARNING', 'CRITICAL')
AND sa.created_at >= CURRENT_DATE - INTERVAL '3 days'
ORDER BY 
    CASE sa.alert_level 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'WARNING' THEN 2 
        ELSE 3 
    END,
    sa.created_at DESC;

-- Grant permissions
GRANT SELECT, INSERT, UPDATE ON backtesting.strategy_config_history TO mlb_sharp_user;
GRANT SELECT, INSERT, UPDATE ON backtesting.strategy_lifecycle_events TO mlb_sharp_user;
GRANT SELECT, INSERT, UPDATE ON backtesting.configuration_performance TO mlb_sharp_user;
GRANT SELECT, INSERT, UPDATE ON backtesting.strategy_alerts TO mlb_sharp_user;
GRANT SELECT, INSERT, UPDATE ON backtesting.orchestrator_update_triggers TO mlb_sharp_user;

GRANT SELECT ON backtesting.recent_config_changes TO mlb_sharp_user;
GRANT SELECT ON backtesting.strategy_status_summary TO mlb_sharp_user;
GRANT SELECT ON backtesting.critical_alerts_unacknowledged TO mlb_sharp_user;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA backtesting TO mlb_sharp_user;

-- Function to cleanup old configuration history
CREATE OR REPLACE FUNCTION backtesting.cleanup_config_history()
RETURNS void AS $$
BEGIN
    -- Keep only last 100 configuration snapshots
    DELETE FROM backtesting.strategy_config_history 
    WHERE id NOT IN (
        SELECT id FROM backtesting.strategy_config_history 
        ORDER BY created_at DESC 
        LIMIT 100
    );
    
    -- Keep only last 30 days of trigger logs
    DELETE FROM backtesting.orchestrator_update_triggers 
    WHERE triggered_at < NOW() - INTERVAL '30 days';
    
    RAISE NOTICE 'Cleaned up old configuration history at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE backtesting.strategy_config_history IS 'Tracks historical strategy configurations for the orchestrator';
COMMENT ON TABLE backtesting.orchestrator_update_triggers IS 'Logs orchestrator update events and triggers';
COMMENT ON VIEW backtesting.recent_config_changes IS 'Shows recent configuration changes with comparison data'; 