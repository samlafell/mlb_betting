-- Strategy Configuration History Schema (Simplified)
-- Tables needed for the Phase 3C StrategyOrchestrator

-- Strategy Configuration History Table
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

-- Indexes for configuration history queries
CREATE INDEX IF NOT EXISTS idx_strategy_config_history_version 
ON backtesting.strategy_config_history(config_version);

CREATE INDEX IF NOT EXISTS idx_strategy_config_history_created 
ON backtesting.strategy_config_history(created_at DESC);

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

-- View for Recent Configuration Changes
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