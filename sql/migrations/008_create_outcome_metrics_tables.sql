-- Migration 006: Create Outcome Metrics Tables
-- Purpose: Add outcome metrics to data quality monitoring system
-- Reference: CLAUDE.md improvement task

-- Create monitoring schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create outcome metrics table
CREATE TABLE IF NOT EXISTS monitoring.outcome_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_id VARCHAR(255) UNIQUE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    
    -- Game and strategy identifiers
    game_external_id VARCHAR(255) NOT NULL,
    strategy_name VARCHAR(100),
    sportsbook_name VARCHAR(100),
    bet_type VARCHAR(50),
    
    -- Metric values
    predicted_value DECIMAL(10,6),
    actual_value DECIMAL(10,6),
    accuracy_score DECIMAL(5,4) CHECK (accuracy_score >= 0 AND accuracy_score <= 1),
    confidence_level DECIMAL(5,4) CHECK (confidence_level >= 0 AND confidence_level <= 1),
    
    -- Data quality correlation
    data_quality_score DECIMAL(5,4) CHECK (data_quality_score >= 0 AND data_quality_score <= 1),
    sharp_action_detected BOOLEAN,
    line_movement_magnitude DECIMAL(10,4),
    
    -- Timestamps
    game_date TIMESTAMPTZ NOT NULL,
    metric_calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    outcome_confirmed_at TIMESTAMPTZ,
    
    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_outcome_metrics_game_external_id 
    ON monitoring.outcome_metrics(game_external_id);

CREATE INDEX IF NOT EXISTS idx_outcome_metrics_strategy_name 
    ON monitoring.outcome_metrics(strategy_name);

CREATE INDEX IF NOT EXISTS idx_outcome_metrics_metric_type 
    ON monitoring.outcome_metrics(metric_type);

CREATE INDEX IF NOT EXISTS idx_outcome_metrics_game_date 
    ON monitoring.outcome_metrics(game_date);

CREATE INDEX IF NOT EXISTS idx_outcome_metrics_data_quality_score 
    ON monitoring.outcome_metrics(data_quality_score);

-- Composite index for performance analysis queries
CREATE INDEX IF NOT EXISTS idx_outcome_metrics_performance_analysis
    ON monitoring.outcome_metrics(strategy_name, metric_type, game_date, accuracy_score);

-- Create outcome metrics aggregations table for pre-computed summaries
CREATE TABLE IF NOT EXISTS monitoring.outcome_metrics_aggregations (
    id BIGSERIAL PRIMARY KEY,
    
    -- Aggregation metadata
    aggregation_period VARCHAR(20) NOT NULL, -- 'daily', 'weekly', 'monthly'
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    strategy_name VARCHAR(100),
    sportsbook_name VARCHAR(100),
    
    -- Performance metrics
    total_predictions INTEGER NOT NULL DEFAULT 0,
    correct_predictions INTEGER NOT NULL DEFAULT 0,
    accuracy_rate DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    average_confidence DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    -- Data quality correlation metrics
    high_quality_predictions INTEGER NOT NULL DEFAULT 0,
    high_quality_correct INTEGER NOT NULL DEFAULT 0,
    high_quality_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    low_quality_predictions INTEGER NOT NULL DEFAULT 0,
    low_quality_correct INTEGER NOT NULL DEFAULT 0,
    low_quality_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    quality_correlation_coefficient DECIMAL(6,4) NOT NULL DEFAULT 0.0,
    
    -- Sharp action correlation metrics
    sharp_action_predictions INTEGER NOT NULL DEFAULT 0,
    sharp_action_correct INTEGER NOT NULL DEFAULT 0,
    sharp_action_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    no_sharp_predictions INTEGER NOT NULL DEFAULT 0,
    no_sharp_correct INTEGER NOT NULL DEFAULT 0,
    no_sharp_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    sharp_correlation_coefficient DECIMAL(6,4) NOT NULL DEFAULT 0.0,
    
    -- Line movement correlation
    avg_line_movement DECIMAL(10,4) NOT NULL DEFAULT 0.0,
    high_movement_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    low_movement_accuracy DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    
    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure unique aggregations
    UNIQUE(aggregation_period, period_start, metric_type, strategy_name, sportsbook_name)
);

-- Create indexes for aggregations table
CREATE INDEX IF NOT EXISTS idx_outcome_aggregations_period 
    ON monitoring.outcome_metrics_aggregations(aggregation_period, period_start);

CREATE INDEX IF NOT EXISTS idx_outcome_aggregations_strategy 
    ON monitoring.outcome_metrics_aggregations(strategy_name, metric_type);

-- Create data quality impact tracking view
CREATE OR REPLACE VIEW monitoring.data_quality_impact_summary AS
SELECT 
    om.metric_type,
    om.strategy_name,
    COUNT(*) as total_metrics,
    
    -- Overall performance
    AVG(om.accuracy_score) as overall_accuracy,
    AVG(om.confidence_level) as overall_confidence,
    AVG(om.data_quality_score) as avg_data_quality,
    
    -- Quality tier performance
    AVG(CASE WHEN om.data_quality_score >= 0.9 THEN om.accuracy_score END) as excellent_quality_accuracy,
    AVG(CASE WHEN om.data_quality_score >= 0.8 AND om.data_quality_score < 0.9 THEN om.accuracy_score END) as good_quality_accuracy,
    AVG(CASE WHEN om.data_quality_score >= 0.6 AND om.data_quality_score < 0.8 THEN om.accuracy_score END) as fair_quality_accuracy,
    AVG(CASE WHEN om.data_quality_score < 0.6 THEN om.accuracy_score END) as poor_quality_accuracy,
    
    -- Quality impact calculation
    (AVG(CASE WHEN om.data_quality_score >= 0.9 THEN om.accuracy_score END) - 
     AVG(CASE WHEN om.data_quality_score < 0.6 THEN om.accuracy_score END)) as quality_impact,
    
    -- Sharp action correlation
    AVG(CASE WHEN om.sharp_action_detected = true THEN om.accuracy_score END) as sharp_action_accuracy,
    AVG(CASE WHEN om.sharp_action_detected = false THEN om.accuracy_score END) as no_sharp_accuracy,
    (AVG(CASE WHEN om.sharp_action_detected = true THEN om.accuracy_score END) - 
     AVG(CASE WHEN om.sharp_action_detected = false THEN om.accuracy_score END)) as sharp_impact,
    
    -- Recent performance (last 7 days)
    AVG(CASE WHEN om.game_date >= NOW() - INTERVAL '7 days' THEN om.accuracy_score END) as recent_accuracy,
    
    -- Count by time periods
    COUNT(CASE WHEN om.game_date >= NOW() - INTERVAL '7 days' THEN 1 END) as last_7_days,
    COUNT(CASE WHEN om.game_date >= NOW() - INTERVAL '30 days' THEN 1 END) as last_30_days,
    
    -- Last updated
    MAX(om.updated_at) as last_updated
    
FROM monitoring.outcome_metrics om
WHERE om.outcome_confirmed_at IS NOT NULL  -- Only confirmed outcomes
GROUP BY om.metric_type, om.strategy_name
ORDER BY om.strategy_name, om.metric_type;

-- Create strategy performance comparison view
CREATE OR REPLACE VIEW monitoring.strategy_performance_comparison AS
SELECT 
    s.strategy_name,
    s.total_predictions,
    s.overall_accuracy,
    s.overall_confidence,
    s.quality_impact,
    s.sharp_impact,
    s.recent_accuracy,
    
    -- Performance ranking
    RANK() OVER (ORDER BY s.overall_accuracy DESC) as accuracy_rank,
    RANK() OVER (ORDER BY s.quality_impact DESC) as quality_impact_rank,
    RANK() OVER (ORDER BY s.recent_accuracy DESC) as recent_performance_rank,
    
    -- Performance grade
    CASE 
        WHEN s.overall_accuracy >= 0.65 THEN 'A'
        WHEN s.overall_accuracy >= 0.60 THEN 'B'
        WHEN s.overall_accuracy >= 0.55 THEN 'C'
        WHEN s.overall_accuracy >= 0.50 THEN 'D'
        ELSE 'F'
    END as performance_grade,
    
    -- Data dependency grade
    CASE 
        WHEN s.quality_impact >= 0.15 THEN 'High'
        WHEN s.quality_impact >= 0.05 THEN 'Medium'
        WHEN s.quality_impact >= -0.05 THEN 'Low'
        ELSE 'Negative'
    END as data_dependency_level
    
FROM monitoring.data_quality_impact_summary s
WHERE s.metric_type = 'strategy_performance'
  AND s.total_metrics >= 10  -- Only strategies with sufficient data
ORDER BY s.overall_accuracy DESC;

-- Create function to update outcome metrics aggregations
CREATE OR REPLACE FUNCTION monitoring.update_outcome_metrics_aggregations(
    p_aggregation_period VARCHAR(20) DEFAULT 'daily'
) RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    start_date TIMESTAMPTZ;
    end_date TIMESTAMPTZ;
BEGIN
    -- Determine date range based on aggregation period
    CASE p_aggregation_period
        WHEN 'daily' THEN
            start_date := DATE_TRUNC('day', NOW() - INTERVAL '1 day');
            end_date := DATE_TRUNC('day', NOW());
        WHEN 'weekly' THEN
            start_date := DATE_TRUNC('week', NOW() - INTERVAL '1 week');
            end_date := DATE_TRUNC('week', NOW());
        WHEN 'monthly' THEN
            start_date := DATE_TRUNC('month', NOW() - INTERVAL '1 month');
            end_date := DATE_TRUNC('month', NOW());
        ELSE
            RAISE EXCEPTION 'Invalid aggregation period: %', p_aggregation_period;
    END CASE;
    
    -- Update aggregations for each strategy and metric type combination
    INSERT INTO monitoring.outcome_metrics_aggregations (
        aggregation_period, period_start, period_end, metric_type, strategy_name,
        total_predictions, correct_predictions, accuracy_rate, average_confidence,
        high_quality_predictions, high_quality_correct, high_quality_accuracy,
        low_quality_predictions, low_quality_correct, low_quality_accuracy,
        sharp_action_predictions, sharp_action_correct, sharp_action_accuracy,
        no_sharp_predictions, no_sharp_correct, no_sharp_accuracy,
        avg_line_movement
    )
    SELECT 
        p_aggregation_period,
        start_date,
        end_date,
        om.metric_type,
        om.strategy_name,
        
        -- Overall metrics
        COUNT(*) as total_predictions,
        SUM(CASE WHEN om.accuracy_score > 0.5 THEN 1 ELSE 0 END) as correct_predictions,
        AVG(om.accuracy_score) as accuracy_rate,
        AVG(om.confidence_level) as average_confidence,
        
        -- High quality metrics (>= 0.8)
        COUNT(CASE WHEN om.data_quality_score >= 0.8 THEN 1 END) as high_quality_predictions,
        SUM(CASE WHEN om.data_quality_score >= 0.8 AND om.accuracy_score > 0.5 THEN 1 ELSE 0 END) as high_quality_correct,
        AVG(CASE WHEN om.data_quality_score >= 0.8 THEN om.accuracy_score END) as high_quality_accuracy,
        
        -- Low quality metrics (< 0.6)
        COUNT(CASE WHEN om.data_quality_score < 0.6 THEN 1 END) as low_quality_predictions,
        SUM(CASE WHEN om.data_quality_score < 0.6 AND om.accuracy_score > 0.5 THEN 1 ELSE 0 END) as low_quality_correct,
        AVG(CASE WHEN om.data_quality_score < 0.6 THEN om.accuracy_score END) as low_quality_accuracy,
        
        -- Sharp action metrics
        COUNT(CASE WHEN om.sharp_action_detected = true THEN 1 END) as sharp_action_predictions,
        SUM(CASE WHEN om.sharp_action_detected = true AND om.accuracy_score > 0.5 THEN 1 ELSE 0 END) as sharp_action_correct,
        AVG(CASE WHEN om.sharp_action_detected = true THEN om.accuracy_score END) as sharp_action_accuracy,
        
        -- No sharp action metrics
        COUNT(CASE WHEN om.sharp_action_detected = false THEN 1 END) as no_sharp_predictions,
        SUM(CASE WHEN om.sharp_action_detected = false AND om.accuracy_score > 0.5 THEN 1 ELSE 0 END) as no_sharp_correct,
        AVG(CASE WHEN om.sharp_action_detected = false THEN om.accuracy_score END) as no_sharp_accuracy,
        
        -- Line movement
        AVG(om.line_movement_magnitude) as avg_line_movement
        
    FROM monitoring.outcome_metrics om
    WHERE om.game_date >= start_date 
      AND om.game_date < end_date
      AND om.outcome_confirmed_at IS NOT NULL
    GROUP BY om.metric_type, om.strategy_name
    
    ON CONFLICT (aggregation_period, period_start, metric_type, strategy_name, sportsbook_name)
    DO UPDATE SET
        total_predictions = EXCLUDED.total_predictions,
        correct_predictions = EXCLUDED.correct_predictions,
        accuracy_rate = EXCLUDED.accuracy_rate,
        average_confidence = EXCLUDED.average_confidence,
        high_quality_predictions = EXCLUDED.high_quality_predictions,
        high_quality_correct = EXCLUDED.high_quality_correct,
        high_quality_accuracy = EXCLUDED.high_quality_accuracy,
        low_quality_predictions = EXCLUDED.low_quality_predictions,
        low_quality_correct = EXCLUDED.low_quality_correct,
        low_quality_accuracy = EXCLUDED.low_quality_accuracy,
        sharp_action_predictions = EXCLUDED.sharp_action_predictions,
        sharp_action_correct = EXCLUDED.sharp_action_correct,
        sharp_action_accuracy = EXCLUDED.sharp_action_accuracy,
        no_sharp_predictions = EXCLUDED.no_sharp_predictions,
        no_sharp_correct = EXCLUDED.no_sharp_correct,
        no_sharp_accuracy = EXCLUDED.no_sharp_accuracy,
        avg_line_movement = EXCLUDED.avg_line_movement,
        updated_at = NOW();
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION monitoring.update_outcome_metrics_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outcome_metrics_update_timestamp
    BEFORE UPDATE ON monitoring.outcome_metrics
    FOR EACH ROW
    EXECUTE FUNCTION monitoring.update_outcome_metrics_updated_at();

CREATE TRIGGER outcome_metrics_aggregations_update_timestamp
    BEFORE UPDATE ON monitoring.outcome_metrics_aggregations
    FOR EACH ROW
    EXECUTE FUNCTION monitoring.update_outcome_metrics_updated_at();

-- Grant permissions
GRANT USAGE ON SCHEMA monitoring TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON monitoring.outcome_metrics TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON monitoring.outcome_metrics_aggregations TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO PUBLIC;

-- Create sample data for testing (optional)
-- This can be removed in production
INSERT INTO monitoring.outcome_metrics (
    metric_id, metric_type, game_external_id, strategy_name,
    bet_type, predicted_value, actual_value, accuracy_score,
    confidence_level, data_quality_score, sharp_action_detected,
    line_movement_magnitude, game_date
) VALUES 
(
    'sample_strategy_performance_001',
    'strategy_performance',
    'mlb_game_2024_001',
    'sharp_action_detector',
    'moneyline',
    1.0, 1.0, 1.0, 0.85, 0.92, true, 2.5,
    '2024-07-01 19:00:00-04'
),
(
    'sample_strategy_performance_002',
    'strategy_performance',
    'mlb_game_2024_002',
    'sharp_action_detector',
    'moneyline',
    1.0, 0.0, 0.0, 0.75, 0.45, false, 0.5,
    '2024-07-02 19:00:00-04'
),
(
    'sample_line_accuracy_001',
    'betting_line_accuracy',
    'mlb_game_2024_001',
    NULL,
    'spread',
    -1.5, -2.0, 0.67, 0.80, 0.88, true, 2.5,
    '2024-07-01 19:00:00-04'
);

-- Create initial aggregations
SELECT monitoring.update_outcome_metrics_aggregations('daily');

COMMIT;