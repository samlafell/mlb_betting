-- =============================================================================
-- DATA QUALITY VALIDATION TABLES
-- =============================================================================
-- Purpose: Tables for storing data quality validation results, metrics, and history
-- Issue #71: Implement Data Quality Validation Gates  
-- Date: 2025-01-22
-- =============================================================================

-- Ensure monitoring schema exists
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- DATA QUALITY VALIDATION HISTORY
-- =============================================================================

-- Data Quality Validation Runs
CREATE TABLE IF NOT EXISTS monitoring.data_quality_validation_runs (
    id BIGSERIAL PRIMARY KEY,
    
    -- Execution metadata
    run_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    stage VARCHAR(20) NOT NULL, -- raw, staging, curated, all
    execution_start_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    execution_end_time TIMESTAMPTZ,
    execution_duration_ms BIGINT,
    
    -- Overall results
    overall_score NUMERIC(5,4) NOT NULL CHECK (overall_score >= 0 AND overall_score <= 1),
    overall_status VARCHAR(20) NOT NULL CHECK (overall_status IN ('passed', 'warning', 'failed', 'pending')),
    
    -- Stage scores (for 'all' stage runs)
    raw_score NUMERIC(5,4) CHECK (raw_score >= 0 AND raw_score <= 1),
    staging_score NUMERIC(5,4) CHECK (staging_score >= 0 AND staging_score <= 1),
    curated_score NUMERIC(5,4) CHECK (curated_score >= 0 AND curated_score <= 1),
    
    -- Validation counts
    total_validations INTEGER DEFAULT 0,
    passed_validations INTEGER DEFAULT 0,
    warning_validations INTEGER DEFAULT 0,
    failed_validations INTEGER DEFAULT 0,
    
    -- Data metrics
    total_records_validated BIGINT DEFAULT 0,
    data_sources_count INTEGER DEFAULT 0,
    
    -- Quality metrics
    data_freshness_score NUMERIC(5,4) CHECK (data_freshness_score >= 0 AND data_freshness_score <= 1),
    anomaly_detection_score NUMERIC(5,4) CHECK (anomaly_detection_score >= 0 AND anomaly_detection_score <= 1),
    quality_gate_pass_rate NUMERIC(5,4) CHECK (quality_gate_pass_rate >= 0 AND quality_gate_pass_rate <= 1),
    
    -- Quality gates results
    raw_to_staging_gate BOOLEAN,
    staging_to_curated_gate BOOLEAN,
    curated_ready_gate BOOLEAN,
    no_critical_failures_gate BOOLEAN,
    overall_pipeline_ready_gate BOOLEAN,
    
    -- Metadata
    triggered_by VARCHAR(100), -- manual, scheduled, pipeline, alert
    configuration_hash VARCHAR(64), -- To track rule changes
    notes TEXT,
    
    CONSTRAINT valid_stage CHECK (stage IN ('raw', 'staging', 'curated', 'all'))
);

-- Data Quality Rule Results
CREATE TABLE IF NOT EXISTS monitoring.data_quality_rule_results (
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to validation run
    validation_run_id BIGINT NOT NULL REFERENCES monitoring.data_quality_validation_runs(id) ON DELETE CASCADE,
    
    -- Rule identification
    rule_name VARCHAR(100) NOT NULL,
    rule_stage VARCHAR(20) NOT NULL,
    rule_dimension VARCHAR(50) NOT NULL, -- completeness, accuracy, consistency, timeliness, validity, uniqueness
    
    -- Rule execution
    execution_start_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    execution_end_time TIMESTAMPTZ,
    execution_duration_ms INTEGER,
    
    -- Results
    score NUMERIC(5,4) NOT NULL CHECK (score >= 0 AND score <= 1),
    status VARCHAR(20) NOT NULL CHECK (status IN ('passed', 'warning', 'failed', 'pending')),
    threshold NUMERIC(5,4) NOT NULL CHECK (threshold >= 0 AND threshold <= 1),
    warning_threshold NUMERIC(5,4) NOT NULL CHECK (warning_threshold >= 0 AND warning_threshold <= 1),
    
    -- Data details
    total_records INTEGER DEFAULT 0,
    valid_records INTEGER DEFAULT 0,
    invalid_records INTEGER DEFAULT 0,
    
    -- Business impact
    business_impact VARCHAR(20) DEFAULT 'medium' CHECK (business_impact IN ('low', 'medium', 'high', 'critical')),
    
    -- Result details
    message TEXT,
    metadata JSONB,
    
    -- Performance optimization
    query_execution_time_ms INTEGER,
    rows_scanned BIGINT,
    
    CONSTRAINT valid_rule_stage CHECK (rule_stage IN ('raw', 'staging', 'curated')),
    CONSTRAINT valid_rule_dimension CHECK (rule_dimension IN ('completeness', 'accuracy', 'consistency', 'timeliness', 'validity', 'uniqueness'))
);

-- Data Quality Alerts History
CREATE TABLE IF NOT EXISTS monitoring.data_quality_alerts (
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to validation run (optional - alerts can be generated outside runs)
    validation_run_id BIGINT REFERENCES monitoring.data_quality_validation_runs(id) ON DELETE SET NULL,
    
    -- Alert identification
    alert_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    alert_level VARCHAR(20) NOT NULL CHECK (alert_level IN ('info', 'warning', 'error', 'critical')),
    
    -- Alert content
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    source VARCHAR(100) NOT NULL,
    
    -- Alert lifecycle
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100),
    resolved_by VARCHAR(100),
    
    -- Alert context
    stage VARCHAR(20),
    rule_name VARCHAR(100),
    affected_records BIGINT,
    severity_score NUMERIC(3,2) CHECK (severity_score >= 0 AND severity_score <= 1),
    
    -- Alert metadata
    metadata JSONB,
    
    -- Alert resolution
    resolution_notes TEXT,
    false_positive BOOLEAN DEFAULT FALSE
);

-- Data Quality Metrics Time Series
CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics_timeseries (
    id BIGSERIAL PRIMARY KEY,
    
    -- Time partitioning
    measurement_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    measurement_date DATE GENERATED ALWAYS AS (measurement_time::DATE) STORED,
    
    -- Metric identification
    stage VARCHAR(20) NOT NULL,
    dimension VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    
    -- Metric values
    value NUMERIC(10,4) NOT NULL,
    threshold NUMERIC(5,4),
    target NUMERIC(5,4),
    
    -- Metric metadata
    data_source VARCHAR(50),
    records_count BIGINT,
    measurement_interval_seconds INTEGER DEFAULT 300, -- 5 minutes default
    
    -- Tags for flexible querying
    tags JSONB,
    
    CONSTRAINT valid_stage_timeseries CHECK (stage IN ('raw', 'staging', 'curated', 'overall')),
    CONSTRAINT valid_dimension_timeseries CHECK (dimension IN ('completeness', 'accuracy', 'consistency', 'timeliness', 'validity', 'uniqueness', 'overall'))
);

-- =============================================================================
-- PERFORMANCE INDEXES
-- =============================================================================

-- Primary query patterns optimization
CREATE INDEX IF NOT EXISTS idx_dq_validation_runs_time ON monitoring.data_quality_validation_runs (execution_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_validation_runs_stage ON monitoring.data_quality_validation_runs (stage, execution_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_validation_runs_status ON monitoring.data_quality_validation_runs (overall_status, execution_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_validation_runs_score ON monitoring.data_quality_validation_runs (overall_score, execution_start_time DESC);

-- Rule results optimization
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_run_id ON monitoring.data_quality_rule_results (validation_run_id);
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_rule ON monitoring.data_quality_rule_results (rule_name, rule_stage);
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_dimension ON monitoring.data_quality_rule_results (rule_dimension, status);
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_business_impact ON monitoring.data_quality_rule_results (business_impact, status);
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_score ON monitoring.data_quality_rule_results (score, execution_start_time DESC);

-- Alerts optimization
CREATE INDEX IF NOT EXISTS idx_dq_alerts_level ON monitoring.data_quality_alerts (alert_level, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_alerts_status ON monitoring.data_quality_alerts (created_at DESC) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dq_alerts_stage ON monitoring.data_quality_alerts (stage, alert_level);
CREATE INDEX IF NOT EXISTS idx_dq_alerts_source ON monitoring.data_quality_alerts (source, created_at DESC);

-- Time series optimization (partitioned queries)
CREATE INDEX IF NOT EXISTS idx_dq_metrics_time ON monitoring.data_quality_metrics_timeseries (measurement_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_stage_dim ON monitoring.data_quality_metrics_timeseries (stage, dimension, measurement_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_name ON monitoring.data_quality_metrics_timeseries (metric_name, measurement_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_date_stage ON monitoring.data_quality_metrics_timeseries (measurement_date, stage);

-- Composite indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_dq_validation_runs_composite ON monitoring.data_quality_validation_runs (stage, overall_status, execution_start_time DESC);
CREATE INDEX IF NOT EXISTS idx_dq_rule_results_composite ON monitoring.data_quality_rule_results (rule_stage, status, business_impact, execution_start_time DESC);

-- =============================================================================
-- VIEWS FOR ANALYTICS
-- =============================================================================

-- Latest Quality Scores by Stage
CREATE OR REPLACE VIEW monitoring.latest_quality_scores AS
SELECT DISTINCT ON (stage)
    stage,
    overall_score,
    overall_status,
    execution_start_time,
    total_records_validated,
    data_freshness_score,
    quality_gate_pass_rate
FROM monitoring.data_quality_validation_runs
WHERE stage != 'all'
ORDER BY stage, execution_start_time DESC;

-- Quality Trends (24 hour rolling)
CREATE OR REPLACE VIEW monitoring.quality_trends_24h AS
SELECT 
    stage,
    AVG(overall_score) as avg_score,
    MIN(overall_score) as min_score,
    MAX(overall_score) as max_score,
    STDDEV(overall_score) as score_volatility,
    COUNT(*) as validation_count,
    COUNT(CASE WHEN overall_status = 'failed' THEN 1 END) as failure_count,
    (COUNT(CASE WHEN overall_status = 'failed' THEN 1 END)::FLOAT / COUNT(*)::FLOAT) as failure_rate
FROM monitoring.data_quality_validation_runs
WHERE execution_start_time > NOW() - INTERVAL '24 hours'
    AND stage != 'all'
GROUP BY stage;

-- Active Quality Alerts
CREATE OR REPLACE VIEW monitoring.active_quality_alerts AS
SELECT 
    alert_level,
    stage,
    title,
    message,
    source,
    created_at,
    EXTRACT(EPOCH FROM (NOW() - created_at))/3600 as hours_active,
    severity_score,
    affected_records
FROM monitoring.data_quality_alerts
WHERE resolved_at IS NULL
ORDER BY 
    CASE alert_level 
        WHEN 'critical' THEN 1 
        WHEN 'error' THEN 2 
        WHEN 'warning' THEN 3 
        ELSE 4 
    END,
    created_at DESC;

-- Rule Performance Analysis
CREATE OR REPLACE VIEW monitoring.rule_performance_summary AS
SELECT 
    rule_name,
    rule_stage,
    rule_dimension,
    COUNT(*) as execution_count,
    AVG(score) as avg_score,
    AVG(execution_duration_ms) as avg_execution_time_ms,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failure_count,
    COUNT(CASE WHEN business_impact = 'critical' AND status = 'failed' THEN 1 END) as critical_failures,
    MAX(execution_start_time) as last_execution
FROM monitoring.data_quality_rule_results
WHERE execution_start_time > NOW() - INTERVAL '7 days'
GROUP BY rule_name, rule_stage, rule_dimension
ORDER BY critical_failures DESC, failure_count DESC, avg_execution_time_ms DESC;

-- Quality SLA Dashboard
CREATE OR REPLACE VIEW monitoring.quality_sla_dashboard AS
SELECT 
    'raw'::TEXT as stage,
    COUNT(*) as total_validations,
    COUNT(CASE WHEN overall_score >= 0.85 THEN 1 END) as sla_compliant,
    (COUNT(CASE WHEN overall_score >= 0.85 THEN 1 END)::FLOAT / COUNT(*)::FLOAT * 100) as sla_percentage,
    AVG(overall_score) as avg_score,
    AVG(execution_duration_ms) as avg_duration_ms
FROM monitoring.data_quality_validation_runs 
WHERE stage = 'raw' AND execution_start_time > NOW() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'staging'::TEXT as stage,
    COUNT(*) as total_validations,
    COUNT(CASE WHEN overall_score >= 0.90 THEN 1 END) as sla_compliant,
    (COUNT(CASE WHEN overall_score >= 0.90 THEN 1 END)::FLOAT / COUNT(*)::FLOAT * 100) as sla_percentage,
    AVG(overall_score) as avg_score,
    AVG(execution_duration_ms) as avg_duration_ms
FROM monitoring.data_quality_validation_runs 
WHERE stage = 'staging' AND execution_start_time > NOW() - INTERVAL '24 hours'

UNION ALL

SELECT 
    'curated'::TEXT as stage,
    COUNT(*) as total_validations,
    COUNT(CASE WHEN overall_score >= 0.95 THEN 1 END) as sla_compliant,
    (COUNT(CASE WHEN overall_score >= 0.95 THEN 1 END)::FLOAT / COUNT(*)::FLOAT * 100) as sla_percentage,
    AVG(overall_score) as avg_score,
    AVG(execution_duration_ms) as avg_duration_ms
FROM monitoring.data_quality_validation_runs 
WHERE stage = 'curated' AND execution_start_time > NOW() - INTERVAL '24 hours';

-- =============================================================================
-- PERFORMANCE OPTIMIZATION FUNCTIONS
-- =============================================================================

-- Function to clean up old validation history (retention policy)
CREATE OR REPLACE FUNCTION monitoring.cleanup_old_quality_data(
    retention_days INTEGER DEFAULT 90
) RETURNS TABLE(
    deleted_runs INTEGER,
    deleted_rule_results INTEGER,
    deleted_alerts INTEGER,
    deleted_metrics INTEGER
) AS $$
DECLARE
    cutoff_date TIMESTAMPTZ;
    runs_deleted INTEGER;
    rules_deleted INTEGER;
    alerts_deleted INTEGER;
    metrics_deleted INTEGER;
BEGIN
    cutoff_date := NOW() - (retention_days || ' days')::INTERVAL;
    
    -- Delete old time series metrics (oldest first)
    DELETE FROM monitoring.data_quality_metrics_timeseries 
    WHERE measurement_time < cutoff_date;
    GET DIAGNOSTICS metrics_deleted = ROW_COUNT;
    
    -- Delete old alerts (resolved alerts older than retention)
    DELETE FROM monitoring.data_quality_alerts 
    WHERE created_at < cutoff_date AND resolved_at IS NOT NULL;
    GET DIAGNOSTICS alerts_deleted = ROW_COUNT;
    
    -- Delete old rule results (cascaded by validation runs)
    DELETE FROM monitoring.data_quality_rule_results 
    WHERE validation_run_id IN (
        SELECT id FROM monitoring.data_quality_validation_runs 
        WHERE execution_start_time < cutoff_date
    );
    GET DIAGNOSTICS rules_deleted = ROW_COUNT;
    
    -- Delete old validation runs
    DELETE FROM monitoring.data_quality_validation_runs 
    WHERE execution_start_time < cutoff_date;
    GET DIAGNOSTICS runs_deleted = ROW_COUNT;
    
    RETURN QUERY VALUES (runs_deleted, rules_deleted, alerts_deleted, metrics_deleted);
END;
$$ LANGUAGE plpgsql;

-- Function to get quality metrics for dashboard
CREATE OR REPLACE FUNCTION monitoring.get_quality_dashboard_metrics(
    hours_back INTEGER DEFAULT 24
) RETURNS TABLE(
    stage TEXT,
    current_score NUMERIC,
    avg_score_24h NUMERIC,
    trend_direction TEXT,
    active_alerts INTEGER,
    last_validation TIMESTAMPTZ,
    sla_compliance NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH recent_scores AS (
        SELECT 
            r.stage,
            r.overall_score,
            r.execution_start_time,
            ROW_NUMBER() OVER (PARTITION BY r.stage ORDER BY r.execution_start_time DESC) as rn
        FROM monitoring.data_quality_validation_runs r
        WHERE r.execution_start_time > NOW() - (hours_back || ' hours')::INTERVAL
            AND r.stage != 'all'
    ),
    latest_scores AS (
        SELECT stage, overall_score as current_score, execution_start_time as last_validation
        FROM recent_scores WHERE rn = 1
    ),
    avg_scores AS (
        SELECT 
            stage, 
            AVG(overall_score) as avg_score
        FROM recent_scores 
        GROUP BY stage
    ),
    alert_counts AS (
        SELECT 
            stage,
            COUNT(*) as alert_count
        FROM monitoring.data_quality_alerts
        WHERE created_at > NOW() - (hours_back || ' hours')::INTERVAL
            AND resolved_at IS NULL
        GROUP BY stage
    )
    SELECT 
        l.stage::TEXT,
        l.current_score,
        a.avg_score,
        CASE 
            WHEN l.current_score > a.avg_score THEN 'improving'
            WHEN l.current_score < a.avg_score THEN 'declining'
            ELSE 'stable'
        END::TEXT as trend_direction,
        COALESCE(ac.alert_count, 0)::INTEGER as active_alerts,
        l.last_validation,
        CASE l.stage
            WHEN 'raw' THEN CASE WHEN l.current_score >= 0.85 THEN 1.0 ELSE 0.0 END
            WHEN 'staging' THEN CASE WHEN l.current_score >= 0.90 THEN 1.0 ELSE 0.0 END
            WHEN 'curated' THEN CASE WHEN l.current_score >= 0.95 THEN 1.0 ELSE 0.0 END
            ELSE 0.0
        END as sla_compliance
    FROM latest_scores l
    LEFT JOIN avg_scores a ON l.stage = a.stage
    LEFT JOIN alert_counts ac ON l.stage = ac.stage
    ORDER BY l.stage;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA monitoring IS 'Monitoring and observability tables including data quality validation tracking';

COMMENT ON TABLE monitoring.data_quality_validation_runs IS 'Historical record of data quality validation executions with overall metrics and results';
COMMENT ON TABLE monitoring.data_quality_rule_results IS 'Detailed results for individual data quality validation rules within each run';
COMMENT ON TABLE monitoring.data_quality_alerts IS 'Data quality alerts with lifecycle tracking and resolution status';
COMMENT ON TABLE monitoring.data_quality_metrics_timeseries IS 'Time-series data for quality metrics trending and analysis';

COMMENT ON VIEW monitoring.latest_quality_scores IS 'Latest quality scores by pipeline stage for current status dashboard';
COMMENT ON VIEW monitoring.quality_trends_24h IS 'Quality trends analysis over the last 24 hours';
COMMENT ON VIEW monitoring.active_quality_alerts IS 'Currently active quality alerts requiring attention';
COMMENT ON VIEW monitoring.rule_performance_summary IS 'Performance analysis of individual quality rules over the last 7 days';
COMMENT ON VIEW monitoring.quality_sla_dashboard IS 'SLA compliance dashboard showing quality metrics against thresholds';

COMMENT ON FUNCTION monitoring.cleanup_old_quality_data(INTEGER) IS 'Cleanup function for maintaining data quality history with configurable retention policy';
COMMENT ON FUNCTION monitoring.get_quality_dashboard_metrics(INTEGER) IS 'Dashboard metrics function providing comprehensive quality status and trends';

-- =============================================================================
-- INITIAL DATA AND VALIDATION
-- =============================================================================

-- Verify table creation
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables 
    WHERE table_schema = 'monitoring' 
    AND table_name LIKE '%data_quality%';
    
    IF table_count >= 4 THEN
        RAISE NOTICE 'Data quality validation tables created successfully: % tables', table_count;
    ELSE
        RAISE WARNING 'Data quality validation table creation may be incomplete: only % tables found', table_count;
    END IF;
END $$;