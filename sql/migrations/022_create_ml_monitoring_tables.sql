-- Migration 022: ML Monitoring and Alerting Tables
-- Purpose: Create tables for ML model performance monitoring and alerting
-- Date: 2025-01-30

-- ================================
-- ML Performance Alerts Table
-- ================================

CREATE TABLE IF NOT EXISTS curated.ml_performance_alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(255) UNIQUE NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    prediction_type VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    message TEXT NOT NULL,
    current_value DECIMAL(10, 6),
    threshold_value DECIMAL(10, 6),
    baseline_value DECIMAL(10, 6),
    metadata JSONB,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ================================
-- ML Feature Drift Detection Table
-- ================================

CREATE TABLE IF NOT EXISTS curated.ml_feature_drift_detection (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    feature_name VARCHAR(200) NOT NULL,
    feature_type VARCHAR(50) NOT NULL,
    baseline_importance DECIMAL(10, 8),
    current_importance DECIMAL(10, 8),
    importance_drift DECIMAL(10, 8),
    baseline_distribution JSONB,
    current_distribution JSONB,
    drift_score DECIMAL(10, 8),
    drift_threshold DECIMAL(10, 8) DEFAULT 0.1,
    drift_detected BOOLEAN DEFAULT FALSE,
    detection_method VARCHAR(50),
    evaluation_period_start TIMESTAMP NOT NULL,
    evaluation_period_end TIMESTAMP NOT NULL,
    sample_size INTEGER,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ================================
-- ML Model Health Status Table
-- ================================

CREATE TABLE IF NOT EXISTS curated.ml_model_health_status (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    prediction_type VARCHAR(50) NOT NULL,
    health_score DECIMAL(5, 4) CHECK (health_score >= 0 AND health_score <= 1),
    status VARCHAR(20) NOT NULL CHECK (status IN ('healthy', 'warning', 'critical', 'degraded')),
    
    -- Performance metrics
    recent_accuracy DECIMAL(10, 8),
    recent_roi DECIMAL(10, 4),
    recent_sharpe_ratio DECIMAL(10, 6),
    recent_max_drawdown DECIMAL(10, 4),
    recent_sample_size INTEGER,
    
    -- Drift metrics
    feature_drift_score DECIMAL(10, 8),
    prediction_drift_score DECIMAL(10, 8),
    
    -- Alert counts
    critical_alerts_count INTEGER DEFAULT 0,
    high_alerts_count INTEGER DEFAULT 0,
    medium_alerts_count INTEGER DEFAULT 0,
    low_alerts_count INTEGER DEFAULT 0,
    
    -- Evaluation period
    evaluation_period_start TIMESTAMP NOT NULL,
    evaluation_period_end TIMESTAMP NOT NULL,
    
    -- Recommendations
    recommendations TEXT[],
    action_required BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Ensure one record per model/type combination per evaluation period
    UNIQUE(model_name, model_version, prediction_type, evaluation_period_end)
);

-- ================================
-- Indexes for Performance
-- ================================

-- Performance alerts indexes
CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_model 
ON curated.ml_performance_alerts(model_name, model_version);

CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_severity_created 
ON curated.ml_performance_alerts(severity, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_type_created 
ON curated.ml_performance_alerts(alert_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_resolved 
ON curated.ml_performance_alerts(resolved, created_at DESC) 
WHERE resolved = FALSE;

-- Feature drift detection indexes
CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_model_feature 
ON curated.ml_feature_drift_detection(model_name, feature_name);

CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_detected 
ON curated.ml_feature_drift_detection(drift_detected, created_at DESC) 
WHERE drift_detected = TRUE;

CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_score 
ON curated.ml_feature_drift_detection(drift_score DESC, created_at DESC);

-- Model health status indexes
CREATE INDEX IF NOT EXISTS idx_ml_model_health_status_model 
ON curated.ml_model_health_status(model_name, model_version, prediction_type);

CREATE INDEX IF NOT EXISTS idx_ml_model_health_status_score 
ON curated.ml_model_health_status(health_score DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_model_health_status_action_required 
ON curated.ml_model_health_status(action_required, created_at DESC) 
WHERE action_required = TRUE;

-- ================================
-- Views for Monitoring Dashboard
-- ================================

-- Real-time alerts view
CREATE OR REPLACE VIEW curated.ml_alerts_dashboard AS
SELECT 
    mpa.model_name,
    mpa.model_version,
    mpa.prediction_type,
    mpa.severity,
    COUNT(*) as alert_count,
    COUNT(CASE WHEN mpa.resolved = FALSE THEN 1 END) as unresolved_count,
    MAX(mpa.created_at) as latest_alert,
    ARRAY_AGG(
        DISTINCT mpa.alert_type 
        ORDER BY mpa.alert_type
    ) as alert_types,
    AVG(mpa.current_value) as avg_current_value,
    MIN(mpa.threshold_value) as min_threshold
FROM curated.ml_performance_alerts mpa
WHERE mpa.created_at >= NOW() - INTERVAL '7 days'
GROUP BY mpa.model_name, mpa.model_version, mpa.prediction_type, mpa.severity
ORDER BY mpa.severity DESC, alert_count DESC;

-- Model health summary view
CREATE OR REPLACE VIEW curated.ml_health_summary AS
SELECT 
    mhs.model_name,
    mhs.model_version,
    mhs.prediction_type,
    mhs.health_score,
    mhs.status,
    mhs.recent_accuracy,
    mhs.recent_roi,
    mhs.recent_sharpe_ratio,
    mhs.feature_drift_score,
    mhs.critical_alerts_count + mhs.high_alerts_count as high_priority_alerts,
    mhs.action_required,
    mhs.evaluation_period_end as last_evaluation,
    
    -- Days since last evaluation
    EXTRACT(EPOCH FROM (NOW() - mhs.evaluation_period_end)) / 86400 as days_since_evaluation,
    
    -- Health trend (compare with previous evaluation)
    LAG(mhs.health_score) OVER (
        PARTITION BY mhs.model_name, mhs.prediction_type 
        ORDER BY mhs.evaluation_period_end
    ) as previous_health_score
    
FROM curated.ml_model_health_status mhs
WHERE mhs.evaluation_period_end >= NOW() - INTERVAL '30 days'
ORDER BY mhs.health_score ASC, mhs.evaluation_period_end DESC;

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE curated.ml_performance_alerts IS 'ML model performance alerts and degradation warnings';
COMMENT ON TABLE curated.ml_feature_drift_detection IS 'Feature drift detection results and importance tracking';
COMMENT ON TABLE curated.ml_model_health_status IS 'Overall model health status and recommendations';

COMMENT ON VIEW curated.ml_alerts_dashboard IS 'Real-time alerts dashboard for ML monitoring';
COMMENT ON VIEW curated.ml_health_summary IS 'Model health summary with trends and evaluation status';