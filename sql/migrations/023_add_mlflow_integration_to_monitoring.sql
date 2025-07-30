-- Migration 023: MLFlow Integration for Custom Monitoring
-- Purpose: Add MLFlow run/experiment references to custom monitoring tables
-- Integrates training lifecycle (MLFlow) with production lifecycle (custom monitoring)
-- Date: 2025-01-30

-- ================================
-- Add MLFlow References to Performance Alerts
-- ================================

ALTER TABLE curated.ml_performance_alerts 
ADD COLUMN IF NOT EXISTS mlflow_experiment_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS mlflow_run_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS model_artifact_uri TEXT;

-- ================================
-- Add MLFlow References to Feature Drift Detection
-- ================================

ALTER TABLE curated.ml_feature_drift_detection 
ADD COLUMN IF NOT EXISTS mlflow_experiment_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS mlflow_run_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS baseline_mlflow_run_id VARCHAR(100);

-- ================================
-- Add MLFlow References to Model Health Status
-- ================================

ALTER TABLE curated.ml_model_health_status 
ADD COLUMN IF NOT EXISTS mlflow_experiment_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS active_mlflow_run_ids TEXT[] DEFAULT '{}',
ADD COLUMN IF NOT EXISTS model_registry_name VARCHAR(200),
ADD COLUMN IF NOT EXISTS model_registry_version VARCHAR(50);

-- ================================
-- Create Unified MLFlow-Custom Monitoring Views
-- ================================

-- Unified model lifecycle view combining MLFlow training and custom production monitoring
CREATE OR REPLACE VIEW curated.unified_model_lifecycle AS
SELECT 
    -- Model identification
    me.experiment_name,
    me.mlflow_experiment_id,
    me.prediction_target,
    
    -- Training phase (MLFlow)
    me.status as experiment_status,
    me.total_runs as training_runs_completed,
    me.best_accuracy as training_best_accuracy,
    me.best_roi as training_best_roi,
    me.created_at as training_started,
    me.last_updated as training_last_updated,
    
    -- Production phase (Custom monitoring)
    mhs.health_score as production_health_score,
    mhs.status as production_status,
    mhs.recent_accuracy as production_accuracy,
    mhs.recent_roi as production_roi,
    mhs.recent_sharpe_ratio as production_sharpe,
    mhs.feature_drift_score as production_drift_score,
    mhs.critical_alerts_count + mhs.high_alerts_count as high_priority_alerts,
    mhs.action_required as production_action_required,
    
    -- Integration metrics
    CASE 
        WHEN mhs.recent_accuracy IS NOT NULL AND me.best_accuracy IS NOT NULL 
        THEN mhs.recent_accuracy - me.best_accuracy 
        ELSE NULL 
    END as accuracy_degradation,
    
    CASE 
        WHEN mhs.recent_roi IS NOT NULL AND me.best_roi IS NOT NULL 
        THEN mhs.recent_roi - me.best_roi 
        ELSE NULL 
    END as roi_degradation,
    
    -- Status assessment
    CASE
        WHEN mhs.status = 'critical' OR mhs.action_required = TRUE THEN 'CRITICAL'
        WHEN mhs.status = 'degraded' OR mhs.health_score < 0.7 THEN 'WARNING'
        WHEN mhs.status = 'healthy' AND me.status = 'active' THEN 'HEALTHY'
        WHEN me.status = 'active' AND mhs.status IS NULL THEN 'TRAINING_ONLY'
        ELSE 'UNKNOWN'
    END as overall_status,
    
    -- Timestamps
    mhs.evaluation_period_end as last_production_evaluation,
    mhs.updated_at as production_last_updated

FROM curated.ml_experiments me
LEFT JOIN curated.ml_model_health_status mhs 
    ON me.mlflow_experiment_id = mhs.mlflow_experiment_id
    AND mhs.evaluation_period_end = (
        SELECT MAX(evaluation_period_end) 
        FROM curated.ml_model_health_status mhs2 
        WHERE mhs2.mlflow_experiment_id = me.mlflow_experiment_id
    )
WHERE me.created_at >= NOW() - INTERVAL '90 days'
ORDER BY 
    CASE overall_status
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'HEALTHY' THEN 3
        WHEN 'TRAINING_ONLY' THEN 4
        ELSE 5
    END,
    me.last_updated DESC;

-- Unified alerts view with MLFlow context
CREATE OR REPLACE VIEW curated.unified_monitoring_alerts AS
SELECT 
    -- Alert information
    mpa.alert_id,
    mpa.model_name,
    mpa.model_version,
    mpa.prediction_type,
    mpa.alert_type,
    mpa.severity,
    mpa.message,
    mpa.current_value,
    mpa.threshold_value,
    mpa.baseline_value,
    mpa.resolved,
    mpa.created_at as alert_created_at,
    
    -- MLFlow context
    mpa.mlflow_experiment_id,
    mpa.mlflow_run_id,
    me.experiment_name,
    me.best_accuracy as training_best_accuracy,
    me.best_roi as training_best_roi,
    
    -- Model artifact information
    mpa.model_artifact_uri,
    
    -- Alert context
    CASE 
        WHEN mpa.mlflow_run_id IS NOT NULL THEN 'LINKED_TO_TRAINING'
        WHEN mpa.mlflow_experiment_id IS NOT NULL THEN 'EXPERIMENT_LINKED'
        ELSE 'PRODUCTION_ONLY'
    END as mlflow_integration_status,
    
    -- Performance comparison
    CASE 
        WHEN mpa.alert_type = 'accuracy_degradation' AND me.best_accuracy IS NOT NULL
        THEN ROUND((mpa.current_value - me.best_accuracy) * 100, 2)
        WHEN mpa.alert_type = 'roi_degradation' AND me.best_roi IS NOT NULL  
        THEN ROUND(mpa.current_value - me.best_roi, 2)
        ELSE NULL
    END as degradation_from_training_best

FROM curated.ml_performance_alerts mpa
LEFT JOIN curated.ml_experiments me ON mpa.mlflow_experiment_id = me.mlflow_experiment_id
WHERE mpa.created_at >= NOW() - INTERVAL '30 days'
ORDER BY 
    CASE mpa.severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        ELSE 4
    END,
    mpa.created_at DESC;

-- Feature drift with MLFlow training context
CREATE OR REPLACE VIEW curated.feature_drift_with_training_context AS
SELECT 
    -- Drift information
    fd.model_name,
    fd.model_version,
    fd.feature_name,
    fd.feature_type,
    fd.baseline_importance,
    fd.current_importance,
    fd.importance_drift,
    fd.drift_score,
    fd.drift_detected,
    fd.created_at as drift_detected_at,
    
    -- MLFlow context
    fd.mlflow_experiment_id,
    fd.mlflow_run_id,
    fd.baseline_mlflow_run_id,
    me.experiment_name,
    
    -- Training vs production comparison
    CASE 
        WHEN fd.baseline_mlflow_run_id IS NOT NULL THEN 'TRAINING_BASELINE'
        WHEN fd.mlflow_experiment_id IS NOT NULL THEN 'EXPERIMENT_LINKED'
        ELSE 'PRODUCTION_BASELINE'
    END as baseline_source,
    
    -- Drift severity assessment
    CASE 
        WHEN fd.drift_score > 0.3 THEN 'SEVERE'
        WHEN fd.drift_score > 0.2 THEN 'MODERATE' 
        WHEN fd.drift_score > 0.1 THEN 'MILD'
        ELSE 'MINIMAL'
    END as drift_severity

FROM curated.ml_feature_drift_detection fd
LEFT JOIN curated.ml_experiments me ON fd.mlflow_experiment_id = me.mlflow_experiment_id
WHERE fd.created_at >= NOW() - INTERVAL '30 days'
AND fd.drift_detected = TRUE
ORDER BY fd.drift_score DESC, fd.created_at DESC;

-- ================================
-- Indexes for MLFlow Integration
-- ================================

-- Performance alerts MLFlow indexes
CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_mlflow_experiment 
ON curated.ml_performance_alerts(mlflow_experiment_id);

CREATE INDEX IF NOT EXISTS idx_ml_performance_alerts_mlflow_run 
ON curated.ml_performance_alerts(mlflow_run_id);

-- Feature drift MLFlow indexes
CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_mlflow_experiment 
ON curated.ml_feature_drift_detection(mlflow_experiment_id);

CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_mlflow_run 
ON curated.ml_feature_drift_detection(mlflow_run_id);

CREATE INDEX IF NOT EXISTS idx_ml_feature_drift_baseline_run 
ON curated.ml_feature_drift_detection(baseline_mlflow_run_id);

-- Model health status MLFlow indexes
CREATE INDEX IF NOT EXISTS idx_ml_model_health_mlflow_experiment 
ON curated.ml_model_health_status(mlflow_experiment_id);

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON VIEW curated.unified_model_lifecycle IS 'Unified view combining MLFlow training metrics with custom production monitoring for complete model lifecycle visibility';
COMMENT ON VIEW curated.unified_monitoring_alerts IS 'Production alerts with MLFlow training context for better alert triage and model performance correlation';
COMMENT ON VIEW curated.feature_drift_with_training_context IS 'Feature drift detection results with MLFlow experiment context for training vs production analysis';

COMMENT ON COLUMN curated.ml_performance_alerts.mlflow_experiment_id IS 'Reference to MLFlow experiment for training context';
COMMENT ON COLUMN curated.ml_performance_alerts.mlflow_run_id IS 'Reference to specific MLFlow run that trained this model';
COMMENT ON COLUMN curated.ml_feature_drift_detection.baseline_mlflow_run_id IS 'MLFlow run ID used to establish baseline feature importance';