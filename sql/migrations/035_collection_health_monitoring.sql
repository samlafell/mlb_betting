-- Collection Health Monitoring Schema
-- Implements comprehensive health monitoring for data collection operations
-- Part of solution for GitHub Issue #36: "Data Collection Fails Silently"

-- =====================================================
-- Collection Health Monitoring Tables
-- =====================================================

-- Main collection health monitoring table
CREATE TABLE IF NOT EXISTS operational.collection_health_monitoring (
    id SERIAL PRIMARY KEY,
    
    -- Source information
    source VARCHAR(50) NOT NULL,
    collector_type VARCHAR(50),
    
    -- Timing information
    collection_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collection_duration_ms INTEGER,
    
    -- Collection metrics
    records_collected INTEGER DEFAULT 0,
    records_valid INTEGER DEFAULT 0,
    records_invalid INTEGER DEFAULT 0,
    
    -- Health indicators
    success_rate DECIMAL(5,2) DEFAULT 0.00,
    confidence_score DECIMAL(3,2) DEFAULT 1.00, -- 0.00 = no confidence, 1.00 = full confidence
    avg_response_time_ms DECIMAL(10,3),
    
    -- Gap detection
    last_successful_collection TIMESTAMPTZ,
    gap_duration_hours DECIMAL(6,2) DEFAULT 0.00,
    consecutive_failures INTEGER DEFAULT 0,
    
    -- Status and alerts
    health_status VARCHAR(20) DEFAULT 'unknown' CHECK (health_status IN ('healthy', 'degraded', 'critical', 'unknown')),
    alert_level VARCHAR(20) DEFAULT 'normal' CHECK (alert_level IN ('normal', 'warning', 'critical')),
    
    -- Metadata
    metadata JSONB,
    failure_patterns TEXT[],
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Collection alerts table
CREATE TABLE IF NOT EXISTS operational.collection_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Alert information
    source VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    message TEXT NOT NULL,
    
    -- Timing information
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    
    -- Context information
    metadata JSONB,
    failure_patterns TEXT[],
    
    -- Recovery information
    recovery_suggestions TEXT[],
    is_auto_recoverable BOOLEAN DEFAULT false,
    
    -- Status tracking
    is_active BOOLEAN DEFAULT true,
    resolution_notes TEXT
);

-- Collection gap detection table
CREATE TABLE IF NOT EXISTS operational.collection_gaps (
    id SERIAL PRIMARY KEY,
    
    -- Gap information
    source VARCHAR(50) NOT NULL,
    gap_start TIMESTAMPTZ NOT NULL,
    gap_end TIMESTAMPTZ,
    gap_duration_hours DECIMAL(6,2),
    
    -- Detection information
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detection_method VARCHAR(50), -- 'automated', 'manual', 'alert_rule'
    
    -- Resolution information
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMPTZ,
    resolution_method VARCHAR(50), -- 'automatic_recovery', 'manual_intervention', 'collector_restart'
    resolution_notes TEXT,
    
    -- Impact assessment
    estimated_missing_records INTEGER,
    business_impact_level VARCHAR(20) DEFAULT 'unknown' CHECK (business_impact_level IN ('low', 'medium', 'high', 'critical', 'unknown')),
    
    -- Metadata
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dead tuple monitoring table
CREATE TABLE IF NOT EXISTS operational.dead_tuple_monitoring (
    id SERIAL PRIMARY KEY,
    
    -- Table information
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    full_table_name VARCHAR(100) GENERATED ALWAYS AS (schema_name || '.' || table_name) STORED,
    
    -- Tuple statistics
    live_tuples BIGINT,
    dead_tuples BIGINT,
    dead_tuple_ratio DECIMAL(5,4),
    
    -- Alert information
    alert_threshold_ratio DECIMAL(5,4) DEFAULT 0.50,
    alert_sent BOOLEAN DEFAULT false,
    alert_sent_at TIMESTAMPTZ,
    
    -- Resolution tracking
    vacuum_recommended BOOLEAN DEFAULT false,
    vacuum_completed BOOLEAN DEFAULT false,
    vacuum_completed_at TIMESTAMPTZ,
    
    -- Monitoring metadata
    monitoring_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Collection performance history table
CREATE TABLE IF NOT EXISTS operational.collection_performance_history (
    id SERIAL PRIMARY KEY,
    
    -- Source and timing
    source VARCHAR(50) NOT NULL,
    collection_date DATE NOT NULL,
    collection_hour INTEGER NOT NULL CHECK (collection_hour >= 0 AND collection_hour <= 23),
    
    -- Aggregated performance metrics
    total_collections INTEGER DEFAULT 0,
    successful_collections INTEGER DEFAULT 0,
    failed_collections INTEGER DEFAULT 0,
    avg_confidence_score DECIMAL(3,2),
    avg_response_time_ms DECIMAL(10,3),
    
    -- Data quality metrics
    total_records_collected BIGINT DEFAULT 0,
    avg_records_per_collection DECIMAL(10,2),
    data_quality_score DECIMAL(3,2),
    
    -- Alert metrics
    alerts_generated INTEGER DEFAULT 0,
    critical_alerts INTEGER DEFAULT 0,
    warning_alerts INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicates
    UNIQUE(source, collection_date, collection_hour)
);

-- =====================================================
-- Indexes for Performance
-- =====================================================

-- Collection health monitoring indexes
CREATE INDEX IF NOT EXISTS idx_collection_health_source_timestamp 
ON operational.collection_health_monitoring(source, collection_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_collection_health_status 
ON operational.collection_health_monitoring(health_status, alert_level);

CREATE INDEX IF NOT EXISTS idx_collection_health_gaps 
ON operational.collection_health_monitoring(gap_duration_hours DESC) 
WHERE gap_duration_hours > 0;

CREATE INDEX IF NOT EXISTS idx_collection_health_failures 
ON operational.collection_health_monitoring(consecutive_failures DESC) 
WHERE consecutive_failures > 0;

-- Collection alerts indexes
CREATE INDEX IF NOT EXISTS idx_collection_alerts_source_active 
ON operational.collection_alerts(source, is_active, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_alerts_severity_active 
ON operational.collection_alerts(severity, is_active, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_alerts_created_at 
ON operational.collection_alerts(created_at DESC);

-- Collection gaps indexes
CREATE INDEX IF NOT EXISTS idx_collection_gaps_source_detected 
ON operational.collection_gaps(source, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_gaps_unresolved 
ON operational.collection_gaps(resolved, gap_duration_hours DESC) 
WHERE NOT resolved;

-- Dead tuple monitoring indexes
CREATE INDEX IF NOT EXISTS idx_dead_tuple_ratio 
ON operational.dead_tuple_monitoring(dead_tuple_ratio DESC, alert_sent);

CREATE INDEX IF NOT EXISTS idx_dead_tuple_table_timestamp 
ON operational.dead_tuple_monitoring(full_table_name, monitoring_timestamp DESC);

-- Performance history indexes
CREATE INDEX IF NOT EXISTS idx_collection_performance_source_date 
ON operational.collection_performance_history(source, collection_date DESC, collection_hour);

CREATE INDEX IF NOT EXISTS idx_collection_performance_quality 
ON operational.collection_performance_history(data_quality_score DESC, avg_confidence_score DESC);

-- =====================================================
-- Automated Triggers and Functions
-- =====================================================

-- Function to automatically detect collection gaps
CREATE OR REPLACE FUNCTION detect_collection_gaps()
RETURNS void AS $$
DECLARE
    gap_record RECORD;
    gap_threshold_hours DECIMAL := 4.0;
BEGIN
    -- Detect gaps for each source
    FOR gap_record IN 
        SELECT 
            source,
            MAX(collection_timestamp) as last_collection,
            EXTRACT(EPOCH FROM (NOW() - MAX(collection_timestamp))) / 3600 as hours_since_last
        FROM operational.collection_health_monitoring
        GROUP BY source
        HAVING EXTRACT(EPOCH FROM (NOW() - MAX(collection_timestamp))) / 3600 > gap_threshold_hours
    LOOP
        -- Insert gap record if not already recorded
        INSERT INTO operational.collection_gaps (
            source, 
            gap_start, 
            gap_duration_hours, 
            detection_method,
            estimated_missing_records,
            business_impact_level
        )
        SELECT 
            gap_record.source,
            gap_record.last_collection,
            gap_record.hours_since_last,
            'automated',
            -- Estimate missing records based on historical average
            COALESCE((
                SELECT AVG(records_collected) * gap_record.hours_since_last
                FROM operational.collection_health_monitoring 
                WHERE source = gap_record.source 
                AND collection_timestamp > NOW() - INTERVAL '7 days'
            ), 0)::INTEGER,
            CASE 
                WHEN gap_record.hours_since_last > 24 THEN 'critical'
                WHEN gap_record.hours_since_last > 12 THEN 'high'
                WHEN gap_record.hours_since_last > 6 THEN 'medium'
                ELSE 'low'
            END
        WHERE NOT EXISTS (
            SELECT 1 FROM operational.collection_gaps 
            WHERE source = gap_record.source 
            AND NOT resolved 
            AND gap_start >= gap_record.last_collection
        );
        
        -- Insert alert for the gap
        INSERT INTO operational.collection_alerts (
            source,
            alert_type,
            severity,
            message,
            metadata,
            recovery_suggestions,
            is_auto_recoverable
        ) VALUES (
            gap_record.source,
            'collection_gap',
            CASE 
                WHEN gap_record.hours_since_last > 8 THEN 'critical'
                ELSE 'warning'
            END,
            format('Collection gap detected for %s: %.1f hours since last collection', 
                   gap_record.source, gap_record.hours_since_last),
            jsonb_build_object(
                'gap_hours', gap_record.hours_since_last,
                'last_collection', gap_record.last_collection,
                'detection_method', 'automated'
            ),
            ARRAY[
                'Check collector health and restart if necessary',
                'Verify network connectivity and API endpoints', 
                'Review collector logs for error patterns'
            ],
            true
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to monitor dead tuple accumulation
CREATE OR REPLACE FUNCTION monitor_dead_tuples()
RETURNS void AS $$
DECLARE
    table_record RECORD;
    alert_threshold DECIMAL := 0.50;
BEGIN
    -- Check all user tables for dead tuple accumulation
    FOR table_record IN
        SELECT 
            schemaname,
            tablename,
            n_live_tup,
            n_dead_tup,
            CASE 
                WHEN n_live_tup = 0 THEN 
                    CASE WHEN n_dead_tup > 0 THEN 1.0 ELSE 0.0 END
                ELSE n_dead_tup::DECIMAL / GREATEST(n_live_tup, 1)
            END as dead_tuple_ratio
        FROM pg_stat_user_tables
        WHERE schemaname IN ('raw_data', 'staging', 'curated', 'operational')
        AND n_dead_tup > 10
    LOOP
        -- Insert or update monitoring record
        INSERT INTO operational.dead_tuple_monitoring (
            schema_name,
            table_name,
            live_tuples,
            dead_tuples,
            dead_tuple_ratio,
            vacuum_recommended
        ) VALUES (
            table_record.schemaname,
            table_record.tablename,
            table_record.n_live_tup,
            table_record.n_dead_tup,
            table_record.dead_tuple_ratio,
            table_record.dead_tuple_ratio > alert_threshold
        )
        ON CONFLICT (schema_name, table_name, DATE(monitoring_timestamp))
        DO UPDATE SET
            live_tuples = EXCLUDED.live_tuples,
            dead_tuples = EXCLUDED.dead_tuples,
            dead_tuple_ratio = EXCLUDED.dead_tuple_ratio,
            vacuum_recommended = EXCLUDED.vacuum_recommended,
            monitoring_timestamp = NOW();
        
        -- Send alert if threshold exceeded and not already alerted
        IF table_record.dead_tuple_ratio > alert_threshold THEN
            INSERT INTO operational.collection_alerts (
                source,
                alert_type,
                severity,
                message,
                metadata,
                recovery_suggestions,
                is_auto_recoverable
            )
            SELECT 
                table_record.schemaname || '.' || table_record.tablename,
                'dead_tuple_accumulation',
                CASE 
                    WHEN table_record.dead_tuple_ratio > 0.8 THEN 'critical'
                    ELSE 'warning'
                END,
                format('High dead tuple ratio in %s.%s: %.1f%%', 
                       table_record.schemaname, table_record.tablename, 
                       table_record.dead_tuple_ratio * 100),
                jsonb_build_object(
                    'schema_name', table_record.schemaname,
                    'table_name', table_record.tablename,
                    'dead_tuple_ratio', table_record.dead_tuple_ratio,
                    'live_tuples', table_record.n_live_tup,
                    'dead_tuples', table_record.n_dead_tup
                ),
                ARRAY[
                    format('Run VACUUM FULL on %s.%s', table_record.schemaname, table_record.tablename),
                    'Investigate transaction patterns causing dead tuples',
                    'Check for long-running transactions or failed commits'
                ],
                false
            WHERE NOT EXISTS (
                SELECT 1 FROM operational.collection_alerts 
                WHERE source = table_record.schemaname || '.' || table_record.tablename
                AND alert_type = 'dead_tuple_accumulation'
                AND is_active = true
                AND created_at > NOW() - INTERVAL '2 hours'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to update collection health metrics
CREATE OR REPLACE FUNCTION update_collection_health_metrics()
RETURNS void AS $$
BEGIN
    -- Update health status based on current metrics
    UPDATE operational.collection_health_monitoring 
    SET 
        health_status = CASE
            WHEN success_rate >= 0.9 AND confidence_score >= 0.8 AND gap_duration_hours < 1.0 AND consecutive_failures < 3 THEN 'healthy'
            WHEN (success_rate >= 0.5 AND success_rate < 0.9) OR (confidence_score >= 0.5 AND confidence_score < 0.8) 
                 OR (gap_duration_hours >= 1.0 AND gap_duration_hours < 4.0) OR (consecutive_failures >= 3 AND consecutive_failures < 5) THEN 'degraded'
            WHEN success_rate < 0.5 OR confidence_score < 0.5 OR gap_duration_hours >= 4.0 OR consecutive_failures >= 5 THEN 'critical'
            ELSE 'unknown'
        END,
        alert_level = CASE
            WHEN success_rate < 0.5 OR confidence_score < 0.5 OR gap_duration_hours >= 4.0 OR consecutive_failures >= 5 THEN 'critical'
            WHEN (success_rate >= 0.5 AND success_rate < 0.8) OR gap_duration_hours >= 2.0 OR consecutive_failures >= 3 THEN 'warning'
            ELSE 'normal'
        END,
        updated_at = NOW()
    WHERE updated_at < NOW() - INTERVAL '5 minutes';
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Helpful Views for Monitoring
-- =====================================================

-- Current collection health summary
CREATE OR REPLACE VIEW operational.collection_health_summary AS
SELECT 
    source,
    health_status,
    alert_level,
    success_rate,
    confidence_score,
    gap_duration_hours,
    consecutive_failures,
    last_successful_collection,
    collection_timestamp as last_check,
    records_collected as last_records_collected,
    EXTRACT(EPOCH FROM (NOW() - collection_timestamp)) / 3600 as hours_since_last_check
FROM operational.collection_health_monitoring cm1
WHERE cm1.collection_timestamp = (
    SELECT MAX(cm2.collection_timestamp) 
    FROM operational.collection_health_monitoring cm2 
    WHERE cm2.source = cm1.source
)
ORDER BY 
    CASE health_status 
        WHEN 'critical' THEN 1 
        WHEN 'degraded' THEN 2 
        WHEN 'healthy' THEN 3 
        ELSE 4 
    END,
    gap_duration_hours DESC;

-- Active alerts summary
CREATE OR REPLACE VIEW operational.active_alerts_summary AS
SELECT 
    source,
    alert_type,
    severity,
    message,
    created_at,
    EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600 as hours_active,
    is_auto_recoverable,
    metadata
FROM operational.collection_alerts
WHERE is_active = true
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1 
        WHEN 'warning' THEN 2 
        ELSE 3 
    END,
    created_at DESC;

-- Collection gaps requiring attention
CREATE OR REPLACE VIEW operational.unresolved_gaps AS
SELECT 
    source,
    gap_start,
    gap_duration_hours,
    business_impact_level,
    estimated_missing_records,
    detected_at,
    EXTRACT(EPOCH FROM (NOW() - detected_at)) / 3600 as hours_since_detection
FROM operational.collection_gaps
WHERE NOT resolved
ORDER BY 
    CASE business_impact_level 
        WHEN 'critical' THEN 1 
        WHEN 'high' THEN 2 
        WHEN 'medium' THEN 3 
        ELSE 4 
    END,
    gap_duration_hours DESC;

-- Dead tuple alerts
CREATE OR REPLACE VIEW operational.dead_tuple_alerts AS
SELECT 
    full_table_name,
    dead_tuple_ratio,
    live_tuples,
    dead_tuples,
    vacuum_recommended,
    vacuum_completed,
    monitoring_timestamp,
    EXTRACT(EPOCH FROM (NOW() - monitoring_timestamp)) / 3600 as hours_since_check
FROM operational.dead_tuple_monitoring
WHERE dead_tuple_ratio > 0.30
AND monitoring_timestamp > NOW() - INTERVAL '24 hours'
ORDER BY dead_tuple_ratio DESC;

-- =====================================================
-- Scheduled Monitoring Jobs (Comments for cron setup)
-- =====================================================

/*
Recommended cron jobs for automated monitoring:

# Run gap detection every 15 minutes
# */15 * * * * psql -d mlb_betting -c "SELECT detect_collection_gaps();"

# Run dead tuple monitoring every hour  
# 0 * * * * psql -d mlb_betting -c "SELECT monitor_dead_tuples();"

# Update health metrics every 5 minutes
# */5 * * * * psql -d mlb_betting -c "SELECT update_collection_health_metrics();"

*/

-- =====================================================
-- Initial Data and Configuration
-- =====================================================

-- Insert initial health monitoring configuration
INSERT INTO operational.collection_health_monitoring (
    source, 
    health_status, 
    alert_level,
    collection_timestamp,
    records_collected,
    success_rate,
    confidence_score
) VALUES 
    ('action_network', 'unknown', 'normal', NOW(), 0, 0.0, 1.0),
    ('vsin', 'unknown', 'normal', NOW(), 0, 0.0, 1.0),
    ('sbd', 'unknown', 'normal', NOW(), 0, 0.0, 1.0),
    ('sports_book_review', 'unknown', 'normal', NOW(), 0, 0.0, 1.0),
    ('mlb_stats_api', 'unknown', 'normal', NOW(), 0, 0.0, 1.0)
ON CONFLICT DO NOTHING;

-- Add table constraints for data integrity
ALTER TABLE operational.collection_health_monitoring 
ADD CONSTRAINT check_confidence_score_range 
CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0);

ALTER TABLE operational.collection_health_monitoring 
ADD CONSTRAINT check_success_rate_range 
CHECK (success_rate >= 0.0 AND success_rate <= 100.0);

ALTER TABLE operational.collection_health_monitoring 
ADD CONSTRAINT check_gap_duration_positive 
CHECK (gap_duration_hours >= 0.0);

ALTER TABLE operational.collection_health_monitoring 
ADD CONSTRAINT check_consecutive_failures_positive 
CHECK (consecutive_failures >= 0);

-- Add missing unique constraint to prevent duplicate dead tuple monitoring
-- Note: Using a regular date column updated by triggers for simplicity
ALTER TABLE operational.dead_tuple_monitoring 
ADD COLUMN monitoring_date DATE DEFAULT CURRENT_DATE;

-- Update existing records
UPDATE operational.dead_tuple_monitoring 
SET monitoring_date = monitoring_timestamp::date 
WHERE monitoring_date IS NULL;

-- Make it non-nullable
ALTER TABLE operational.dead_tuple_monitoring 
ALTER COLUMN monitoring_date SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_tuple_monitoring_daily 
ON operational.dead_tuple_monitoring (schema_name, table_name, monitoring_date);

COMMENT ON TABLE operational.collection_health_monitoring IS 'Comprehensive health monitoring for data collection operations - tracks success rates, confidence scores, and failure patterns';
COMMENT ON TABLE operational.collection_alerts IS 'Real-time alerts for collection issues with automated resolution tracking';
COMMENT ON TABLE operational.collection_gaps IS 'Detection and tracking of collection gaps that may indicate silent failures';
COMMENT ON TABLE operational.dead_tuple_monitoring IS 'Monitoring of database dead tuple accumulation indicating data corruption issues';
COMMENT ON TABLE operational.collection_performance_history IS 'Historical performance metrics for collection trend analysis';

-- Grant appropriate permissions
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA operational TO samlafell;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA operational TO samlafell;