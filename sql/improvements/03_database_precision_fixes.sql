-- Database Precision Constraint Fixes
-- Purpose: Resolve numeric field overflow errors in STAGING zone migration
-- Issue: DECIMAL(3,2) constraint limits quality scores to 9.99, causing migration failures
-- Solution: Expand precision to DECIMAL(5,2) allowing scores up to 999.99
-- Created: 2025-07-21

-- ================================
-- Fix Quality Score Precision Across All STAGING Tables
-- ================================

BEGIN;

-- 1. Expand precision for staging.moneylines
ALTER TABLE staging.moneylines 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

-- Add validation constraint to ensure reasonable ranges
ALTER TABLE staging.moneylines 
ADD CONSTRAINT chk_moneylines_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 100.00);

-- 2. Expand precision for staging.spreads
ALTER TABLE staging.spreads 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.spreads 
ADD CONSTRAINT chk_spreads_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 100.00);

-- 3. Expand precision for staging.totals
ALTER TABLE staging.totals 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.totals 
ADD CONSTRAINT chk_totals_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 100.00);

-- 4. Expand precision for staging.games
ALTER TABLE staging.games 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.games 
ADD CONSTRAINT chk_games_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 100.00);

-- 5. Update other quality-related tables
ALTER TABLE staging.data_quality_metrics 
ALTER COLUMN quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.data_quality_metrics 
ALTER COLUMN completeness_score TYPE DECIMAL(5,2);

ALTER TABLE staging.data_quality_metrics 
ALTER COLUMN accuracy_score TYPE DECIMAL(5,2);

ALTER TABLE staging.data_quality_metrics 
ALTER COLUMN consistency_score TYPE DECIMAL(5,2);

-- Add constraints for data_quality_metrics
ALTER TABLE staging.data_quality_metrics 
ADD CONSTRAINT chk_quality_metrics_scores 
CHECK (
    quality_score >= 0.00 AND quality_score <= 100.00 AND
    completeness_score >= 0.00 AND completeness_score <= 100.00 AND
    accuracy_score >= 0.00 AND accuracy_score <= 100.00 AND
    consistency_score >= 0.00 AND consistency_score <= 100.00
);

COMMIT;

-- ================================
-- Performance Indexes for Migration Optimization
-- ================================

BEGIN;

-- Composite indexes to optimize STAGING migration queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_games_external_validation 
ON staging.games (external_id, validation_status, data_quality_score);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_moneylines_game_quality_date 
ON staging.moneylines (game_id, data_quality_score, processed_at) 
WHERE validation_status = 'validated';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_spreads_sportsbook_date 
ON staging.spreads (sportsbook_name, processed_at, data_quality_score);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_totals_game_sportsbook 
ON staging.totals (game_id, sportsbook_name, validation_status);

-- Optimize RAW to STAGING lookup queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_moneylines_game_external_collected 
ON raw_data.moneylines_raw (game_external_id, collected_at) 
WHERE game_external_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_spreads_game_sportsbook 
ON raw_data.spreads_raw (game_external_id, sportsbook_name, collected_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_totals_game_date_source 
ON raw_data.totals_raw (game_external_id, game_date, source);

-- Indexes for quality score filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_moneylines_quality_validation 
ON staging.moneylines (data_quality_score DESC, validation_status) 
WHERE data_quality_score >= 80.00;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_spreads_quality_validation 
ON staging.spreads (data_quality_score DESC, validation_status) 
WHERE data_quality_score >= 80.00;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_totals_quality_validation 
ON staging.totals (data_quality_score DESC, validation_status) 
WHERE data_quality_score >= 80.00;

COMMIT;

-- ================================
-- Add Migration Performance Monitoring
-- ================================

-- Create view for monitoring data quality distribution
CREATE OR REPLACE VIEW staging.quality_score_distribution AS
SELECT 
    'moneylines' as table_name,
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY data_quality_score) as median_quality_score,
    COUNT(CASE WHEN data_quality_score >= 90.00 THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality_score BETWEEN 70.00 AND 89.99 THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality_score < 70.00 THEN 1 END) as low_quality_count
FROM staging.moneylines

UNION ALL

SELECT 
    'spreads' as table_name,
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY data_quality_score) as median_quality_score,
    COUNT(CASE WHEN data_quality_score >= 90.00 THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality_score BETWEEN 70.00 AND 89.99 THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality_score < 70.00 THEN 1 END) as low_quality_count
FROM staging.spreads

UNION ALL

SELECT 
    'totals' as table_name,
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY data_quality_score) as median_quality_score,
    COUNT(CASE WHEN data_quality_score >= 90.00 THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality_score BETWEEN 70.00 AND 89.99 THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality_score < 70.00 THEN 1 END) as low_quality_count
FROM staging.totals

UNION ALL

SELECT 
    'games' as table_name,
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY data_quality_score) as median_quality_score,
    COUNT(CASE WHEN data_quality_score >= 90.00 THEN 1 END) as high_quality_count,
    COUNT(CASE WHEN data_quality_score BETWEEN 70.00 AND 89.99 THEN 1 END) as medium_quality_count,
    COUNT(CASE WHEN data_quality_score < 70.00 THEN 1 END) as low_quality_count
FROM staging.games;

-- Create view for migration failure analysis
CREATE OR REPLACE VIEW staging.migration_failure_analysis AS
WITH failed_records AS (
    SELECT 
        'totals' as table_name,
        COUNT(*) as failed_count,
        AVG(data_quality_score) as avg_failed_quality_score,
        array_agg(DISTINCT validation_status) as failure_statuses
    FROM staging.totals 
    WHERE validation_status IN ('needs_review', 'invalid')
    
    UNION ALL
    
    SELECT 
        'moneylines' as table_name,
        COUNT(*) as failed_count,
        AVG(data_quality_score) as avg_failed_quality_score,
        array_agg(DISTINCT validation_status) as failure_statuses
    FROM staging.moneylines 
    WHERE validation_status IN ('needs_review', 'invalid')
    
    UNION ALL
    
    SELECT 
        'spreads' as table_name,
        COUNT(*) as failed_count,
        AVG(data_quality_score) as avg_failed_quality_score,
        array_agg(DISTINCT validation_status) as failure_statuses
    FROM staging.spreads 
    WHERE validation_status IN ('needs_review', 'invalid')
)
SELECT * FROM failed_records WHERE failed_count > 0;

-- ================================
-- Validation and Testing
-- ================================

-- Test that quality scores can now handle values up to 100.00
DO $$
DECLARE
    test_result RECORD;
BEGIN
    -- Test inserting a high quality score
    INSERT INTO staging.games (external_id, data_quality_score, validation_status)
    VALUES ('test_precision_fix', 99.99, 'validated');
    
    -- Verify it was inserted correctly
    SELECT data_quality_score INTO test_result 
    FROM staging.games 
    WHERE external_id = 'test_precision_fix';
    
    IF test_result.data_quality_score = 99.99 THEN
        RAISE NOTICE 'SUCCESS: Precision fix working correctly - inserted quality score: %', test_result.data_quality_score;
    ELSE
        RAISE EXCEPTION 'FAILED: Precision fix not working - expected 99.99, got %', test_result.data_quality_score;
    END IF;
    
    -- Clean up test record
    DELETE FROM staging.games WHERE external_id = 'test_precision_fix';
    
    RAISE NOTICE 'Database precision constraint fixes completed successfully!';
END $$;

-- ================================
-- Documentation and Comments
-- ================================

COMMENT ON CONSTRAINT chk_moneylines_quality_score ON staging.moneylines IS 
'Ensures quality scores are between 0.00 and 100.00 (expanded from 9.99 limit)';

COMMENT ON CONSTRAINT chk_spreads_quality_score ON staging.spreads IS 
'Ensures quality scores are between 0.00 and 100.00 (expanded from 9.99 limit)';

COMMENT ON CONSTRAINT chk_totals_quality_score ON staging.totals IS 
'Ensures quality scores are between 0.00 and 100.00 (expanded from 9.99 limit)';

COMMENT ON CONSTRAINT chk_games_quality_score ON staging.games IS 
'Ensures quality scores are between 0.00 and 100.00 (expanded from 9.99 limit)';

COMMENT ON VIEW staging.quality_score_distribution IS 
'Monitoring view for data quality score distribution across STAGING tables';

COMMENT ON VIEW staging.migration_failure_analysis IS 
'Analysis view for identifying patterns in migration failures';