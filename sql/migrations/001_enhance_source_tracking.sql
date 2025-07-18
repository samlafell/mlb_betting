-- =============================================================================
-- Migration 001: Enhanced Source Tracking for Betting Lines
-- =============================================================================
-- Purpose: Add comprehensive source attribution and metadata tracking
-- to all betting lines tables to enable unified source identification
-- 
-- Author: Claude Code
-- Date: 2025-07-15
-- Dependencies: consolidated_schema.sql
-- =============================================================================

BEGIN;

-- Add source tracking columns to moneyline table
ALTER TABLE core_betting.betting_lines_moneyline 
ADD COLUMN IF NOT EXISTS source_metadata JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS collection_batch_id UUID,
ADD COLUMN IF NOT EXISTS source_reliability_score DECIMAL(3,2) DEFAULT 0.95,
ADD COLUMN IF NOT EXISTS collection_method VARCHAR(20) DEFAULT 'API',
ADD COLUMN IF NOT EXISTS external_source_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS source_api_version VARCHAR(10);

-- Add source tracking columns to spreads table  
ALTER TABLE core_betting.betting_lines_spreads
ADD COLUMN IF NOT EXISTS source_metadata JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS collection_batch_id UUID,
ADD COLUMN IF NOT EXISTS source_reliability_score DECIMAL(3,2) DEFAULT 0.95,
ADD COLUMN IF NOT EXISTS collection_method VARCHAR(20) DEFAULT 'API',
ADD COLUMN IF NOT EXISTS external_source_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS source_api_version VARCHAR(10);

-- Add source tracking columns to totals table
ALTER TABLE core_betting.betting_lines_totals
ADD COLUMN IF NOT EXISTS source_metadata JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS collection_batch_id UUID, 
ADD COLUMN IF NOT EXISTS source_reliability_score DECIMAL(3,2) DEFAULT 0.95,
ADD COLUMN IF NOT EXISTS collection_method VARCHAR(20) DEFAULT 'API',
ADD COLUMN IF NOT EXISTS external_source_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS source_api_version VARCHAR(10);

-- Create data source metadata tracking table
CREATE TABLE IF NOT EXISTS core_betting.data_source_metadata (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(30) NOT NULL UNIQUE,
    source_version VARCHAR(10),
    api_endpoint VARCHAR(200),
    rate_limit_per_hour INTEGER,
    reliability_score DECIMAL(3,2) DEFAULT 0.95,
    last_successful_collection TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'ACTIVE', -- 'ACTIVE', 'DEPRECATED', 'MAINTENANCE'
    configuration JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT valid_reliability_score CHECK (reliability_score >= 0.0 AND reliability_score <= 1.0),
    CONSTRAINT valid_status CHECK (status IN ('ACTIVE', 'DEPRECATED', 'MAINTENANCE', 'DISABLED'))
);

-- Insert known data sources with their reliability scores
INSERT INTO core_betting.data_source_metadata (source_name, reliability_score, configuration) VALUES
('VSIN', 0.92, '{"description": "Vegas Stats & Information Network", "data_types": ["sharp_action", "line_movement"]}'),
('SBD', 0.88, '{"description": "Sports Betting Dime", "data_types": ["current_odds", "line_history"]}'), 
('ACTION_NETWORK', 0.96, '{"description": "Action Network API", "data_types": ["comprehensive_odds", "public_betting"]}'),
('SPORTS_BOOK_REVIEW_DEPRECATED', 0.85, '{"description": "Sports Book Review (SBR) - deprecated source", "data_types": ["consensus", "trends"], "replacement": "SBRUnifiedCollector", "status": "deprecated"}'),
('MLB_STATS_API', 0.98, '{"description": "Official MLB Stats API", "data_types": ["game_data", "official_results"]}'),
('ODDS_API', 0.87, '{"description": "The Odds API", "data_types": ["live_odds", "historical_odds"]}')
ON CONFLICT (source_name) DO UPDATE SET
    reliability_score = EXCLUDED.reliability_score,
    configuration = EXCLUDED.configuration,
    updated_at = NOW();

-- Create standardized source type enum
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'data_source_type') THEN
        CREATE TYPE data_source_type AS ENUM (
            'VSIN',
            'SBD', 
            'ACTION_NETWORK',
            'SPORTS_BOOK_REVIEW_DEPRECATED',
            'MLB_STATS_API',
            'ODDS_API',
            'MANUAL_ENTRY',
            'UNKNOWN'
        );
    END IF;
END $$;

-- Temporarily drop views that depend on source columns
DROP VIEW IF EXISTS core_betting.data_source_quality_analysis CASCADE;
DROP VIEW IF EXISTS core_betting.sportsbook_mapping_status CASCADE;
DROP VIEW IF EXISTS core_betting.unmapped_sportsbook_analysis CASCADE;

-- Update existing source columns to use enum (if they exist)
DO $$
BEGIN
    -- Check if source column exists and update its type for moneyline
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core_betting' 
        AND table_name = 'betting_lines_moneyline' 
        AND column_name = 'source'
    ) THEN
        -- Remove the default constraint first, then alter type, then add new default
        ALTER TABLE core_betting.betting_lines_moneyline 
        ALTER COLUMN source DROP DEFAULT;
        
        ALTER TABLE core_betting.betting_lines_moneyline 
        ALTER COLUMN source TYPE data_source_type 
        USING CASE 
            WHEN UPPER(source) = 'SPORTSBOOKREVIEW' THEN 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type
            WHEN UPPER(source) = 'VSIN' THEN 'VSIN'::data_source_type
            WHEN UPPER(source) = 'SBD' THEN 'SBD'::data_source_type
            WHEN UPPER(source) = 'ACTION_NETWORK' THEN 'ACTION_NETWORK'::data_source_type
            WHEN UPPER(source) = 'MLB_STATS_API' THEN 'MLB_STATS_API'::data_source_type
            WHEN UPPER(source) = 'ODDS_API' THEN 'ODDS_API'::data_source_type
            ELSE 'UNKNOWN'::data_source_type
        END;
        
        ALTER TABLE core_betting.betting_lines_moneyline 
        ALTER COLUMN source SET DEFAULT 'UNKNOWN'::data_source_type;
    ELSE
        ALTER TABLE core_betting.betting_lines_moneyline 
        ADD COLUMN source data_source_type DEFAULT 'UNKNOWN';
    END IF;
    
    -- Check if source column exists and update its type for spreads
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core_betting' 
        AND table_name = 'betting_lines_spreads' 
        AND column_name = 'source'
    ) THEN
        ALTER TABLE core_betting.betting_lines_spreads 
        ALTER COLUMN source DROP DEFAULT;
        
        ALTER TABLE core_betting.betting_lines_spreads 
        ALTER COLUMN source TYPE data_source_type 
        USING CASE 
            WHEN UPPER(source) = 'SPORTSBOOKREVIEW' THEN 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type
            WHEN UPPER(source) = 'VSIN' THEN 'VSIN'::data_source_type
            WHEN UPPER(source) = 'SBD' THEN 'SBD'::data_source_type
            WHEN UPPER(source) = 'ACTION_NETWORK' THEN 'ACTION_NETWORK'::data_source_type
            WHEN UPPER(source) = 'MLB_STATS_API' THEN 'MLB_STATS_API'::data_source_type
            WHEN UPPER(source) = 'ODDS_API' THEN 'ODDS_API'::data_source_type
            ELSE 'UNKNOWN'::data_source_type
        END;
        
        ALTER TABLE core_betting.betting_lines_spreads 
        ALTER COLUMN source SET DEFAULT 'UNKNOWN'::data_source_type;
    ELSE
        ALTER TABLE core_betting.betting_lines_spreads 
        ADD COLUMN source data_source_type DEFAULT 'UNKNOWN';
    END IF;
    
    -- Check if source column exists and update its type for totals
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core_betting' 
        AND table_name = 'betting_lines_totals' 
        AND column_name = 'source'
    ) THEN
        ALTER TABLE core_betting.betting_lines_totals 
        ALTER COLUMN source DROP DEFAULT;
        
        ALTER TABLE core_betting.betting_lines_totals 
        ALTER COLUMN source TYPE data_source_type 
        USING CASE 
            WHEN UPPER(source) = 'SPORTSBOOKREVIEW' THEN 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type
            WHEN UPPER(source) = 'VSIN' THEN 'VSIN'::data_source_type
            WHEN UPPER(source) = 'SBD' THEN 'SBD'::data_source_type
            WHEN UPPER(source) = 'ACTION_NETWORK' THEN 'ACTION_NETWORK'::data_source_type
            WHEN UPPER(source) = 'MLB_STATS_API' THEN 'MLB_STATS_API'::data_source_type
            WHEN UPPER(source) = 'ODDS_API' THEN 'ODDS_API'::data_source_type
            ELSE 'UNKNOWN'::data_source_type
        END;
        
        ALTER TABLE core_betting.betting_lines_totals 
        ALTER COLUMN source SET DEFAULT 'UNKNOWN'::data_source_type;
    ELSE
        ALTER TABLE core_betting.betting_lines_totals 
        ADD COLUMN source data_source_type DEFAULT 'UNKNOWN';
    END IF;
END $$;

-- Create indexes for efficient source-based queries
CREATE INDEX IF NOT EXISTS idx_moneyline_source_game 
ON core_betting.betting_lines_moneyline (source, game_id, odds_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_spreads_source_game 
ON core_betting.betting_lines_spreads (source, game_id, odds_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_totals_source_game 
ON core_betting.betting_lines_totals (source, game_id, odds_timestamp DESC);

-- Create indexes for collection batch tracking
CREATE INDEX IF NOT EXISTS idx_moneyline_batch 
ON core_betting.betting_lines_moneyline (collection_batch_id) 
WHERE collection_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_spreads_batch 
ON core_betting.betting_lines_spreads (collection_batch_id) 
WHERE collection_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_totals_batch 
ON core_betting.betting_lines_totals (collection_batch_id) 
WHERE collection_batch_id IS NOT NULL;

-- Create function to update data source metadata
CREATE OR REPLACE FUNCTION core_betting.update_source_last_collection(
    p_source_name VARCHAR(30)
) RETURNS VOID AS $$
BEGIN
    UPDATE core_betting.data_source_metadata 
    SET last_successful_collection = NOW(),
        updated_at = NOW()
    WHERE source_name = p_source_name;
    
    IF NOT FOUND THEN
        INSERT INTO core_betting.data_source_metadata (source_name, last_successful_collection)
        VALUES (p_source_name, NOW());
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create function to validate source attribution completeness
CREATE OR REPLACE FUNCTION core_betting.validate_source_attribution()
RETURNS TABLE(
    table_name VARCHAR(50), 
    total_records BIGINT,
    missing_source_count BIGINT,
    completion_percentage DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'betting_lines_moneyline'::VARCHAR(50), 
        COUNT(*)::BIGINT as total,
        COUNT(*) FILTER (WHERE source IS NULL OR source = 'UNKNOWN')::BIGINT as missing,
        ROUND(
            (COUNT(*) FILTER (WHERE source IS NOT NULL AND source != 'UNKNOWN')::DECIMAL / COUNT(*)::DECIMAL) * 100, 
            2
        ) as completion_pct
    FROM core_betting.betting_lines_moneyline;
    
    RETURN QUERY
    SELECT 
        'betting_lines_spreads'::VARCHAR(50), 
        COUNT(*)::BIGINT as total,
        COUNT(*) FILTER (WHERE source IS NULL OR source = 'UNKNOWN')::BIGINT as missing,
        ROUND(
            (COUNT(*) FILTER (WHERE source IS NOT NULL AND source != 'UNKNOWN')::DECIMAL / COUNT(*)::DECIMAL) * 100, 
            2
        ) as completion_pct
    FROM core_betting.betting_lines_spreads;
    
    RETURN QUERY
    SELECT 
        'betting_lines_totals'::VARCHAR(50), 
        COUNT(*)::BIGINT as total,
        COUNT(*) FILTER (WHERE source IS NULL OR source = 'UNKNOWN')::BIGINT as missing,
        ROUND(
            (COUNT(*) FILTER (WHERE source IS NOT NULL AND source != 'UNKNOWN')::DECIMAL / COUNT(*)::DECIMAL) * 100, 
            2
        ) as completion_pct
    FROM core_betting.betting_lines_totals;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function to automatically update source reliability
CREATE OR REPLACE FUNCTION core_betting.update_source_reliability()
RETURNS TRIGGER AS $$
BEGIN
    -- Update last successful collection timestamp
    PERFORM core_betting.update_source_last_collection(NEW.source::VARCHAR);
    
    -- Add collection batch metadata if provided
    IF NEW.collection_batch_id IS NOT NULL THEN
        NEW.source_metadata = COALESCE(NEW.source_metadata, '{}'::jsonb) || 
                              jsonb_build_object('batch_processed_at', NOW());
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach triggers to all betting lines tables
DROP TRIGGER IF EXISTS tr_moneyline_source_update ON core_betting.betting_lines_moneyline;
CREATE TRIGGER tr_moneyline_source_update
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_moneyline
    FOR EACH ROW EXECUTE FUNCTION core_betting.update_source_reliability();

DROP TRIGGER IF EXISTS tr_spreads_source_update ON core_betting.betting_lines_spreads;
CREATE TRIGGER tr_spreads_source_update
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_spreads
    FOR EACH ROW EXECUTE FUNCTION core_betting.update_source_reliability();

DROP TRIGGER IF EXISTS tr_totals_source_update ON core_betting.betting_lines_totals;
CREATE TRIGGER tr_totals_source_update
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_totals
    FOR EACH ROW EXECUTE FUNCTION core_betting.update_source_reliability();

-- Add comments for documentation
COMMENT ON TABLE core_betting.data_source_metadata IS 'Tracks metadata and reliability metrics for all data sources feeding betting lines';
COMMENT ON COLUMN core_betting.data_source_metadata.reliability_score IS 'Score from 0.0 to 1.0 indicating historical accuracy and completeness of the source';
COMMENT ON COLUMN core_betting.betting_lines_moneyline.source_metadata IS 'JSON metadata about the collection process, API version, and data quality indicators';
COMMENT ON COLUMN core_betting.betting_lines_moneyline.collection_batch_id IS 'UUID linking related records collected in the same batch operation';

-- Recreate the data source quality analysis view with updated source enum
CREATE OR REPLACE VIEW core_betting.data_source_quality_analysis AS
SELECT 
    source,
    COUNT(*) AS total_records,
    ROUND(AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS sportsbook_mapping_success_pct,
    ROUND(AVG(data_completeness_score), 3) AS avg_completeness,
    COUNT(DISTINCT sportsbook) AS distinct_sportsbooks_found,
    COUNT(DISTINCT CASE WHEN sportsbook_id IS NOT NULL THEN sportsbook_id ELSE NULL END) AS distinct_sportsbooks_mapped,
    MIN(created_at) AS first_record,
    MAX(created_at) AS latest_record
FROM (
    SELECT source::VARCHAR, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_moneyline
    UNION ALL
    SELECT source::VARCHAR, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_spreads
    UNION ALL
    SELECT source::VARCHAR, sportsbook_id, sportsbook, data_completeness_score, created_at
    FROM core_betting.betting_lines_totals
) AS combined_sources
GROUP BY source
ORDER BY avg_completeness DESC;

COMMENT ON VIEW core_betting.data_source_quality_analysis IS 'Data quality analysis by source with sportsbook mapping success rates and completeness scores';

-- Recreate sportsbook mapping status view
CREATE OR REPLACE VIEW core_betting.sportsbook_mapping_status AS
SELECT 'moneyline'::text AS table_name,
    COUNT(*) AS total_rows,
    COUNT(sportsbook_id) AS mapped_rows,
    ROUND(COUNT(sportsbook_id)::numeric * 100.0 / COUNT(*)::numeric, 2) AS mapping_percentage,
    COUNT(DISTINCT sportsbook_id) AS unique_sportsbooks,
    COUNT(DISTINCT source) AS data_sources
FROM core_betting.betting_lines_moneyline
UNION ALL
SELECT 'spreads'::text AS table_name,
    COUNT(*) AS total_rows,
    COUNT(sportsbook_id) AS mapped_rows,
    ROUND(COUNT(sportsbook_id)::numeric * 100.0 / COUNT(*)::numeric, 2) AS mapping_percentage,
    COUNT(DISTINCT sportsbook_id) AS unique_sportsbooks,
    COUNT(DISTINCT source) AS data_sources
FROM core_betting.betting_lines_spreads
UNION ALL
SELECT 'totals'::text AS table_name,
    COUNT(*) AS total_rows,
    COUNT(sportsbook_id) AS mapped_rows,
    ROUND(COUNT(sportsbook_id)::numeric * 100.0 / COUNT(*)::numeric, 2) AS mapping_percentage,
    COUNT(DISTINCT sportsbook_id) AS unique_sportsbooks,
    COUNT(DISTINCT source) AS data_sources
FROM core_betting.betting_lines_totals;

-- Recreate unmapped sportsbook analysis view
CREATE OR REPLACE VIEW core_betting.unmapped_sportsbook_analysis AS
SELECT 
    source::VARCHAR,
    sportsbook,
    COUNT(*) AS occurrence_count,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen
FROM (
    SELECT source, sportsbook, created_at
    FROM core_betting.betting_lines_moneyline
    WHERE sportsbook_id IS NULL
    UNION ALL
    SELECT source, sportsbook, created_at
    FROM core_betting.betting_lines_spreads
    WHERE sportsbook_id IS NULL
    UNION ALL
    SELECT source, sportsbook, created_at
    FROM core_betting.betting_lines_totals
    WHERE sportsbook_id IS NULL
) AS unmapped
WHERE sportsbook IS NOT NULL
GROUP BY source, sportsbook
ORDER BY COUNT(*) DESC;

COMMENT ON VIEW core_betting.sportsbook_mapping_status IS 'Status of sportsbook ID mapping across all betting lines tables';
COMMENT ON VIEW core_betting.unmapped_sportsbook_analysis IS 'Analysis of unmapped sportsbooks by source for data quality monitoring';

COMMIT;

-- Validation query to check migration success
SELECT 'Migration 001 completed successfully' as status;
SELECT * FROM core_betting.validate_source_attribution();