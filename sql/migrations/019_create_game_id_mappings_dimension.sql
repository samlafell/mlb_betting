-- Migration: Create Game ID Mappings Dimension Table
-- Purpose: Centralize game ID mappings to eliminate thousands of API calls per pipeline run
-- Created: 2025-07-28
-- Reference: .claude/tasks/game_id_mapping_optimization.md

-- ================================
-- Phase 1: Create Central Game ID Mapping Dimension Table
-- ================================

CREATE TABLE IF NOT EXISTS staging.game_id_mappings (
    -- Surrogate key
    id BIGSERIAL PRIMARY KEY,
    
    -- MLB Stats API (authoritative source)
    mlb_stats_api_game_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- External source game IDs
    action_network_game_id VARCHAR(255),
    vsin_game_id VARCHAR(255), 
    sbd_game_id VARCHAR(255),
    sbr_game_id VARCHAR(255),
    
    -- Game identification (for validation/debugging)
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMPTZ,
    
    -- Data quality and resolution metadata
    resolution_confidence DECIMAL(3,2) DEFAULT 1.0, -- 0.0-1.0 confidence score
    primary_source VARCHAR(50), -- Which source was used for initial resolution
    last_verified_at TIMESTAMPTZ DEFAULT NOW(),
    verification_attempts INTEGER DEFAULT 0,
    
    -- Audit trail
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_confidence CHECK (resolution_confidence BETWEEN 0.0 AND 1.0),
    CONSTRAINT has_at_least_one_external_id CHECK (
        action_network_game_id IS NOT NULL OR
        vsin_game_id IS NOT NULL OR 
        sbd_game_id IS NOT NULL OR
        sbr_game_id IS NOT NULL
    ),
    CONSTRAINT valid_primary_source CHECK (
        primary_source IN ('action_network', 'vsin', 'sbd', 'sbr', 'manual')
    )
);

-- ================================
-- Phase 2: Create Performance Indexes
-- ================================

-- Primary lookup indexes (used by fact table joins)
CREATE UNIQUE INDEX IF NOT EXISTS idx_game_mappings_mlb_id 
ON staging.game_id_mappings(mlb_stats_api_game_id);

CREATE INDEX IF NOT EXISTS idx_game_mappings_action_network 
ON staging.game_id_mappings(action_network_game_id) 
WHERE action_network_game_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_mappings_vsin 
ON staging.game_id_mappings(vsin_game_id) 
WHERE vsin_game_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_mappings_sbd 
ON staging.game_id_mappings(sbd_game_id) 
WHERE sbd_game_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_game_mappings_sbr 
ON staging.game_id_mappings(sbr_game_id) 
WHERE sbr_game_id IS NOT NULL;

-- Composite indexes for pipeline operations
CREATE INDEX IF NOT EXISTS idx_game_mappings_date_teams 
ON staging.game_id_mappings(game_date, home_team, away_team);

CREATE INDEX IF NOT EXISTS idx_game_mappings_verification 
ON staging.game_id_mappings(last_verified_at, verification_attempts);

-- Multi-column index for efficient external ID lookups
CREATE INDEX IF NOT EXISTS idx_game_mappings_external_ids 
ON staging.game_id_mappings(action_network_game_id, vsin_game_id, sbd_game_id, sbr_game_id) 
WHERE (action_network_game_id IS NOT NULL OR vsin_game_id IS NOT NULL OR 
       sbd_game_id IS NOT NULL OR sbr_game_id IS NOT NULL);

-- ================================
-- Phase 3: Add Table Comments and Documentation
-- ================================

COMMENT ON TABLE staging.game_id_mappings IS 
'Central dimension table mapping external game IDs from all sources to MLB Stats API game IDs. 
Eliminates thousands of API calls per pipeline run by providing O(1) lookup performance.';

COMMENT ON COLUMN staging.game_id_mappings.mlb_stats_api_game_id IS 
'MLB Stats API official game ID - authoritative source for cross-system integration';

COMMENT ON COLUMN staging.game_id_mappings.action_network_game_id IS 
'Action Network external game ID from API responses';

COMMENT ON COLUMN staging.game_id_mappings.vsin_game_id IS 
'VSIN external game ID from betting data';

COMMENT ON COLUMN staging.game_id_mappings.sbd_game_id IS 
'SportsBettingDime external game ID from WordPress API';

COMMENT ON COLUMN staging.game_id_mappings.sbr_game_id IS 
'SportsBookReview external game ID from betting data';

COMMENT ON COLUMN staging.game_id_mappings.resolution_confidence IS 
'Confidence score (0.0-1.0) indicating reliability of the MLB API game ID mapping';

COMMENT ON COLUMN staging.game_id_mappings.primary_source IS 
'Data source used for initial resolution: action_network, vsin, sbd, sbr, or manual';

COMMENT ON COLUMN staging.game_id_mappings.last_verified_at IS 
'Timestamp of last verification that the mapping is still valid';

COMMENT ON COLUMN staging.game_id_mappings.verification_attempts IS 
'Number of times this mapping has been verified or re-validated';

-- ================================
-- Phase 4: Create Utility Functions
-- ================================

-- Function to find unmapped external IDs from raw tables
CREATE OR REPLACE FUNCTION staging.find_unmapped_external_ids(
    source_filter VARCHAR DEFAULT NULL,
    limit_results INTEGER DEFAULT 100
)
RETURNS TABLE(
    external_id VARCHAR,
    source_type VARCHAR,
    home_team VARCHAR,
    away_team VARCHAR,
    game_date DATE,
    raw_table VARCHAR
) AS $$
BEGIN
    -- Action Network unmapped IDs
    IF source_filter IS NULL OR source_filter = 'action_network' THEN
        RETURN QUERY
        SELECT 
            g.external_game_id::VARCHAR as external_id,
            'action_network'::VARCHAR as source_type,
            g.away_team::VARCHAR as home_team,
            g.home_team::VARCHAR as away_team,
            DATE(g.start_time) as game_date,
            'raw_data.action_network_games'::VARCHAR as raw_table
        FROM raw_data.action_network_games g
        WHERE g.external_game_id NOT IN (
            SELECT m.action_network_game_id 
            FROM staging.game_id_mappings m 
            WHERE m.action_network_game_id IS NOT NULL
        )
        AND g.away_team IS NOT NULL 
        AND g.home_team IS NOT NULL
        AND g.start_time IS NOT NULL
        LIMIT limit_results;
    END IF;
    
    -- Additional sources can be added here as needed
    -- VSIN, SBD, SBR tables would follow similar patterns
    
END;
$$ LANGUAGE plpgsql;

-- Function to get mapping statistics
CREATE OR REPLACE FUNCTION staging.get_game_id_mapping_stats()
RETURNS TABLE(
    total_mappings BIGINT,
    action_network_count BIGINT,
    vsin_count BIGINT,
    sbd_count BIGINT,
    sbr_count BIGINT,
    avg_confidence DECIMAL,
    last_updated TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_mappings,
        COUNT(m.action_network_game_id) as action_network_count,
        COUNT(m.vsin_game_id) as vsin_count,
        COUNT(m.sbd_game_id) as sbd_count,
        COUNT(m.sbr_game_id) as sbr_count,
        AVG(m.resolution_confidence) as avg_confidence,
        MAX(m.updated_at) as last_updated
    FROM staging.game_id_mappings m;
END;
$$ LANGUAGE plpgsql;

-- Function to validate mapping integrity
CREATE OR REPLACE FUNCTION staging.validate_game_id_mappings()
RETURNS TABLE(
    validation_type VARCHAR,
    issue_count BIGINT,
    sample_mlb_id VARCHAR
) AS $$
BEGIN
    -- Check for duplicate MLB game IDs (should not happen due to unique constraint)
    RETURN QUERY
    SELECT 
        'duplicate_mlb_ids'::VARCHAR as validation_type,
        COUNT(*) - COUNT(DISTINCT mlb_stats_api_game_id) as issue_count,
        MIN(mlb_stats_api_game_id)::VARCHAR as sample_mlb_id
    FROM staging.game_id_mappings
    HAVING COUNT(*) - COUNT(DISTINCT mlb_stats_api_game_id) > 0;
    
    -- Check for low confidence mappings
    RETURN QUERY
    SELECT 
        'low_confidence_mappings'::VARCHAR as validation_type,
        COUNT(*) as issue_count,
        MIN(mlb_stats_api_game_id)::VARCHAR as sample_mlb_id
    FROM staging.game_id_mappings
    WHERE resolution_confidence < 0.8;
    
    -- Check for old unverified mappings
    RETURN QUERY
    SELECT 
        'unverified_mappings'::VARCHAR as validation_type,
        COUNT(*) as issue_count,
        MIN(mlb_stats_api_game_id)::VARCHAR as sample_mlb_id
    FROM staging.game_id_mappings
    WHERE last_verified_at < NOW() - INTERVAL '30 days';
    
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 5: Grant Permissions
-- ================================

-- Grant permissions for utility functions
GRANT EXECUTE ON FUNCTION staging.find_unmapped_external_ids(VARCHAR, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION staging.get_game_id_mapping_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION staging.validate_game_id_mappings() TO PUBLIC;

-- ================================
-- Phase 6: Initial Validation and Summary
-- ================================

-- Display creation summary
DO $$
DECLARE
    summary_text TEXT;
BEGIN
    summary_text := E'Game ID Mappings Dimension Table Created Successfully:\n';
    summary_text := summary_text || E'- Table: staging.game_id_mappings with full constraint validation\n';
    summary_text := summary_text || E'- Indexes: 7 performance indexes for O(1) lookups\n';
    summary_text := summary_text || E'- Functions: 3 utility functions for management and validation\n';
    summary_text := summary_text || E'- Documentation: Complete column and table comments\n';
    summary_text := summary_text || E'\nNext Steps:\n';
    summary_text := summary_text || E'1. Run data population script to migrate existing mappings\n';
    summary_text := summary_text || E'2. Update staging processors to use JOINs instead of API calls\n';
    summary_text := summary_text || E'3. Implement GameIDMappingService for automated resolution\n';
    
    RAISE NOTICE '%', summary_text;
END $$;

-- Show table structure
\d staging.game_id_mappings

-- Show created indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'game_id_mappings'
AND schemaname = 'staging'
ORDER BY indexname;

-- Initial stats (should be empty)
SELECT * FROM staging.get_game_id_mapping_stats();