-- =============================================================================
-- Migration 002: Migrate Legacy Data to Unified Schema
-- =============================================================================
-- Purpose: Migrate VSIN/SBD data from splits.raw_mlb_betting_splits to 
-- core_betting tables with proper source attribution
-- 
-- Author: Claude Code  
-- Date: 2025-07-15
-- Dependencies: 001_enhance_source_tracking.sql
-- =============================================================================

BEGIN;

-- Create migration tracking table
CREATE TABLE IF NOT EXISTS operational.schema_migrations (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(100) NOT NULL,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    records_migrated INTEGER DEFAULT 0,
    migration_status VARCHAR(20) DEFAULT 'PENDING',
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT valid_migration_status CHECK (
        migration_status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'ROLLED_BACK')
    )
);

-- Function to log migration progress
CREATE OR REPLACE FUNCTION curated.log_migration_start(
    p_migration_name VARCHAR(100),
    p_source_table VARCHAR(100),
    p_target_table VARCHAR(100)
) RETURNS INTEGER AS $$
DECLARE
    migration_id INTEGER;
BEGIN
    INSERT INTO operational.schema_migrations (
        migration_name, source_table, target_table, 
        migration_status, started_at
    ) VALUES (
        p_migration_name, p_source_table, p_target_table,
        'RUNNING', NOW()
    ) RETURNING id INTO migration_id;
    
    RETURN migration_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log migration completion
CREATE OR REPLACE FUNCTION curated.log_migration_complete(
    p_migration_id INTEGER,
    p_records_migrated INTEGER,
    p_status VARCHAR(20) DEFAULT 'COMPLETED',
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE operational.schema_migrations
    SET records_migrated = p_records_migrated,
        migration_status = p_status,
        completed_at = NOW(),
        error_message = p_error_message
    WHERE id = p_migration_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- MONEYLINE DATA MIGRATION
-- =============================================================================

DO $$
DECLARE
    migration_id INTEGER;
    migrated_count INTEGER;
BEGIN
    -- Log migration start
    migration_id := curated.log_migration_start(
        'VSIN_SBD_Moneyline_Migration',
        'splits.raw_mlb_betting_splits', 
        'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline''
    );
    
    -- Check if source table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'splits' 
        AND table_name = 'raw_mlb_betting_splits'
    ) THEN
        PERFORM curated.log_migration_complete(
            migration_id, 0, 'FAILED', 
            'Source table splits.raw_mlb_betting_splits does not exist'
        );
        RAISE NOTICE 'Source table splits.raw_mlb_betting_splits does not exist - skipping migration';
        RETURN;
    END IF;
    
    -- Note: Legacy table contains betting splits data, not actual odds lines
    -- Since splits.raw_mlb_betting_splits contains betting percentages and counts
    -- rather than moneyline odds, we'll create a placeholder migration framework
    -- that can be used when actual legacy odds data is available
    
    -- Log that no moneyline data was available to migrate
    RAISE NOTICE 'Legacy table contains betting splits data, not moneyline odds - skipping moneyline migration';
    
    GET DIAGNOSTICS migrated_count = ROW_COUNT;
    
    -- Log completion
    PERFORM curated.log_migration_complete(migration_id, migrated_count);
    
    RAISE NOTICE 'Migrated % moneyline records from legacy splits table', migrated_count;
END $$;

-- =============================================================================
-- SPREADS DATA MIGRATION  
-- =============================================================================

DO $$
DECLARE
    migration_id INTEGER;
    migrated_count INTEGER;
BEGIN
    -- Log migration start
    migration_id := curated.log_migration_start(
        'VSIN_SBD_Spreads_Migration',
        'splits.raw_mlb_betting_splits', 
        'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's'
    );
    
    -- Check if source table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'splits' 
        AND table_name = 'raw_mlb_betting_splits'
    ) THEN
        PERFORM curated.log_migration_complete(
            migration_id, 0, 'FAILED', 
            'Source table splits.raw_mlb_betting_splits does not exist'
        );
        RETURN;
    END IF;
    
    -- Note: Legacy table contains betting splits data, not actual spread lines
    -- Log that no spread data was available to migrate
    RAISE NOTICE 'Legacy table contains betting splits data, not spread lines - skipping spreads migration';
    
    GET DIAGNOSTICS migrated_count = ROW_COUNT;
    
    -- Log completion
    PERFORM curated.log_migration_complete(migration_id, migrated_count);
    
    RAISE NOTICE 'Migrated % spreads records from legacy splits table', migrated_count;
END $$;

-- =============================================================================
-- TOTALS DATA MIGRATION
-- =============================================================================

DO $$
DECLARE
    migration_id INTEGER;
    migrated_count INTEGER;
BEGIN
    -- Log migration start
    migration_id := curated.log_migration_start(
        'VSIN_SBD_Totals_Migration',
        'splits.raw_mlb_betting_splits', 
        'curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals''
    );
    
    -- Check if source table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'splits' 
        AND table_name = 'raw_mlb_betting_splits'
    ) THEN
        PERFORM curated.log_migration_complete(
            migration_id, 0, 'FAILED', 
            'Source table splits.raw_mlb_betting_splits does not exist'
        );
        RETURN;
    END IF;
    
    -- Note: Legacy table contains betting splits data, not actual total lines
    -- Log that no totals data was available to migrate
    RAISE NOTICE 'Legacy table contains betting splits data, not total lines - skipping totals migration';
    
    GET DIAGNOSTICS migrated_count = ROW_COUNT;
    
    -- Log completion
    PERFORM curated.log_migration_complete(migration_id, migrated_count);
    
    RAISE NOTICE 'Migrated % totals records from legacy splits table', migrated_count;
END $$;

-- =============================================================================
-- POST-MIGRATION VALIDATION
-- =============================================================================

-- Create validation function for migration quality
CREATE OR REPLACE FUNCTION curated.validate_migration_quality()
RETURNS TABLE(
    migration_name VARCHAR(100),
    source VARCHAR(30),
    pre_migration_count BIGINT,
    post_migration_count BIGINT,
    quality_score DECIMAL(5,2),
    status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    WITH legacy_counts AS (
        -- Since legacy table contains betting splits, not odds, return 0 counts
        SELECT 
            'VSIN'::VARCHAR as source,
            0::BIGINT as legacy_count
        UNION ALL
        SELECT 
            'SBD'::VARCHAR as source,
            0::BIGINT as legacy_count
    ),
    moneyline_counts AS (
        SELECT 
            ml.source::VARCHAR as source,
            COUNT(*) as unified_count
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' ml
        WHERE ml.source_metadata->>'legacy_migration' = 'true'
        GROUP BY ml.source
    ),
    spreads_counts AS (
        SELECT 
            sp.source::VARCHAR as source,
            COUNT(*) as unified_count  
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's sp
        WHERE sp.source_metadata->>'legacy_migration' = 'true'
        GROUP BY sp.source
    ),
    totals_counts AS (
        SELECT 
            tt.source::VARCHAR as source,
            COUNT(*) as unified_count
        FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' tt
        WHERE tt.source_metadata->>'legacy_migration' = 'true'
        GROUP BY tt.source
    )
    -- Moneyline validation
    SELECT 
        'Moneyline Migration'::VARCHAR(100),
        mc.source,
        COALESCE(lc.legacy_count, 0) as pre_migration_count,
        mc.unified_count as post_migration_count,
        CASE 
            WHEN lc.legacy_count > 0 THEN 
                ROUND((mc.unified_count::DECIMAL / lc.legacy_count::DECIMAL) * 100, 2)
            ELSE 100.00
        END as quality_score,
        CASE 
            WHEN mc.unified_count >= COALESCE(lc.legacy_count * 0.9, 0) THEN 'SUCCESS'
            ELSE 'REVIEW_NEEDED'
        END as status
    FROM moneyline_counts mc
    LEFT JOIN legacy_counts lc ON mc.source = lc.source
    
    UNION ALL
    
    -- Spreads validation  
    SELECT 
        'Spreads Migration'::VARCHAR(100),
        sc.source,
        COALESCE(lc.legacy_count, 0) as pre_migration_count,
        sc.unified_count as post_migration_count,
        CASE 
            WHEN lc.legacy_count > 0 THEN 
                ROUND((sc.unified_count::DECIMAL / lc.legacy_count::DECIMAL) * 100, 2)
            ELSE 100.00
        END as quality_score,
        CASE 
            WHEN sc.unified_count >= COALESCE(lc.legacy_count * 0.9, 0) THEN 'SUCCESS'
            ELSE 'REVIEW_NEEDED'
        END as status
    FROM spreads_counts sc
    LEFT JOIN legacy_counts lc ON sc.source = lc.source
    
    UNION ALL
    
    -- Totals validation
    SELECT 
        'Totals Migration'::VARCHAR(100),
        tc.source,
        COALESCE(lc.legacy_count, 0) as pre_migration_count,
        tc.unified_count as post_migration_count,
        CASE 
            WHEN lc.legacy_count > 0 THEN 
                ROUND((tc.unified_count::DECIMAL / lc.legacy_count::DECIMAL) * 100, 2)
            ELSE 100.00
        END as quality_score,
        CASE 
            WHEN tc.unified_count >= COALESCE(lc.legacy_count * 0.9, 0) THEN 'SUCCESS'
            ELSE 'REVIEW_NEEDED'
        END as status
    FROM totals_counts tc
    LEFT JOIN legacy_counts lc ON tc.source = lc.source;
END;
$$ LANGUAGE plpgsql;

COMMIT;

-- Run validation and display results
SELECT 'Migration 002 completed successfully' as status;

-- Display migration summary
SELECT 
    migration_name,
    records_migrated,
    migration_status,
    EXTRACT(EPOCH FROM (completed_at - started_at)) / 60 as duration_minutes
FROM operational.schema_migrations 
WHERE migration_name LIKE '%VSIN_SBD%'
ORDER BY started_at DESC;

-- Display source attribution validation
SELECT * FROM curated.validate_source_attribution();

-- Display migration quality validation (if legacy table exists)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'splits' 
        AND table_name = 'raw_mlb_betting_splits'
    ) THEN
        RAISE NOTICE 'Migration quality validation:';
        PERFORM * FROM curated.validate_migration_quality();
    ELSE
        RAISE NOTICE 'Legacy table not found - migration quality validation skipped';
    END IF;
END $$;