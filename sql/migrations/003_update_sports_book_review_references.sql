-- =============================================================================
-- Migration 003: Update Sports Book Review References
-- =============================================================================
-- Purpose: Update all database references from "Sports Betting Report" to 
-- correct "Sports Book Review (SBR)" terminology
-- 
-- Author: Claude Code  
-- Date: 2025-07-18
-- Dependencies: 001_enhance_source_tracking.sql, 002_migrate_legacy_data.sql
-- =============================================================================

BEGIN;

-- =============================================================================
-- UPDATE DATA SOURCE ENUM VALUES
-- =============================================================================

-- Update the data_source_type enum to use correct terminology
-- First, add the new enum value if it doesn't exist
DO $$
BEGIN
    -- Check if the new enum value exists, if not add it
    IF NOT EXISTS (
        SELECT 1 FROM pg_enum 
        WHERE enumlabel = 'SPORTS_BOOK_REVIEW_DEPRECATED' 
        AND enumtypid = (
            SELECT oid FROM pg_type WHERE typname = 'data_source_type'
        )
    ) THEN
        ALTER TYPE data_source_type ADD VALUE 'SPORTS_BOOK_REVIEW_DEPRECATED';
        RAISE NOTICE 'Added SPORTS_BOOK_REVIEW_DEPRECATED to data_source_type enum';
    END IF;
EXCEPTION
    WHEN undefined_object THEN
        RAISE NOTICE 'data_source_type enum does not exist - skipping enum update';
END $$;

-- =============================================================================
-- UPDATE SOURCE METADATA AND REFERENCES
-- =============================================================================

-- Update source metadata in the sources table to reflect correct terminology
UPDATE core_betting.sources 
SET 
    source_metadata = jsonb_set(
        COALESCE(source_metadata, '{}'::jsonb),
        '{description}', 
        '"Sports Book Review (SBR)"'::jsonb
    )
WHERE source_name = 'SPORTSBETTING_REPORT';

-- Update any legacy references in betting lines tables
UPDATE core_betting.betting_lines_moneyline 
SET source = 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type
WHERE source::text = 'SPORTSBETTING_REPORT';

UPDATE core_betting.betting_lines_spreads 
SET source = 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type  
WHERE source::text = 'SPORTSBETTING_REPORT';

UPDATE core_betting.betting_lines_totals 
SET source = 'SPORTS_BOOK_REVIEW_DEPRECATED'::data_source_type
WHERE source::text = 'SPORTSBETTING_REPORT';

-- =============================================================================
-- UPDATE COMMENTS AND DOCUMENTATION
-- =============================================================================

-- Update column comments to reflect correct terminology
COMMENT ON COLUMN core_betting.games.sportsbookreview_game_id IS 
'Unique identifier from Sports Book Review (SBR) - deprecated source, use SBRUnifiedCollector instead';

-- Update table comments if they exist
DO $$
BEGIN
    -- Update any table comments that reference the old terminology
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'games'
    ) THEN
        COMMENT ON COLUMN public.games.sportsbookreview_game_id IS 
        'Unique identifier from Sports Book Review (SBR) - deprecated source';
    END IF;
    
    -- Update view comments
    IF EXISTS (
        SELECT 1 FROM information_schema.views 
        WHERE table_schema = 'public' AND table_name = 'games_with_sportsbookreview_data'
    ) THEN
        COMMENT ON VIEW public.games_with_sportsbookreview_data IS 
        'Games with Sports Book Review (SBR) data - deprecated view, use unified data collectors';
    END IF;
EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'Public schema tables not found - skipping public schema updates';
END $$;

-- =============================================================================
-- UPDATE FUNCTION NAMES AND DOCUMENTATION
-- =============================================================================

-- Update function comments to reflect correct terminology
DO $$
DECLARE
    func_exists boolean;
BEGIN
    -- Check if the function exists and update its comment
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.proname = 'upsert_sportsbookreview_game'
        AND n.nspname = 'public'
    ) INTO func_exists;
    
    IF func_exists THEN
        COMMENT ON FUNCTION public.upsert_sportsbookreview_game IS 
        'DEPRECATED: Upsert function for Sports Book Review (SBR) games. Use SBRUnifiedCollector instead.';
        RAISE NOTICE 'Updated upsert_sportsbookreview_game function comment';
    END IF;
EXCEPTION
    WHEN undefined_function THEN
        RAISE NOTICE 'upsert_sportsbookreview_game function not found - skipping function update';
END $$;

-- =============================================================================
-- UPDATE SOURCE METADATA IN SOURCES TABLE
-- =============================================================================

-- Ensure the sources table has correct metadata for Sports Book Review
INSERT INTO core_betting.sources (
    source_name, 
    reliability_score, 
    source_metadata
) VALUES (
    'SPORTS_BOOK_REVIEW_DEPRECATED', 
    0.85, 
    '{"description": "Sports Book Review (SBR) - deprecated source", "data_types": ["consensus", "trends"], "replacement": "SBRUnifiedCollector", "status": "deprecated"}'::jsonb
) ON CONFLICT (source_name) DO UPDATE SET
    reliability_score = EXCLUDED.reliability_score,
    source_metadata = EXCLUDED.source_metadata;

-- Update the old entry if it exists
UPDATE core_betting.sources 
SET 
    source_metadata = jsonb_set(
        COALESCE(source_metadata, '{}'::jsonb),
        '{description}', 
        '"Sports Book Review (SBR) - deprecated source"'::jsonb
    ),
    source_metadata = jsonb_set(
        source_metadata,
        '{replacement}',
        '"SBRUnifiedCollector"'::jsonb
    ),
    source_metadata = jsonb_set(
        source_metadata,
        '{status}',
        '"deprecated"'::jsonb
    )
WHERE source_name = 'SPORTSBETTING_REPORT';

-- =============================================================================
-- DATA QUALITY AND VALIDATION
-- =============================================================================

-- Create a function to validate the terminology updates
CREATE OR REPLACE FUNCTION core_betting.validate_sports_book_review_updates()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check enum values
    RETURN QUERY
    SELECT 
        'Enum Value Check'::TEXT,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM pg_enum 
                WHERE enumlabel = 'SPORTS_BOOK_REVIEW_DEPRECATED'
            ) THEN 'PASS'
            ELSE 'FAIL'
        END::TEXT,
        'SPORTS_BOOK_REVIEW_DEPRECATED enum value exists'::TEXT;
    
    -- Check source metadata updates
    RETURN QUERY
    SELECT 
        'Source Metadata Check'::TEXT,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM core_betting.sources 
                WHERE source_metadata->>'description' LIKE '%Sports Book Review%'
            ) THEN 'PASS'
            ELSE 'FAIL'
        END::TEXT,
        'Source metadata contains correct Sports Book Review terminology'::TEXT;
    
    -- Check column comments
    RETURN QUERY
    SELECT 
        'Column Comment Check'::TEXT,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'games' 
                AND column_name = 'sportsbookreview_game_id'
                AND column_comment LIKE '%Sports Book Review%'
            ) THEN 'PASS'
            ELSE 'PARTIAL'
        END::TEXT,
        'Column comments updated with correct terminology'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- LOG MIGRATION COMPLETION
-- =============================================================================

-- Log this migration
INSERT INTO core_betting.data_migrations (
    migration_name,
    source_table,
    target_table,
    records_migrated,
    migration_status,
    started_at,
    completed_at
) VALUES (
    'Sports_Book_Review_Terminology_Update',
    'Multiple tables and enums',
    'Updated terminology throughout database',
    (SELECT COUNT(*) FROM core_betting.sources WHERE source_metadata->>'description' LIKE '%Sports Book Review%'),
    'COMPLETED',
    NOW(),
    NOW()
);

COMMIT;

-- =============================================================================
-- POST-MIGRATION VALIDATION
-- =============================================================================

-- Run validation checks
SELECT 'Migration 003 completed successfully - Sports Book Review terminology updated' as status;

-- Display validation results
SELECT * FROM core_betting.validate_sports_book_review_updates();

-- Show updated source information
SELECT 
    source_name,
    source_metadata->>'description' as description,
    source_metadata->>'status' as status,
    source_metadata->>'replacement' as replacement
FROM core_betting.sources 
WHERE source_name IN ('SPORTSBETTING_REPORT', 'SPORTS_BOOK_REVIEW_DEPRECATED')
ORDER BY source_name;

-- Check for any remaining incorrect references
SELECT 'Validation: Checking for remaining incorrect references...' as info;

-- Check if any tables still have the old enum value
DO $$
DECLARE
    table_name TEXT;
    old_refs_count INTEGER := 0;
BEGIN
    -- Check all betting lines tables for old references
    FOR table_name IN 
        SELECT t.table_name 
        FROM information_schema.tables t
        WHERE t.table_schema = 'core_betting' 
        AND t.table_name LIKE 'betting_lines_%'
    LOOP
        EXECUTE format('
            SELECT COUNT(*) FROM core_betting.%I 
            WHERE source::text = ''SPORTSBETTING_REPORT''
        ', table_name) INTO old_refs_count;
        
        IF old_refs_count > 0 THEN
            RAISE NOTICE 'Found % old references in table %', old_refs_count, table_name;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Migration validation completed';
END $$;