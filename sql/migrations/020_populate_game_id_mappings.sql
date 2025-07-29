-- Migration: Populate Game ID Mappings from Existing Data
-- Purpose: Migrate existing resolved game ID mappings from curated.games_complete
-- Created: 2025-07-28
-- Reference: .claude/tasks/game_id_mapping_optimization.md

-- ================================
-- Phase 1: Data Population from curated.games_complete
-- ================================

-- Insert existing mappings from curated.games_complete table
INSERT INTO staging.game_id_mappings (
    mlb_stats_api_game_id,
    action_network_game_id,
    vsin_game_id, 
    sbd_game_id,
    sbr_game_id,
    home_team,
    away_team,
    game_date,
    game_datetime,
    primary_source,
    resolution_confidence,
    last_verified_at,
    created_at,
    updated_at
)
SELECT DISTINCT
    gc.mlb_stats_api_game_id,
    gc.action_network_game_id::VARCHAR(255),
    gc.vsin_game_id,
    NULL as sbd_game_id,  -- SBD not in curated.games_complete yet
    gc.sportsbookreview_game_id as sbr_game_id,
    gc.home_team,
    gc.away_team,
    gc.game_date,
    gc.game_datetime,
    -- Determine primary source based on which external ID exists
    COALESCE(
        CASE WHEN gc.action_network_game_id IS NOT NULL THEN 'action_network' END,
        CASE WHEN gc.vsin_game_id IS NOT NULL THEN 'vsin' END,
        CASE WHEN gc.sportsbookreview_game_id IS NOT NULL THEN 'sbr' END,
        'manual'
    ) as primary_source,
    -- Set high confidence for existing curated data
    CASE 
        WHEN gc.data_quality = 'HIGH' THEN 1.0
        WHEN gc.data_quality = 'MEDIUM' THEN 0.8
        WHEN gc.data_quality = 'LOW' THEN 0.6
        ELSE 0.9
    END as resolution_confidence,
    NOW() as last_verified_at,
    COALESCE(gc.created_at, NOW()) as created_at,
    COALESCE(gc.updated_at, NOW()) as updated_at
FROM curated.games_complete gc
WHERE gc.mlb_stats_api_game_id IS NOT NULL
  AND gc.mlb_stats_api_game_id != ''
  -- Ensure we have at least one external ID
  AND (
      gc.action_network_game_id IS NOT NULL OR
      gc.vsin_game_id IS NOT NULL OR 
      gc.sportsbookreview_game_id IS NOT NULL
  )
  -- Avoid duplicate entries
ON CONFLICT (mlb_stats_api_game_id) DO UPDATE SET
    -- Update external IDs if new ones are found
    action_network_game_id = COALESCE(EXCLUDED.action_network_game_id, staging.game_id_mappings.action_network_game_id),
    vsin_game_id = COALESCE(EXCLUDED.vsin_game_id, staging.game_id_mappings.vsin_game_id),
    sbd_game_id = COALESCE(EXCLUDED.sbd_game_id, staging.game_id_mappings.sbd_game_id),
    sbr_game_id = COALESCE(EXCLUDED.sbr_game_id, staging.game_id_mappings.sbr_game_id),
    -- Update metadata if confidence is higher
    resolution_confidence = GREATEST(staging.game_id_mappings.resolution_confidence, EXCLUDED.resolution_confidence),
    last_verified_at = NOW(),
    updated_at = NOW();

-- ================================
-- Phase 2: Validate Population Results
-- ================================

-- Show population statistics
DO $$
DECLARE
    mapping_count INTEGER;
    action_network_count INTEGER;
    vsin_count INTEGER;
    sbd_count INTEGER;
    sbr_count INTEGER;
    avg_confidence DECIMAL;
    unique_mlb_ids INTEGER;
BEGIN
    -- Get counts
    SELECT 
        COUNT(*),
        COUNT(action_network_game_id),
        COUNT(vsin_game_id),
        COUNT(sbd_game_id),
        COUNT(sbr_game_id),
        AVG(resolution_confidence),
        COUNT(DISTINCT mlb_stats_api_game_id)
    INTO 
        mapping_count,
        action_network_count,
        vsin_count,
        sbd_count,
        sbr_count,
        avg_confidence,
        unique_mlb_ids
    FROM staging.game_id_mappings;
    
    RAISE NOTICE 'Game ID Mappings Population Complete:';
    RAISE NOTICE '- Total mappings created: %', mapping_count;
    RAISE NOTICE '- Unique MLB game IDs: %', unique_mlb_ids;
    RAISE NOTICE '- Action Network mappings: %', action_network_count;
    RAISE NOTICE '- VSIN mappings: %', vsin_count;
    RAISE NOTICE '- SBD mappings: %', sbd_count;
    RAISE NOTICE '- SBR mappings: %', sbr_count;
    RAISE NOTICE '- Average confidence: %', ROUND(avg_confidence, 3);
END $$;

-- ================================
-- Phase 3: Additional Population from Staging Tables
-- ================================

-- Add mappings from Action Network staging tables where we have MLB IDs
INSERT INTO staging.game_id_mappings (
    mlb_stats_api_game_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    game_datetime,
    primary_source,
    resolution_confidence,
    last_verified_at
)
SELECT DISTINCT
    g.mlb_stats_api_game_id,
    g.external_game_id as action_network_game_id,
    g.home_team_normalized as home_team,
    g.away_team_normalized as away_team,
    g.game_date,
    CASE 
        WHEN g.game_time IS NOT NULL THEN 
            (g.game_date + g.game_time)::TIMESTAMPTZ
        ELSE NULL
    END as game_datetime,
    'action_network' as primary_source,
    0.9 as resolution_confidence, -- High confidence for staging data
    NOW() as last_verified_at
FROM staging.action_network_games g
WHERE g.mlb_stats_api_game_id IS NOT NULL
  AND g.mlb_stats_api_game_id != ''
  AND g.external_game_id IS NOT NULL
  -- Only insert if not already exists
  AND g.mlb_stats_api_game_id NOT IN (
      SELECT mlb_stats_api_game_id 
      FROM staging.game_id_mappings
  )
ON CONFLICT (mlb_stats_api_game_id) DO UPDATE SET
    action_network_game_id = COALESCE(staging.game_id_mappings.action_network_game_id, EXCLUDED.action_network_game_id),
    last_verified_at = NOW(),
    updated_at = NOW();

-- ================================
-- Phase 4: Data Quality Validation
-- ================================

-- Check for any data quality issues
DO $$
DECLARE
    duplicate_check INTEGER;
    confidence_check INTEGER;
    external_id_check INTEGER;
BEGIN
    -- Check for duplicate MLB IDs (should be 0 due to unique constraint)
    SELECT COUNT(*) - COUNT(DISTINCT mlb_stats_api_game_id)
    INTO duplicate_check
    FROM staging.game_id_mappings;
    
    -- Check for low confidence mappings
    SELECT COUNT(*)
    INTO confidence_check
    FROM staging.game_id_mappings
    WHERE resolution_confidence < 0.5;
    
    -- Check for records without any external IDs (should be 0 due to constraint)
    SELECT COUNT(*)
    INTO external_id_check
    FROM staging.game_id_mappings
    WHERE action_network_game_id IS NULL 
      AND vsin_game_id IS NULL 
      AND sbd_game_id IS NULL 
      AND sbr_game_id IS NULL;
    
    RAISE NOTICE 'Data Quality Validation:';
    RAISE NOTICE '- Duplicate MLB game IDs: % (should be 0)', duplicate_check;
    RAISE NOTICE '- Low confidence mappings (<0.5): %', confidence_check;
    RAISE NOTICE '- Records without external IDs: % (should be 0)', external_id_check;
    
    IF duplicate_check > 0 OR external_id_check > 0 THEN
        RAISE WARNING 'Data quality issues detected! Please review the mappings.';
    END IF;
END $$;

-- ================================
-- Phase 5: Create Backup and Validation Views
-- ================================

-- Create a view for easy validation of mappings
CREATE OR REPLACE VIEW staging.v_game_id_mapping_summary AS
SELECT 
    m.mlb_stats_api_game_id,
    m.home_team,
    m.away_team,
    m.game_date,
    m.primary_source,
    m.resolution_confidence,
    -- External ID availability flags
    CASE WHEN m.action_network_game_id IS NOT NULL THEN '✓' ELSE '✗' END as has_action_network,
    CASE WHEN m.vsin_game_id IS NOT NULL THEN '✓' ELSE '✗' END as has_vsin,
    CASE WHEN m.sbd_game_id IS NOT NULL THEN '✓' ELSE '✗' END as has_sbd,
    CASE WHEN m.sbr_game_id IS NOT NULL THEN '✓' ELSE '✗' END as has_sbr,
    -- Total external IDs for this game
    COALESCE(
        (CASE WHEN m.action_network_game_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN m.vsin_game_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN m.sbd_game_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN m.sbr_game_id IS NOT NULL THEN 1 ELSE 0 END),
        0
    ) as external_id_count,
    m.last_verified_at,
    m.created_at,
    m.updated_at
FROM staging.game_id_mappings m
ORDER BY m.game_date DESC, m.mlb_stats_api_game_id;

-- Add comment to the view
COMMENT ON VIEW staging.v_game_id_mapping_summary IS 
'Summary view of game ID mappings showing external ID availability and data quality metrics';

-- ================================
-- Phase 6: Performance Test Queries
-- ================================

-- Test performance of common lookup patterns
EXPLAIN (ANALYZE, BUFFERS) 
SELECT m.mlb_stats_api_game_id, m.home_team, m.away_team
FROM staging.game_id_mappings m 
WHERE m.action_network_game_id = '258267'  -- Sample Action Network ID
LIMIT 1;

EXPLAIN (ANALYZE, BUFFERS)
SELECT m.mlb_stats_api_game_id, m.home_team, m.away_team  
FROM staging.game_id_mappings m
WHERE m.game_date BETWEEN '2024-07-01' AND '2024-07-31'
ORDER BY m.game_date;

-- ================================
-- Final Summary Report
-- ================================

-- Generate final population summary
SELECT 
    'Population Summary' as report_type,
    COUNT(*) as total_mappings,
    COUNT(DISTINCT mlb_stats_api_game_id) as unique_mlb_games,
    MIN(game_date) as earliest_game,
    MAX(game_date) as latest_game,
    ROUND(AVG(resolution_confidence), 3) as avg_confidence
FROM staging.game_id_mappings

UNION ALL

SELECT 
    'Source Distribution' as report_type,
    NULL as total_mappings,
    NULL as unique_mlb_games, 
    NULL as earliest_game,
    NULL as latest_game,
    NULL as avg_confidence

UNION ALL

SELECT 
    'Action Network' as report_type,
    COUNT(*) as total_mappings,
    NULL, NULL, NULL, NULL
FROM staging.game_id_mappings 
WHERE action_network_game_id IS NOT NULL

UNION ALL

SELECT 
    'VSIN' as report_type,
    COUNT(*) as total_mappings,
    NULL, NULL, NULL, NULL
FROM staging.game_id_mappings 
WHERE vsin_game_id IS NOT NULL

UNION ALL

SELECT 
    'SBD' as report_type,
    COUNT(*) as total_mappings,
    NULL, NULL, NULL, NULL
FROM staging.game_id_mappings 
WHERE sbd_game_id IS NOT NULL

UNION ALL

SELECT 
    'SBR' as report_type,
    COUNT(*) as total_mappings,
    NULL, NULL, NULL, NULL
FROM staging.game_id_mappings 
WHERE sbr_game_id IS NOT NULL;

-- Show sample mappings for verification
SELECT 
    mlb_stats_api_game_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    primary_source,
    resolution_confidence
FROM staging.game_id_mappings 
ORDER BY game_date DESC 
LIMIT 10;