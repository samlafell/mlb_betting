-- =============================================================================
-- Unified Game Outcomes View
-- =============================================================================
-- Purpose: Cross-system unified view of MLB games with outcomes and betting data
-- Integration: Uses MLB Stats API game IDs for comprehensive data correlation
--
-- This view combines:
-- - Game information from multiple sources (Action Network, VSIN, SBD)
-- - Official MLB Stats API data and outcomes
-- - Betting data and sharp action indicators
-- - Cross-source data quality assessment
-- =============================================================================

-- Drop existing views and functions
DROP VIEW IF EXISTS staging.v_unified_game_outcomes CASCADE;
DROP VIEW IF EXISTS staging.v_game_outcome_summary CASCADE;
DROP FUNCTION IF EXISTS staging.get_unified_game_outcome(VARCHAR) CASCADE;

-- =============================================================================
-- Main Unified Game Outcomes View
-- =============================================================================

CREATE OR REPLACE VIEW staging.v_unified_game_outcomes AS
WITH game_sources AS (
    -- Action Network games
    SELECT 
        mlb_stats_api_game_id,
        'action_network' as source,
        external_game_id as source_game_id,
        home_team_normalized,
        away_team_normalized,
        game_date,
        created_at as first_seen,
        updated_at as last_updated
    FROM staging.action_network_games
    WHERE mlb_stats_api_game_id IS NOT NULL

    UNION ALL

    -- VSIN betting data games (if table exists)
    SELECT 
        mlb_stats_api_game_id,
        'vsin' as source,
        external_matchup_id as source_game_id,
        home_team_normalized,
        away_team_normalized,
        game_date,
        created_at as first_seen,
        updated_at as last_updated
    FROM staging.vsin_betting_data
    WHERE mlb_stats_api_game_id IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'staging' AND table_name = 'vsin_betting_data'
    )

    UNION ALL

    -- SBD betting splits games (if table exists)
    SELECT 
        s.mlb_stats_api_game_id,
        'sbd' as source,
        s.external_matchup_id as source_game_id,
        s.home_team_normalized,
        s.away_team_normalized,
        s.game_date,
        s.created_at as first_seen,
        s.updated_at as last_updated
    FROM (
        SELECT 
            NULL::VARCHAR as mlb_stats_api_game_id,
            'sbd' as source,
            NULL::VARCHAR as external_matchup_id,
            NULL::VARCHAR as home_team_normalized,
            NULL::VARCHAR as away_team_normalized,
            NULL::DATE as game_date,
            NULL::TIMESTAMP as created_at,
            NULL::TIMESTAMP as updated_at
        WHERE EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'staging' AND table_name = 'sbd_betting_splits'
        )
        AND FALSE  -- This ensures no rows when table doesn't exist
    ) s
    WHERE s.mlb_stats_api_game_id IS NOT NULL

    UNION ALL

    -- Odds API games (if table exists)
    SELECT 
        mlb_stats_api_game_id,
        'odds_api' as source,
        CONCAT(sport_key, '_', home_team, '_', away_team) as source_game_id,
        home_team as home_team_normalized,
        away_team as away_team_normalized,
        commence_time::date as game_date,
        created_at as first_seen,
        updated_at as last_updated
    FROM staging.odds_api_games
    WHERE mlb_stats_api_game_id IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'staging' AND table_name = 'odds_api_games'
    )
),

-- Aggregate game information across all sources
unified_games AS (
    SELECT 
        mlb_stats_api_game_id,
        -- Take the most complete team names (prefer Action Network)
        COALESCE(
            MAX(CASE WHEN source = 'action_network' THEN home_team_normalized END),
            MAX(home_team_normalized)
        ) as home_team_normalized,
        COALESCE(
            MAX(CASE WHEN source = 'action_network' THEN away_team_normalized END),
            MAX(away_team_normalized)
        ) as away_team_normalized,
        
        -- Game timing
        MAX(game_date) as game_date,
        MIN(first_seen) as first_seen_across_sources,
        MAX(last_updated) as last_updated_across_sources,
        
        -- Source tracking
        ARRAY_AGG(DISTINCT source ORDER BY source) as data_sources,
        COUNT(DISTINCT source) as source_count,
        
        -- Source-specific IDs for reference
        JSONB_OBJECT_AGG(
            source, 
            source_game_id
        ) FILTER (WHERE source_game_id IS NOT NULL) as source_game_ids
        
    FROM game_sources
    GROUP BY mlb_stats_api_game_id
),

-- Get Action Network betting data summary
action_network_summary AS (
    SELECT 
        external_game_id,
        COUNT(DISTINCT sportsbook_name) as sportsbooks_count,
        COUNT(DISTINCT market_type) as markets_count,
        COUNT(*) as total_odds_records,
        MIN(updated_at) as first_odds_timestamp,
        MAX(updated_at) as latest_odds_timestamp,
        
        -- Current odds summary (latest for each market/sportsbook)
        COUNT(*) FILTER (WHERE is_current_odds = TRUE) as current_odds_count
    FROM staging.action_network_odds_historical
    WHERE mlb_stats_api_game_id IS NOT NULL
    GROUP BY external_game_id
),

-- Get VSIN sharp action summary  
vsin_sharp_summary AS (
    SELECT 
        mlb_stats_api_game_id,
        COUNT(*) as vsin_records_count,
        COUNT(DISTINCT sportsbook_name) as vsin_sportsbooks_count,
        
        -- Sharp action indicators
        COUNT(*) FILTER (
            WHERE moneyline_sharp_side IS NOT NULL 
            OR total_sharp_side IS NOT NULL 
            OR runline_sharp_side IS NOT NULL
        ) as sharp_action_records,
        
        AVG(sharp_confidence) as avg_sharp_confidence,
        MAX(sharp_confidence) as max_sharp_confidence,
        
        -- RLM indicators
        COUNT(*) FILTER (
            WHERE moneyline_rlm_detected = TRUE 
            OR total_rlm_detected = TRUE 
            OR runline_rlm_detected = TRUE
        ) as rlm_records
    FROM staging.vsin_betting_data
    WHERE mlb_stats_api_game_id IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'staging' AND table_name = 'vsin_betting_data'
    )
    GROUP BY mlb_stats_api_game_id
),

-- Get game outcomes from multiple sources
game_outcomes AS (
    -- Try to get outcomes from multiple potential sources
    -- This is a placeholder structure - adapt based on your actual outcome tables
    SELECT 
        mlb_stats_api_game_id,
        'placeholder' as outcome_source,
        NULL::INTEGER as home_score,
        NULL::INTEGER as away_score,
        NULL::VARCHAR as game_status,
        NULL::TIMESTAMP as completed_at
    WHERE FALSE  -- Placeholder for when no data sources are available
    
    -- MLB Stats API outcomes (when available)
    UNION ALL
    SELECT 
        mlb_stats_api_game_id, 
        'mlb_stats_api' as outcome_source,
        home_score, 
        away_score, 
        game_status, 
        completed_at
    FROM staging.mlb_game_outcomes
    WHERE mlb_stats_api_game_id IS NOT NULL
    AND game_status IN ('completed', 'final')
    AND EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'staging' AND table_name = 'mlb_game_outcomes'
    )
    
    -- Enhanced games outcomes (fallback)
    UNION ALL
    SELECT 
        mlb_stats_api_game_id,
        'enhanced_games' as outcome_source,
        home_score,
        away_score,
        game_status,
        updated_at as completed_at
    FROM curated.enhanced_games
    WHERE mlb_stats_api_game_id IS NOT NULL
    AND game_status IN ('completed', 'final')
    AND home_score IS NOT NULL
    AND away_score IS NOT NULL
)

-- Main view query
SELECT 
    ug.mlb_stats_api_game_id,
    
    -- Game identification
    ug.home_team_normalized,
    ug.away_team_normalized,
    ug.game_date,
    
    -- Data source tracking
    ug.data_sources,
    ug.source_count,
    ug.source_game_ids,
    
    -- Timing information
    ug.first_seen_across_sources,
    ug.last_updated_across_sources,
    
    -- Action Network betting data
    COALESCE(ans.sportsbooks_count, 0) as action_network_sportsbooks,
    COALESCE(ans.markets_count, 0) as action_network_markets,
    COALESCE(ans.total_odds_records, 0) as action_network_odds_records,
    ans.first_odds_timestamp,
    ans.latest_odds_timestamp,
    COALESCE(ans.current_odds_count, 0) as current_odds_available,
    
    -- VSIN sharp action data
    COALESCE(vss.vsin_records_count, 0) as vsin_records_count,
    COALESCE(vss.vsin_sportsbooks_count, 0) as vsin_sportsbooks_count,
    COALESCE(vss.sharp_action_records, 0) as sharp_action_records,
    ROUND(vss.avg_sharp_confidence, 3) as avg_sharp_confidence,
    ROUND(vss.max_sharp_confidence, 3) as max_sharp_confidence,
    COALESCE(vss.rlm_records, 0) as rlm_records,
    
    -- Sharp action flags
    CASE 
        WHEN vss.sharp_action_records > 0 THEN TRUE 
        ELSE FALSE 
    END as has_sharp_action,
    CASE 
        WHEN vss.rlm_records > 0 THEN TRUE 
        ELSE FALSE 
    END as has_reverse_line_movement,
    
    -- Game outcomes (placeholder)
    go.outcome_source,
    go.home_score,
    go.away_score,
    go.game_status,
    go.completed_at,
    
    -- Derived game result
    CASE 
        WHEN go.home_score IS NOT NULL AND go.away_score IS NOT NULL THEN
            CASE 
                WHEN go.home_score > go.away_score THEN 'home_win'
                WHEN go.away_score > go.home_score THEN 'away_win'
                ELSE 'tie'
            END
        ELSE NULL
    END as game_result,
    
    -- Data completeness score
    CASE 
        WHEN ug.source_count >= 3 THEN 1.0
        WHEN ug.source_count = 2 THEN 0.8
        WHEN ug.source_count = 1 THEN 0.6
        ELSE 0.0
    END as data_completeness_score,
    
    -- Integration quality
    CASE 
        WHEN ans.total_odds_records > 0 AND vss.vsin_records_count > 0 THEN 'excellent'
        WHEN ans.total_odds_records > 0 OR vss.vsin_records_count > 0 THEN 'good'
        WHEN ug.source_count > 1 THEN 'fair'
        ELSE 'limited'
    END as integration_quality

FROM unified_games ug
LEFT JOIN action_network_summary ans ON ug.source_game_ids->>'action_network' = ans.external_game_id
LEFT JOIN vsin_sharp_summary vss ON ug.mlb_stats_api_game_id = vss.mlb_stats_api_game_id
LEFT JOIN game_outcomes go ON ug.mlb_stats_api_game_id = go.mlb_stats_api_game_id

ORDER BY ug.game_date DESC, ug.mlb_stats_api_game_id;

-- =============================================================================
-- Game Outcome Summary View
-- =============================================================================

CREATE OR REPLACE VIEW staging.v_game_outcome_summary AS
SELECT 
    -- Date aggregation
    game_date,
    COUNT(*) as total_games,
    
    -- Source coverage
    COUNT(DISTINCT mlb_stats_api_game_id) as unique_mlb_games,
    AVG(source_count) as avg_sources_per_game,
    
    -- Integration quality distribution
    COUNT(*) FILTER (WHERE integration_quality = 'excellent') as excellent_integration,
    COUNT(*) FILTER (WHERE integration_quality = 'good') as good_integration,
    COUNT(*) FILTER (WHERE integration_quality = 'fair') as fair_integration,
    COUNT(*) FILTER (WHERE integration_quality = 'limited') as limited_integration,
    
    -- Betting data coverage
    COUNT(*) FILTER (WHERE action_network_odds_records > 0) as games_with_odds,
    COUNT(*) FILTER (WHERE sharp_action_records > 0) as games_with_sharp_action,
    COUNT(*) FILTER (WHERE rlm_records > 0) as games_with_rlm,
    
    -- Average betting metrics
    AVG(action_network_sportsbooks) as avg_sportsbooks_per_game,
    AVG(action_network_odds_records) as avg_odds_records_per_game,
    AVG(avg_sharp_confidence) as overall_avg_sharp_confidence,
    
    -- Data quality
    AVG(data_completeness_score) as avg_data_completeness,
    
    -- Game outcomes (when available)
    COUNT(*) FILTER (WHERE game_result IS NOT NULL) as games_with_outcomes,
    COUNT(*) FILTER (WHERE game_result = 'home_win') as home_wins,
    COUNT(*) FILTER (WHERE game_result = 'away_win') as away_wins,
    COUNT(*) FILTER (WHERE game_result = 'tie') as ties

FROM staging.v_unified_game_outcomes
GROUP BY game_date
ORDER BY game_date DESC;

-- =============================================================================
-- Utility Functions
-- =============================================================================

-- Function to get unified game outcome by MLB Stats API game ID
CREATE OR REPLACE FUNCTION staging.get_unified_game_outcome(p_mlb_game_id VARCHAR)
RETURNS TABLE(
    mlb_stats_api_game_id VARCHAR,
    home_team_normalized VARCHAR,
    away_team_normalized VARCHAR,
    game_date DATE,
    data_sources TEXT[],
    source_count INTEGER,
    has_sharp_action BOOLEAN,
    has_reverse_line_movement BOOLEAN,
    integration_quality TEXT,
    game_result VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        v.mlb_stats_api_game_id,
        v.home_team_normalized,
        v.away_team_normalized,
        v.game_date,
        v.data_sources,
        v.source_count,
        v.has_sharp_action,
        v.has_reverse_line_movement,
        v.integration_quality,
        v.game_result
    FROM staging.v_unified_game_outcomes v
    WHERE v.mlb_stats_api_game_id = p_mlb_game_id;
END;
$$ LANGUAGE plpgsql;

-- Function to find games with cross-source discrepancies
CREATE OR REPLACE FUNCTION staging.find_cross_source_discrepancies()
RETURNS TABLE(
    mlb_stats_api_game_id VARCHAR,
    home_team_normalized VARCHAR,
    away_team_normalized VARCHAR,
    game_date DATE,
    data_sources TEXT[],
    source_count INTEGER,
    discrepancy_type TEXT,
    details JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH source_details AS (
        SELECT 
            v.mlb_stats_api_game_id,
            v.home_team_normalized,
            v.away_team_normalized,
            v.game_date,
            v.data_sources,
            v.source_count,
            v.source_game_ids
        FROM staging.v_unified_game_outcomes v
        WHERE v.source_count > 1  -- Only games with multiple sources
    )
    SELECT 
        sd.mlb_stats_api_game_id,
        sd.home_team_normalized,
        sd.away_team_normalized,
        sd.game_date,
        sd.data_sources,
        sd.source_count,
        'multiple_source_ids'::TEXT as discrepancy_type,
        JSONB_BUILD_OBJECT(
            'source_game_ids', sd.source_game_ids,
            'note', 'Game appears in multiple sources with different IDs'
        ) as details
    FROM source_details sd;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Indexes for Performance
-- =============================================================================

-- Since this is a view, we'll create indexes on the underlying tables
-- These may already exist from the migration, but we'll ensure they're present

-- Action Network indexes
CREATE INDEX IF NOT EXISTS idx_an_games_mlb_api_id ON staging.action_network_games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_an_odds_historical_mlb_api_id ON staging.action_network_odds_historical(mlb_stats_api_game_id);

-- VSIN indexes (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'vsin_betting_data') THEN
        CREATE INDEX IF NOT EXISTS idx_vsin_betting_mlb_api_id ON staging.vsin_betting_data(mlb_stats_api_game_id);
    END IF;
END $$;

-- SBD indexes (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'sbd_betting_splits') THEN
        CREATE INDEX IF NOT EXISTS idx_sbd_splits_mlb_api_id ON staging.sbd_betting_splits(mlb_stats_api_game_id);
    END IF;
END $$;

-- Odds API indexes (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'odds_api_games') THEN
        CREATE INDEX IF NOT EXISTS idx_odds_api_games_mlb_api_id ON staging.odds_api_games(mlb_stats_api_game_id);
    END IF;
END $$;

-- =============================================================================
-- Documentation and Comments
-- =============================================================================

COMMENT ON VIEW staging.v_unified_game_outcomes IS 
'Comprehensive unified view of MLB games combining data from all sources (Action Network, VSIN, SBD, Odds API) using MLB Stats API game IDs for cross-system integration. Includes betting data, sharp action indicators, and game outcomes.';

COMMENT ON VIEW staging.v_game_outcome_summary IS 
'Daily summary of game outcome data showing source coverage, integration quality, and betting data completeness across all unified games.';

COMMENT ON FUNCTION staging.get_unified_game_outcome IS 
'Retrieves unified game outcome data for a specific MLB Stats API game ID.';

COMMENT ON FUNCTION staging.find_cross_source_discrepancies IS 
'Identifies games that appear in multiple sources to help detect data quality issues or integration problems.';

-- =============================================================================
-- Example Usage Queries
-- =============================================================================

/*
-- Example 1: Get all games from today with sharp action
SELECT mlb_stats_api_game_id, home_team_normalized, away_team_normalized,
       data_sources, has_sharp_action, max_sharp_confidence
FROM staging.v_unified_game_outcomes 
WHERE game_date = CURRENT_DATE 
  AND has_sharp_action = TRUE
ORDER BY max_sharp_confidence DESC;

-- Example 2: Daily summary for the last week
SELECT * FROM staging.v_game_outcome_summary 
WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY game_date DESC;

-- Example 3: Games with excellent integration quality
SELECT mlb_stats_api_game_id, home_team_normalized, away_team_normalized,
       source_count, action_network_sportsbooks, sharp_action_records
FROM staging.v_unified_game_outcomes 
WHERE integration_quality = 'excellent'
  AND game_date >= CURRENT_DATE - INTERVAL '3 days'
ORDER BY sharp_action_records DESC;

-- Example 4: Cross-source data validation
SELECT * FROM staging.find_cross_source_discrepancies()
WHERE game_date >= CURRENT_DATE - INTERVAL '1 day';

-- Example 5: Get specific game details
SELECT * FROM staging.get_unified_game_outcome('12345');
*/