-- =============================================================================
-- Unified Game Outcomes View (Simplified Version)
-- =============================================================================
-- Purpose: Cross-system unified view focusing on existing tables
-- Integration: Uses MLB Stats API game IDs for comprehensive data correlation
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
    -- Action Network games (primary source)
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
),

-- Aggregate game information across sources
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
        
        -- Current odds summary
        COUNT(*) FILTER (WHERE is_current_odds = TRUE) as current_odds_count
    FROM staging.action_network_odds_historical
    WHERE mlb_stats_api_game_id IS NOT NULL
    GROUP BY external_game_id
),

-- Get VSIN sharp action summary (if table exists)
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
    GROUP BY mlb_stats_api_game_id
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
    AVG(data_completeness_score) as avg_data_completeness

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
    integration_quality TEXT
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
        v.integration_quality
    FROM staging.v_unified_game_outcomes v
    WHERE v.mlb_stats_api_game_id = p_mlb_game_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Documentation and Comments
-- =============================================================================

COMMENT ON VIEW staging.v_unified_game_outcomes IS 
'Unified view of MLB games combining Action Network and VSIN data using MLB Stats API game IDs for cross-system integration. Includes betting data and sharp action indicators.';

COMMENT ON VIEW staging.v_game_outcome_summary IS 
'Daily summary of unified game data showing source coverage, integration quality, and betting data completeness.';

COMMENT ON FUNCTION staging.get_unified_game_outcome IS 
'Retrieves unified game outcome data for a specific MLB Stats API game ID.';

-- =============================================================================
-- Performance Indexes
-- =============================================================================

-- Ensure key indexes exist
CREATE INDEX IF NOT EXISTS idx_an_games_mlb_api_id ON staging.action_network_games(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_an_odds_historical_mlb_api_id ON staging.action_network_odds_historical(mlb_stats_api_game_id);
CREATE INDEX IF NOT EXISTS idx_vsin_betting_mlb_api_id ON staging.vsin_betting_data(mlb_stats_api_game_id);

-- =============================================================================
-- Example Usage
-- =============================================================================

/*
-- Example 1: Get today's games with sharp action
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

-- Example 3: Games with excellent integration
SELECT mlb_stats_api_game_id, home_team_normalized, away_team_normalized,
       source_count, action_network_sportsbooks, sharp_action_records
FROM staging.v_unified_game_outcomes 
WHERE integration_quality = 'excellent'
  AND game_date >= CURRENT_DATE - INTERVAL '3 days'
ORDER BY sharp_action_records DESC;
*/