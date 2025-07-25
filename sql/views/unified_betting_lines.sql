-- =============================================================================
-- Unified Betting Lines Views
-- =============================================================================
-- Purpose: Create unified views that combine all betting lines data across
-- sources with comprehensive source attribution and analytics capabilities
-- 
-- Author: Claude Code
-- Date: 2025-07-15
-- Dependencies: 001_enhance_source_tracking.sql, 002_migrate_legacy_data.sql
-- =============================================================================

-- =============================================================================
-- CORE UNIFIED VIEW: All Betting Lines with Source Attribution
-- =============================================================================

CREATE OR REPLACE VIEW analytics.unified_betting_lines AS
SELECT 
    'moneyline' as line_type,
    m.game_id,
    m.sportsbook,
    m.source::VARCHAR as source,
    m.home_ml::DECIMAL as home_value,
    m.away_ml::DECIMAL as away_value,
    NULL::DECIMAL as line_value,
    NULL::DECIMAL as secondary_price,
    m.odds_timestamp,
    m.source_reliability_score,
    m.data_quality,
    m.collection_method,
    m.source_metadata,
    m.collection_batch_id,
    m.created_at,
    m.updated_at,
    -- Derived fields for analytics
    ABS(m.home_ml - m.away_ml) as value_spread,
    CASE 
        WHEN m.home_ml < m.away_ml THEN 'HOME_FAVORED'
        WHEN m.away_ml < m.home_ml THEN 'AWAY_FAVORED'
        ELSE 'PICK_EM'
    END as market_direction,
    -- Data freshness indicator
    CASE 
        WHEN m.odds_timestamp > NOW() - INTERVAL '1 hour' THEN 'FRESH'
        WHEN m.odds_timestamp > NOW() - INTERVAL '4 hours' THEN 'RECENT'
        WHEN m.odds_timestamp > NOW() - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'VERY_STALE'
    END as data_freshness
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' m
WHERE m.home_ml IS NOT NULL OR m.away_ml IS NOT NULL

UNION ALL

SELECT 
    'spread' as line_type,
    s.game_id,
    s.sportsbook,
    s.source::VARCHAR as source,
    s.home_spread_price::DECIMAL as home_value,
    s.away_spread_price::DECIMAL as away_value,
    s.home_spread::DECIMAL as line_value,
    s.away_spread::DECIMAL as secondary_price,
    s.odds_timestamp,
    s.source_reliability_score,
    s.data_quality,
    s.collection_method,
    s.source_metadata,
    s.collection_batch_id,
    s.created_at,
    s.updated_at,
    -- Derived fields for analytics
    ABS(s.home_spread_price - s.away_spread_price) as value_spread,
    CASE 
        WHEN s.home_spread < 0 THEN 'HOME_FAVORED'
        WHEN s.home_spread > 0 THEN 'AWAY_FAVORED'
        ELSE 'PICK_EM'
    END as market_direction,
    -- Data freshness indicator
    CASE 
        WHEN s.odds_timestamp > NOW() - INTERVAL '1 hour' THEN 'FRESH'
        WHEN s.odds_timestamp > NOW() - INTERVAL '4 hours' THEN 'RECENT'
        WHEN s.odds_timestamp > NOW() - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'VERY_STALE'
    END as data_freshness
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's s
WHERE s.home_spread IS NOT NULL OR s.away_spread IS NOT NULL

UNION ALL

SELECT 
    'total' as line_type,
    t.game_id,
    t.sportsbook,
    t.source::VARCHAR as source,
    t.over_price::DECIMAL as home_value,
    t.under_price::DECIMAL as away_value,
    t.total_line::DECIMAL as line_value,
    NULL::DECIMAL as secondary_price,
    t.odds_timestamp,
    t.source_reliability_score,
    t.data_quality,
    t.collection_method,
    t.source_metadata,
    t.collection_batch_id,
    t.created_at,
    t.updated_at,
    -- Derived fields for analytics
    ABS(t.over_price - t.under_price) as value_spread,
    CASE 
        WHEN t.over_price < t.under_price THEN 'OVER_FAVORED'
        WHEN t.under_price < t.over_price THEN 'UNDER_FAVORED'
        ELSE 'BALANCED'
    END as market_direction,
    -- Data freshness indicator
    CASE 
        WHEN t.odds_timestamp > NOW() - INTERVAL '1 hour' THEN 'FRESH'
        WHEN t.odds_timestamp > NOW() - INTERVAL '4 hours' THEN 'RECENT'
        WHEN t.odds_timestamp > NOW() - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'VERY_STALE'
    END as data_freshness
FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' t
WHERE t.total_line IS NOT NULL;

-- =============================================================================
-- SOURCE COMPARISON VIEW: Latest Lines by Source
-- =============================================================================

CREATE OR REPLACE VIEW analytics.latest_lines_by_source AS
WITH ranked_lines AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, line_type, sportsbook, source 
            ORDER BY odds_timestamp DESC, source_reliability_score DESC
        ) as rn
    FROM analytics.unified_betting_lines
)
SELECT 
    game_id,
    line_type,
    sportsbook,
    source,
    home_value,
    away_value,
    line_value,
    secondary_price,
    odds_timestamp,
    source_reliability_score,
    data_quality,
    data_freshness,
    market_direction,
    value_spread
FROM ranked_lines 
WHERE rn = 1;

-- =============================================================================
-- SOURCE PERFORMANCE DASHBOARD VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.source_performance_dashboard AS
SELECT 
    source,
    COUNT(*) as total_records_today,
    COUNT(DISTINCT game_id) as games_covered,
    COUNT(DISTINCT sportsbook) as sportsbook_coverage,
    COUNT(DISTINCT line_type) as line_types_covered,
    AVG(source_reliability_score) as avg_reliability_score,
    MAX(odds_timestamp) as latest_update,
    MIN(odds_timestamp) as earliest_update,
    COUNT(*) FILTER (WHERE data_quality = 'HIGH') as high_quality_count,
    COUNT(*) FILTER (WHERE data_quality = 'MEDIUM') as medium_quality_count,
    COUNT(*) FILTER (WHERE data_quality = 'LOW') as low_quality_count,
    COUNT(*) FILTER (WHERE data_freshness = 'FRESH') as fresh_data_count,
    COUNT(*) FILTER (WHERE data_freshness = 'RECENT') as recent_data_count,
    COUNT(*) FILTER (WHERE data_freshness IN ('STALE', 'VERY_STALE')) as stale_data_count,
    -- Coverage percentages
    ROUND(
        (COUNT(DISTINCT game_id)::DECIMAL / NULLIF(
            (SELECT COUNT(DISTINCT game_id) FROM analytics.unified_betting_lines 
             WHERE DATE(odds_timestamp) = CURRENT_DATE), 0
        )) * 100, 2
    ) as game_coverage_percentage,
    -- Data quality score
    ROUND(
        (COUNT(*) FILTER (WHERE data_quality = 'HIGH') * 1.0 + 
         COUNT(*) FILTER (WHERE data_quality = 'MEDIUM') * 0.7 + 
         COUNT(*) FILTER (WHERE data_quality = 'LOW') * 0.3) / 
        NULLIF(COUNT(*), 0), 3
    ) as weighted_quality_score
FROM analytics.unified_betting_lines
WHERE DATE(odds_timestamp) = CURRENT_DATE
GROUP BY source
ORDER BY avg_reliability_score DESC, total_records_today DESC;

-- =============================================================================
-- CROSS-SOURCE ARBITRAGE DETECTION VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.cross_source_arbitrage AS
WITH latest_by_source AS (
    SELECT 
        game_id,
        line_type,
        sportsbook,
        source,
        home_value,
        away_value,
        line_value,
        odds_timestamp,
        source_reliability_score,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, line_type, sportsbook, source 
            ORDER BY odds_timestamp DESC, source_reliability_score DESC
        ) as rn
    FROM analytics.unified_betting_lines
    WHERE data_freshness IN ('FRESH', 'RECENT')
      AND data_quality IN ('HIGH', 'MEDIUM')
),
source_pairs AS (
    SELECT 
        s1.game_id,
        s1.line_type,
        s1.sportsbook,
        s1.source as source_1,
        s1.home_value as source_1_home,
        s1.away_value as source_1_away,
        s1.line_value as source_1_line,
        s1.source_reliability_score as source_1_reliability,
        s2.source as source_2,
        s2.home_value as source_2_home,
        s2.away_value as source_2_away,
        s2.line_value as source_2_line,
        s2.source_reliability_score as source_2_reliability,
        ABS(s1.home_value - s2.home_value) as home_variance,
        ABS(s1.away_value - s2.away_value) as away_variance,
        CASE 
            WHEN s1.line_value IS NOT NULL AND s2.line_value IS NOT NULL 
            THEN ABS(s1.line_value - s2.line_value)
            ELSE NULL
        END as line_variance
    FROM latest_by_source s1
    JOIN latest_by_source s2 ON 
        s1.game_id = s2.game_id 
        AND s1.line_type = s2.line_type
        AND s1.sportsbook = s2.sportsbook
        AND s1.source < s2.source  -- Avoid duplicate pairs
    WHERE s1.rn = 1 AND s2.rn = 1
)
SELECT 
    game_id,
    line_type,
    sportsbook,
    source_1,
    source_2,
    source_1_home,
    source_1_away,
    source_2_home,
    source_2_away,
    home_variance,
    away_variance,
    line_variance,
    -- Arbitrage opportunity scoring
    CASE 
        WHEN line_type = 'moneyline' AND (home_variance > 50 OR away_variance > 50) THEN 'HIGH'
        WHEN line_type = 'spread' AND (home_variance > 30 OR away_variance > 30) THEN 'HIGH'  
        WHEN line_type = 'total' AND (home_variance > 30 OR away_variance > 30) THEN 'HIGH'
        WHEN line_type = 'moneyline' AND (home_variance > 25 OR away_variance > 25) THEN 'MEDIUM'
        WHEN line_type = 'spread' AND (home_variance > 15 OR away_variance > 15) THEN 'MEDIUM'
        WHEN line_type = 'total' AND (home_variance > 15 OR away_variance > 15) THEN 'MEDIUM'
        ELSE 'LOW'
    END as arbitrage_opportunity,
    -- Reliability score for the comparison
    (source_1_reliability + source_2_reliability) / 2 as avg_reliability
FROM source_pairs
WHERE home_variance > 10 OR away_variance > 10 OR line_variance > 0.5
ORDER BY 
    CASE 
        WHEN line_type = 'moneyline' THEN GREATEST(home_variance, away_variance)
        ELSE GREATEST(home_variance, away_variance)
    END DESC;

-- =============================================================================
-- GAME COVERAGE ANALYSIS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.game_coverage_by_source AS
WITH game_source_coverage AS (
    SELECT 
        game_id,
        source,
        COUNT(DISTINCT line_type) as line_types_available,
        COUNT(DISTINCT sportsbook) as sportsbooks_available,
        MAX(odds_timestamp) as latest_update,
        AVG(source_reliability_score) as avg_reliability
    FROM analytics.unified_betting_lines
    WHERE DATE(odds_timestamp) = CURRENT_DATE
    GROUP BY game_id, source
),
game_totals AS (
    SELECT 
        game_id,
        COUNT(DISTINCT source) as total_sources,
        COUNT(DISTINCT sportsbook) as total_sportsbooks,
        COUNT(DISTINCT line_type) as total_line_types
    FROM analytics.unified_betting_lines
    WHERE DATE(odds_timestamp) = CURRENT_DATE
    GROUP BY game_id
)
SELECT 
    gsc.game_id,
    gsc.source,
    gsc.line_types_available,
    gsc.sportsbooks_available,
    gsc.latest_update,
    gsc.avg_reliability,
    gt.total_sources,
    gt.total_sportsbooks,
    gt.total_line_types,
    -- Coverage percentages
    ROUND((gsc.line_types_available::DECIMAL / gt.total_line_types) * 100, 1) as line_type_coverage_pct,
    ROUND((gsc.sportsbooks_available::DECIMAL / gt.total_sportsbooks) * 100, 1) as sportsbook_coverage_pct,
    -- Coverage quality score
    ROUND(
        (gsc.line_types_available::DECIMAL / 3.0 * 0.4 +  -- Max 3 line types (ML, spread, total)
         gsc.sportsbooks_available::DECIMAL / gt.total_sportsbooks * 0.4 +
         gsc.avg_reliability * 0.2) * 100, 1
    ) as coverage_quality_score
FROM game_source_coverage gsc
JOIN game_totals gt ON gsc.game_id = gt.game_id
ORDER BY gsc.game_id, coverage_quality_score DESC;

-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

COMMENT ON VIEW analytics.unified_betting_lines IS 
'Unified view combining all betting lines (moneyline, spread, total) from all sources with comprehensive source attribution and analytics fields';

COMMENT ON VIEW analytics.latest_lines_by_source IS 
'Latest betting lines for each game/sportsbook/source combination, ranked by timestamp and reliability';

COMMENT ON VIEW analytics.source_performance_dashboard IS 
'Real-time dashboard showing performance metrics, coverage, and data quality for each data source';

COMMENT ON VIEW analytics.cross_source_arbitrage IS 
'Cross-source comparison view for identifying arbitrage opportunities and significant price discrepancies';

COMMENT ON VIEW analytics.game_coverage_by_source IS 
'Analysis of data coverage by source for each game, including completeness and quality metrics';

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Indexes on the underlying tables are created in the migration files
-- These support the views' performance for common query patterns

-- Example usage queries included as comments:

/*
-- Query 1: All sources for a specific game (your original requirement)
SELECT 
    line_type,
    sportsbook,
    source,
    home_value,
    away_value,
    line_value,
    odds_timestamp,
    source_reliability_score
FROM analytics.unified_betting_lines 
WHERE game_id = 1003
ORDER BY source, line_type, odds_timestamp DESC;

-- Query 2: Compare sources for totals specifically  
SELECT 
    sportsbook,
    source,
    line_value as total_line,
    home_value as over_price,
    away_value as under_price,
    odds_timestamp
FROM analytics.unified_betting_lines 
WHERE game_id = 1003 
  AND line_type = 'total'
ORDER BY source, odds_timestamp DESC;

-- Query 3: Source reliability analysis
SELECT 
    source,
    COUNT(*) as total_lines,
    AVG(source_reliability_score) as avg_reliability,
    COUNT(DISTINCT sportsbook) as sportsbook_coverage,
    MAX(odds_timestamp) as latest_update
FROM analytics.unified_betting_lines 
WHERE game_id = 1003
GROUP BY source
ORDER BY avg_reliability DESC;

-- Query 4: Arbitrage opportunities for a game
SELECT *
FROM analytics.cross_source_arbitrage
WHERE game_id = 1003
  AND arbitrage_opportunity IN ('HIGH', 'MEDIUM')
ORDER BY 
    CASE WHEN arbitrage_opportunity = 'HIGH' THEN 1 ELSE 2 END,
    GREATEST(home_variance, away_variance) DESC;

-- Query 5: Source performance dashboard
SELECT *
FROM analytics.source_performance_dashboard
ORDER BY weighted_quality_score DESC;
*/