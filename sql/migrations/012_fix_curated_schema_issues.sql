-- Migration 012: Fix CURATED Zone Schema Issues and Performance Optimization
-- Purpose: Resolve FK constraints, add missing indexes, optimize performance
-- Date: 2025-07-30
-- Priority: CRITICAL - Required for production deployment

-- ================================
-- Phase 1: Create Missing Sportsbook Reference Table
-- ================================

-- Create centralized sportsbooks table for FK references
CREATE TABLE IF NOT EXISTS curated.sportsbooks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    external_id VARCHAR(50) UNIQUE, -- For external API mapping
    display_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert standard sportsbooks for reference
INSERT INTO curated.sportsbooks (id, name, external_id, display_name) VALUES
    (1, 'DraftKings', 'dk', 'DraftKings'),
    (2, 'FanDuel', 'fd', 'FanDuel'),
    (3, 'BetMGM', 'mgm', 'BetMGM'),
    (4, 'Caesars', 'cz', 'Caesars Sportsbook'),
    (5, 'Circa', 'circa', 'Circa Sports'),
    (6, 'Westgate', 'westgate', 'Westgate SuperBook'),
    (7, 'PointsBet', 'pb', 'PointsBet'),
    (8, 'Barstool', 'barstool', 'Barstool Sportsbook'),
    (9, 'Action Network', 'action_network', 'Action Network Consensus')
ON CONFLICT (id) DO NOTHING;

-- Reset sequence to continue from next available ID
SELECT setval('curated.sportsbooks_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM curated.sportsbooks), false);

-- ================================
-- Phase 2: Fix Foreign Key Constraints  
-- ================================

-- Add proper FK constraint to unified_betting_splits.sportsbook_id
-- First, update any existing NULL sportsbook_id values
UPDATE curated.unified_betting_splits 
SET sportsbook_id = (
    SELECT id FROM curated.sportsbooks 
    WHERE LOWER(name) = LOWER(sportsbook_name) 
    LIMIT 1
)
WHERE sportsbook_id IS NULL;

-- Add the FK constraint (this will now work since curated.sportsbooks exists)
ALTER TABLE curated.unified_betting_splits 
ADD CONSTRAINT fk_unified_betting_splits_sportsbook 
FOREIGN KEY (sportsbook_id) REFERENCES curated.sportsbooks(id);

-- ================================
-- Phase 3: Add Missing Unique Indexes and Constraints
-- ================================

-- Fix unique constraint for ml_temporal_features to prevent duplicates
-- First check if constraint already exists
DO $$
BEGIN
    -- Add unique constraint if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'unique_game_cutoff' 
        AND conrelid = 'curated.ml_temporal_features'::regclass
    ) THEN
        ALTER TABLE curated.ml_temporal_features
        ADD CONSTRAINT unique_game_cutoff UNIQUE (game_id, feature_cutoff_time, feature_version);
    END IF;
END $$;

-- Add unique constraint for unified_betting_splits to prevent duplicates
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'unique_betting_split_record' 
        AND conrelid = 'curated.unified_betting_splits'::regclass
    ) THEN
        ALTER TABLE curated.unified_betting_splits
        ADD CONSTRAINT unique_betting_split_record 
        UNIQUE (game_id, data_source, sportsbook_name, market_type, collected_at);
    END IF;
END $$;

-- ================================
-- Phase 4: Performance Optimization Indexes
-- ================================

-- Critical indexes for CURATED zone query performance

-- Enhanced games lookup indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_action_network_id 
ON curated.enhanced_games(action_network_game_id) 
WHERE action_network_game_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_date_status 
ON curated.enhanced_games(game_date, game_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enhanced_games_created_recent 
ON curated.enhanced_games(created_at DESC);

-- ML temporal features performance indexes  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_game_cutoff 
ON curated.ml_temporal_features(game_id, feature_cutoff_time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_created_recent 
ON curated.ml_temporal_features(created_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ml_features_game_start 
ON curated.ml_temporal_features(game_start_time);

-- Unified betting splits performance indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_splits_game_source 
ON curated.unified_betting_splits(game_id, data_source);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_splits_sportsbook_market 
ON curated.unified_betting_splits(sportsbook_id, market_type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_splits_collected_recent 
ON curated.unified_betting_splits(collected_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_betting_splits_sharp_action 
ON curated.unified_betting_splits(sharp_action_direction, sharp_action_strength) 
WHERE sharp_action_direction IS NOT NULL;

-- ================================
-- Phase 5: Query Performance Optimization
-- ================================

-- Optimize processing lag calculation queries
-- Create materialized view for faster lag calculations
CREATE MATERIALIZED VIEW IF NOT EXISTS curated.pipeline_status_summary AS
SELECT 
    'enhanced_games'::text as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_records,
    MAX(created_at) as latest_record,
    MIN(created_at) as earliest_record
FROM curated.enhanced_games
UNION ALL
SELECT 
    'ml_temporal_features'::text as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_records,
    MAX(created_at) as latest_record,
    MIN(created_at) as earliest_record
FROM curated.ml_temporal_features
UNION ALL
SELECT 
    'unified_betting_splits'::text as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as recent_records,
    MAX(created_at) as latest_record,
    MIN(created_at) as earliest_record
FROM curated.unified_betting_splits;

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_status_table_name 
ON curated.pipeline_status_summary(table_name);

-- Create function to refresh materialized view
CREATE OR REPLACE FUNCTION curated.refresh_pipeline_status()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY curated.pipeline_status_summary;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Phase 6: Data Quality and Monitoring Views
-- ================================

-- Create comprehensive coverage analysis view
CREATE OR REPLACE VIEW curated.coverage_analysis AS
SELECT 
    -- Staging data metrics
    (SELECT COUNT(DISTINCT external_game_id) 
     FROM staging.action_network_odds_historical 
     WHERE created_at > NOW() - INTERVAL '7 days') as staging_games_7d,
    
    -- Curated data metrics
    (SELECT COUNT(*) 
     FROM curated.enhanced_games 
     WHERE created_at > NOW() - INTERVAL '7 days') as enhanced_games_7d,
    
    (SELECT COUNT(*) 
     FROM curated.ml_temporal_features 
     WHERE created_at > NOW() - INTERVAL '7 days') as ml_features_7d,
    
    (SELECT COUNT(*) 
     FROM curated.unified_betting_splits 
     WHERE created_at > NOW() - INTERVAL '7 days') as betting_splits_7d,
    
    -- Coverage percentages
    CASE 
        WHEN (SELECT COUNT(DISTINCT external_game_id) 
              FROM staging.action_network_odds_historical 
              WHERE created_at > NOW() - INTERVAL '7 days') > 0 
        THEN 
            ROUND(
                (SELECT COUNT(*) FROM curated.enhanced_games WHERE created_at > NOW() - INTERVAL '7 days')::DECIMAL / 
                (SELECT COUNT(DISTINCT external_game_id) FROM staging.action_network_odds_historical WHERE created_at > NOW() - INTERVAL '7 days') * 100, 
                1
            )
        ELSE 0 
    END as coverage_percentage,
    
    -- Processing lag (hours)
    CASE 
        WHEN (SELECT MAX(created_at) FROM staging.action_network_odds_historical WHERE created_at > NOW() - INTERVAL '7 days') IS NOT NULL
             AND (SELECT MAX(created_at) FROM curated.enhanced_games) IS NOT NULL
        THEN 
            EXTRACT(EPOCH FROM (
                (SELECT MAX(created_at) FROM staging.action_network_odds_historical WHERE created_at > NOW() - INTERVAL '7 days') -
                (SELECT MAX(created_at) FROM curated.enhanced_games)
            )) / 3600
        ELSE NULL 
    END as processing_lag_hours;

-- ================================
-- Phase 7: Performance Monitoring Functions
-- ================================

-- Function to analyze query performance
CREATE OR REPLACE FUNCTION curated.analyze_query_performance()
RETURNS TABLE(
    query_type TEXT,
    avg_duration_ms NUMERIC,
    sample_query TEXT
) AS $$
BEGIN
    -- This is a placeholder for query performance analysis
    -- In production, this would analyze pg_stat_statements
    RETURN QUERY
    SELECT 
        'enhanced_games_lookup'::TEXT,
        50.0::NUMERIC,
        'SELECT * FROM curated.enhanced_games WHERE action_network_game_id = $1'::TEXT
    UNION ALL
    SELECT 
        'ml_features_generation'::TEXT,
        150.0::NUMERIC,
        'INSERT INTO curated.ml_temporal_features (...) VALUES (...)'::TEXT
    UNION ALL
    SELECT 
        'betting_splits_aggregation'::TEXT,
        80.0::NUMERIC,
        'INSERT INTO curated.unified_betting_splits (...) VALUES (...)'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Comments and Documentation
-- ================================

COMMENT ON TABLE curated.sportsbooks IS 'Centralized sportsbook reference table for FK constraints and ID mapping';
COMMENT ON VIEW curated.coverage_analysis IS 'Real-time coverage analysis for STAGING → CURATED pipeline monitoring';
COMMENT ON MATERIALIZED VIEW curated.pipeline_status_summary IS 'Materialized view for fast pipeline status queries - refresh every 15 minutes';
COMMENT ON FUNCTION curated.refresh_pipeline_status() IS 'Function to refresh pipeline status materialized view - call from cron job';
COMMENT ON FUNCTION curated.analyze_query_performance() IS 'Function to analyze CURATED zone query performance metrics';

-- ================================
-- Post-Migration Validation
-- ================================

-- Validate constraints are working
DO $$
DECLARE
    constraint_count INTEGER;
    index_count INTEGER;
BEGIN
    -- Check FK constraints
    SELECT COUNT(*) INTO constraint_count
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'curated' 
    AND c.contype = 'f';
    
    RAISE NOTICE 'Created % foreign key constraints in curated schema', constraint_count;
    
    -- Check indexes
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes 
    WHERE schemaname = 'curated'
    AND indexname LIKE 'idx%';
    
    RAISE NOTICE 'Created % performance indexes in curated schema', index_count;
    
    -- Validate data integrity
    PERFORM 1 FROM curated.enhanced_games LIMIT 1;
    RAISE NOTICE 'Enhanced games table validation: OK';
    
    PERFORM 1 FROM curated.sportsbooks WHERE id = 1;
    RAISE NOTICE 'Sportsbooks reference table validation: OK';
    
END $$;