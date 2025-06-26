-- Timing Analysis Schema for Betting Recommendation Accuracy
-- This schema tracks the performance of betting recommendations based on timing relative to game start

CREATE SCHEMA IF NOT EXISTS timing_analysis;

-- Table for storing timing bucket performance metrics
CREATE TABLE IF NOT EXISTS timing_analysis.timing_bucket_performance (
    id SERIAL PRIMARY KEY,
    
    -- Analysis configuration
    timing_bucket VARCHAR(10) NOT NULL CHECK (timing_bucket IN ('0-2h', '2-6h', '6-24h', '24h+')),
    source VARCHAR(20) DEFAULT NULL, -- VSIN, SBD, NULL for all sources
    book VARCHAR(50) DEFAULT NULL,   -- DraftKings, Circa, etc., NULL for all books 
    split_type VARCHAR(20) DEFAULT NULL, -- moneyline, spread, total, NULL for all types
    strategy_name VARCHAR(100) DEFAULT NULL, -- NULL for all strategies
    
    -- Analysis period
    analysis_start_date DATE NOT NULL,
    analysis_end_date DATE NOT NULL,
    
    -- Basic performance metrics
    total_bets INTEGER NOT NULL DEFAULT 0 CHECK (total_bets >= 0),
    wins INTEGER NOT NULL DEFAULT 0 CHECK (wins >= 0),
    losses INTEGER NOT NULL DEFAULT 0 CHECK (losses >= 0),
    pushes INTEGER NOT NULL DEFAULT 0 CHECK (pushes >= 0),
    
    -- Financial metrics (using DECIMAL for precision)
    total_units_wagered DECIMAL(12,2) NOT NULL DEFAULT 0.00 CHECK (total_units_wagered >= 0),
    total_profit_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    
    -- Odds tracking
    avg_odds_at_recommendation DECIMAL(8,2) DEFAULT NULL,
    avg_closing_odds DECIMAL(8,2) DEFAULT NULL,
    
    -- Calculated metrics (stored for performance)
    win_rate DECIMAL(6,3) GENERATED ALWAYS AS (
        CASE WHEN total_bets > 0 THEN (wins::DECIMAL / total_bets::DECIMAL) * 100 ELSE 0 END
    ) STORED,
    
    roi_percentage DECIMAL(8,3) GENERATED ALWAYS AS (
        CASE WHEN total_units_wagered > 0 THEN (total_profit_loss / total_units_wagered) * 100 ELSE 0 END
    ) STORED,
    
    confidence_level VARCHAR(20) GENERATED ALWAYS AS (
        CASE 
            WHEN total_bets >= 100 THEN 'VERY_HIGH'
            WHEN total_bets >= 50 THEN 'HIGH'
            WHEN total_bets >= 20 THEN 'MODERATE'
            ELSE 'LOW'
        END
    ) STORED,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Constraint to ensure wins + losses + pushes don't exceed total
    CONSTRAINT valid_bet_counts CHECK (wins + losses + pushes <= total_bets)
    
    -- Note: Unique constraint created as separate index below due to COALESCE limitations
);

-- Comprehensive timing analysis results table
CREATE TABLE IF NOT EXISTS timing_analysis.comprehensive_analyses (
    id SERIAL PRIMARY KEY,
    
    -- Analysis identification
    analysis_name VARCHAR(200) NOT NULL,
    total_games_analyzed INTEGER NOT NULL DEFAULT 0 CHECK (total_games_analyzed >= 0),
    total_recommendations INTEGER NOT NULL DEFAULT 0 CHECK (total_recommendations >= 0),
    
    -- Analysis period
    analysis_start_date DATE NOT NULL,
    analysis_end_date DATE NOT NULL,
    
    -- Overall performance (same structure as bucket performance)
    overall_total_bets INTEGER NOT NULL DEFAULT 0 CHECK (overall_total_bets >= 0),
    overall_wins INTEGER NOT NULL DEFAULT 0 CHECK (overall_wins >= 0),
    overall_losses INTEGER NOT NULL DEFAULT 0 CHECK (overall_losses >= 0),
    overall_pushes INTEGER NOT NULL DEFAULT 0 CHECK (overall_pushes >= 0),
    overall_total_units_wagered DECIMAL(12,2) NOT NULL DEFAULT 0.00 CHECK (overall_total_units_wagered >= 0),
    overall_total_profit_loss DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    overall_avg_odds_at_recommendation DECIMAL(8,2) DEFAULT NULL,
    overall_avg_closing_odds DECIMAL(8,2) DEFAULT NULL,
    
    -- Overall calculated metrics
    overall_win_rate DECIMAL(6,3) GENERATED ALWAYS AS (
        CASE WHEN overall_total_bets > 0 THEN (overall_wins::DECIMAL / overall_total_bets::DECIMAL) * 100 ELSE 0 END
    ) STORED,
    
    overall_roi_percentage DECIMAL(8,3) GENERATED ALWAYS AS (
        CASE WHEN overall_total_units_wagered > 0 THEN (overall_total_profit_loss / overall_total_units_wagered) * 100 ELSE 0 END
    ) STORED,
    
    -- Best performing configurations
    best_bucket VARCHAR(10) DEFAULT NULL,
    best_source VARCHAR(20) DEFAULT NULL,
    best_strategy VARCHAR(100) DEFAULT NULL,
    
    -- Trend analysis (stored as JSON)
    trends JSONB DEFAULT '{}',
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Constraint for overall bet counts
    CONSTRAINT valid_overall_bet_counts CHECK (overall_wins + overall_losses + overall_pushes <= overall_total_bets)
);

-- Realtime timing recommendations cache table
CREATE TABLE IF NOT EXISTS timing_analysis.timing_recommendations_cache (
    id SERIAL PRIMARY KEY,
    
    -- Lookup parameters (used as cache key)
    timing_bucket VARCHAR(10) NOT NULL,
    source VARCHAR(20) DEFAULT NULL,
    book VARCHAR(50) DEFAULT NULL,
    split_type VARCHAR(20) NOT NULL,
    strategy_name VARCHAR(100) DEFAULT NULL,
    
    -- Cached recommendation
    recommendation TEXT NOT NULL,
    confidence VARCHAR(50) NOT NULL,
    expected_win_rate DECIMAL(6,3) DEFAULT NULL,
    expected_roi DECIMAL(8,3) DEFAULT NULL,
    risk_factors JSONB DEFAULT '[]',
    sample_size_warning BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Historical metrics used for recommendation
    historical_total_bets INTEGER DEFAULT NULL,
    historical_win_rate DECIMAL(6,3) DEFAULT NULL,
    historical_roi DECIMAL(8,3) DEFAULT NULL,
    
    -- Cache management
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (NOW() + INTERVAL '1 hour'),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    
    -- Note: Unique constraint created as separate index below due to COALESCE limitations
);

-- Historical recommendation tracking table (for building timing analysis data)
CREATE TABLE IF NOT EXISTS timing_analysis.recommendation_history (
    id SERIAL PRIMARY KEY,
    
    -- Recommendation identification
    game_id VARCHAR(50) NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Recommendation details
    recommendation_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    hours_until_game DECIMAL(8,3) NOT NULL,
    timing_bucket VARCHAR(10) GENERATED ALWAYS AS (
        CASE 
            WHEN hours_until_game <= 2 THEN '0-2h'
            WHEN hours_until_game <= 6 THEN '2-6h' 
            WHEN hours_until_game <= 24 THEN '6-24h'
            ELSE '24h+'
        END
    ) STORED,
    
    source VARCHAR(20) NOT NULL,
    book VARCHAR(50) DEFAULT NULL,
    split_type VARCHAR(20) NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    
    -- Betting details
    recommended_side VARCHAR(50) NOT NULL, -- "home", "away", "over", "under", etc.
    odds_at_recommendation DECIMAL(8,2) DEFAULT NULL,
    closing_odds DECIMAL(8,2) DEFAULT NULL,
    
    -- Outcome (populated after game completion)
    outcome VARCHAR(10) DEFAULT NULL CHECK (outcome IS NULL OR outcome IN ('win', 'loss', 'push')),
    actual_profit_loss DECIMAL(8,2) DEFAULT NULL,
    units_wagered DECIMAL(6,2) DEFAULT 1.00, -- Default to 1 unit
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    
    -- Note: Unique constraint created as separate index below due to COALESCE limitations
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_timing_bucket_performance_lookup 
ON timing_analysis.timing_bucket_performance(timing_bucket, source, book, split_type, strategy_name);

CREATE INDEX IF NOT EXISTS idx_timing_bucket_performance_period
ON timing_analysis.timing_bucket_performance(analysis_start_date, analysis_end_date);

CREATE INDEX IF NOT EXISTS idx_timing_bucket_performance_roi
ON timing_analysis.timing_bucket_performance(roi_percentage DESC);

CREATE INDEX IF NOT EXISTS idx_comprehensive_analyses_name
ON timing_analysis.comprehensive_analyses(analysis_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_timing_recommendations_cache_lookup
ON timing_analysis.timing_recommendations_cache(timing_bucket, source, book, split_type, strategy_name);

CREATE INDEX IF NOT EXISTS idx_timing_recommendations_cache_expiry
ON timing_analysis.timing_recommendations_cache(expires_at);

CREATE INDEX IF NOT EXISTS idx_recommendation_history_game
ON timing_analysis.recommendation_history(game_id, game_datetime);

CREATE INDEX IF NOT EXISTS idx_recommendation_history_timing
ON timing_analysis.recommendation_history(timing_bucket, source, split_type, strategy_name);

CREATE INDEX IF NOT EXISTS idx_recommendation_history_outcome
ON timing_analysis.recommendation_history(outcome, hours_until_game);

-- Unique indexes to replace UNIQUE constraints with COALESCE
CREATE UNIQUE INDEX IF NOT EXISTS uniq_timing_bucket_analysis
ON timing_analysis.timing_bucket_performance(
    timing_bucket, 
    COALESCE(source, ''), 
    COALESCE(book, ''), 
    COALESCE(split_type, ''), 
    COALESCE(strategy_name, ''),
    analysis_start_date, 
    analysis_end_date
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_timing_cache_key
ON timing_analysis.timing_recommendations_cache(
    timing_bucket,
    COALESCE(source, ''),
    COALESCE(book, ''),
    split_type,
    COALESCE(strategy_name, '')
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_recommendation_tracking
ON timing_analysis.recommendation_history(
    game_id,
    source,
    COALESCE(book, ''),
    split_type,
    strategy_name,
    recommendation_datetime
);

-- Views for common queries

-- View for current timing performance across all buckets
CREATE OR REPLACE VIEW timing_analysis.current_timing_performance AS
SELECT 
    timing_bucket,
    source,
    book,
    split_type,
    strategy_name,
    total_bets,
    win_rate,
    roi_percentage,
    confidence_level,
    avg_odds_at_recommendation,
    avg_closing_odds,
    (avg_closing_odds - avg_odds_at_recommendation) as avg_odds_movement,
    updated_at,
    -- Performance grade
    CASE 
        WHEN win_rate >= 60 AND roi_percentage >= 10 THEN 'EXCELLENT'
        WHEN win_rate >= 55 AND roi_percentage >= 5 THEN 'GOOD'
        WHEN win_rate >= 52 AND roi_percentage >= 0 THEN 'PROFITABLE'
        WHEN win_rate >= 50 THEN 'BREAKEVEN'
        ELSE 'UNPROFITABLE'
    END as performance_grade,
    -- Recommendation confidence
    CASE 
        WHEN confidence_level IN ('HIGH', 'VERY_HIGH') AND win_rate >= 55 AND roi_percentage >= 5 THEN 'HIGH_CONFIDENCE'
        WHEN confidence_level IN ('MODERATE', 'HIGH', 'VERY_HIGH') AND win_rate >= 52 AND roi_percentage >= 0 THEN 'MODERATE_CONFIDENCE'
        WHEN confidence_level = 'LOW' THEN 'INSUFFICIENT_DATA'
        ELSE 'LOW_CONFIDENCE'
    END as recommendation_confidence
FROM timing_analysis.timing_bucket_performance
WHERE analysis_end_date >= CURRENT_DATE - INTERVAL '30 days'  -- Recent data only
ORDER BY roi_percentage DESC, win_rate DESC;

-- View for best timing recommendations by category
CREATE OR REPLACE VIEW timing_analysis.best_timing_by_category AS
WITH ranked_performance AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY source, split_type, strategy_name ORDER BY roi_percentage DESC, win_rate DESC) as rank
    FROM timing_analysis.current_timing_performance
    WHERE confidence_level IN ('HIGH', 'VERY_HIGH')
      AND total_bets >= 20
)
SELECT 
    source,
    split_type,
    strategy_name,
    timing_bucket as best_timing_bucket,
    win_rate as best_win_rate,
    roi_percentage as best_roi,
    total_bets as sample_size,
    performance_grade,
    recommendation_confidence
FROM ranked_performance
WHERE rank = 1
  AND performance_grade IN ('EXCELLENT', 'GOOD', 'PROFITABLE')
ORDER BY roi_percentage DESC;

-- Function to clean up expired cache entries
CREATE OR REPLACE FUNCTION timing_analysis.cleanup_expired_cache()
RETURNS void AS $$
DECLARE
    var_count INTEGER;
BEGIN
    -- Delete expired cache entries
    DELETE FROM timing_analysis.timing_recommendations_cache 
    WHERE expires_at < NOW();
    
    -- Log cleanup
    GET DIAGNOSTICS var_count = ROW_COUNT;
    IF var_count > 0 THEN
        RAISE NOTICE 'Cleaned up % expired timing recommendation cache entries', var_count;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to update recommendation outcomes
CREATE OR REPLACE FUNCTION timing_analysis.update_recommendation_outcomes()
RETURNS void AS $$
DECLARE
    var_count INTEGER;
BEGIN
    -- Update recommendation outcomes based on game_outcome table
    -- This assumes game_outcome table exists with proper structure
    UPDATE timing_analysis.recommendation_history rh
    SET 
        outcome = CASE 
            WHEN go.outcome IS NULL THEN NULL
            -- For moneyline bets
            WHEN rh.split_type = 'moneyline' AND rh.recommended_side = 'home' THEN 
                CASE WHEN go.home_win THEN 'win' ELSE 'loss' END
            WHEN rh.split_type = 'moneyline' AND rh.recommended_side = 'away' THEN 
                CASE WHEN NOT go.home_win THEN 'win' ELSE 'loss' END
            -- For total bets  
            WHEN rh.split_type = 'total' AND rh.recommended_side = 'over' THEN
                CASE WHEN go.over THEN 'win' ELSE 'loss' END
            WHEN rh.split_type = 'total' AND rh.recommended_side = 'under' THEN
                CASE WHEN NOT go.over THEN 'win' ELSE 'loss' END
            -- For spread bets (if home_cover_spread is available)
            WHEN rh.split_type = 'spread' AND rh.recommended_side = 'home' THEN
                CASE WHEN go.home_cover_spread THEN 'win' ELSE 'loss' END
            WHEN rh.split_type = 'spread' AND rh.recommended_side = 'away' THEN
                CASE WHEN NOT go.home_cover_spread THEN 'win' ELSE 'loss' END
            ELSE 'loss' -- Default case
        END,
        updated_at = NOW()
    FROM (
        SELECT DISTINCT game_id, home_win, over, home_cover_spread, 
               CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 'completed' ELSE NULL END as outcome
        FROM public.game_outcomes
    ) go
    WHERE rh.game_id = go.game_id
      AND rh.outcome IS NULL  -- Only update unresolved recommendations
      AND go.outcome = 'completed';  -- Only for completed games
      
    -- Calculate profit/loss for resolved bets
    UPDATE timing_analysis.recommendation_history
    SET actual_profit_loss = CASE 
        WHEN outcome = 'win' AND odds_at_recommendation IS NOT NULL THEN
            -- Calculate profit based on American odds
            CASE 
                WHEN odds_at_recommendation > 0 THEN units_wagered * (odds_at_recommendation / 100)
                ELSE units_wagered * (100 / ABS(odds_at_recommendation))
            END
        WHEN outcome = 'loss' THEN -units_wagered
        WHEN outcome = 'push' THEN 0
        ELSE NULL
    END
    WHERE outcome IS NOT NULL AND actual_profit_loss IS NULL;
    
    -- Log updates
    GET DIAGNOSTICS var_count = ROW_COUNT;
    IF var_count > 0 THEN
        RAISE NOTICE 'Updated outcomes for % recommendation history records', var_count;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON SCHEMA timing_analysis IS 'Schema for tracking betting recommendation timing accuracy and performance';
COMMENT ON TABLE timing_analysis.timing_bucket_performance IS 'Performance metrics aggregated by timing buckets and filters';
COMMENT ON TABLE timing_analysis.comprehensive_analyses IS 'Complete timing analysis results across all buckets';
COMMENT ON TABLE timing_analysis.timing_recommendations_cache IS 'Cache for real-time timing recommendations';
COMMENT ON TABLE timing_analysis.recommendation_history IS 'Historical record of all recommendations with timing and outcomes'; 