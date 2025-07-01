-- Enhanced Backtesting Schema for Improved Methodology
-- 
-- This schema addresses the requirements from the enhanced backtesting methodology:
-- 1. Integration of bet losses across multiple backtesting components
-- 2. Proper tracking of recommendations in database tables
-- 3. Alignment of all backtesting metrics with actual user recommendations
-- 4. Enhanced pre-game email notification tracking

-- ==============================================================================
-- ENHANCED TRACKING SCHEMA
-- ==============================================================================

-- Create enhanced tracking schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS tracking;

-- Enhanced pre-game recommendations table with better loss tracking
DROP TABLE IF EXISTS tracking.pre_game_recommendations CASCADE;
CREATE TABLE tracking.pre_game_recommendations (
    -- Primary identification
    recommendation_id VARCHAR PRIMARY KEY,
    game_pk INTEGER NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Recommendation details
    recommendation TEXT NOT NULL,
    bet_type VARCHAR NOT NULL CHECK (bet_type IN ('moneyline', 'spread', 'total', 'prop')),
    confidence_level VARCHAR NOT NULL CHECK (confidence_level IN ('HIGH', 'MODERATE', 'LOW')),
    signal_source VARCHAR NOT NULL,
    signal_strength DOUBLE PRECISION NOT NULL CHECK (signal_strength >= 0 AND signal_strength <= 1),
    
    -- Betting details
    recommended_odds DECIMAL(10,2), -- American odds format (-110, +150, etc.)
    recommended_bet_size DECIMAL(10,2) DEFAULT 100.00, -- Unit size in dollars
    bet_side VARCHAR, -- 'home', 'away', 'over', 'under', team name
    
    -- Timing
    recommended_at TIMESTAMP WITH TIME ZONE NOT NULL,
    email_sent BOOLEAN DEFAULT TRUE,
    notification_sent_at TIMESTAMP WITH TIME ZONE,
    
    -- Enhanced outcome tracking
    game_completed BOOLEAN DEFAULT FALSE,
    bet_won BOOLEAN,
    actual_outcome TEXT,
    final_score VARCHAR, -- e.g., "5-3"
    
    -- Enhanced profit/loss tracking
    profit_loss DECIMAL(12,2), -- Net profit/loss for this bet
    gross_profit DECIMAL(12,2), -- Gross profit (if won)
    total_loss DECIMAL(12,2), -- Total loss (if lost)
    
    -- Backtesting alignment
    backtesting_strategy_used VARCHAR, -- Which strategy generated this
    backtesting_confidence_score DECIMAL(5,3), -- Confidence from backtesting
    live_vs_backtest_variance DECIMAL(8,4), -- Difference between live and backtest confidence
    
    -- Enhanced metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    outcome_updated_at TIMESTAMP WITH TIME ZONE,
    
    -- Source tracking for alignment analysis
    source_component VARCHAR DEFAULT 'live_recommendation' CHECK (source_component IN ('live_recommendation', 'backtesting', 'manual', 'test')),
    evaluation_method VARCHAR DEFAULT 'standard' -- Which evaluation logic was used
);

-- Create indexes for better performance
CREATE INDEX idx_pre_game_recommendations_game_datetime ON tracking.pre_game_recommendations(game_datetime DESC);
CREATE INDEX idx_pre_game_recommendations_game_pk ON tracking.pre_game_recommendations(game_pk);
CREATE INDEX idx_pre_game_recommendations_bet_type ON tracking.pre_game_recommendations(bet_type);
CREATE INDEX idx_pre_game_recommendations_signal_source ON tracking.pre_game_recommendations(signal_source);
CREATE INDEX idx_pre_game_recommendations_completed ON tracking.pre_game_recommendations(game_completed, bet_won);
CREATE INDEX idx_pre_game_recommendations_source ON tracking.pre_game_recommendations(source_component);

-- ==============================================================================
-- ENHANCED BACKTESTING SCHEMA
-- ==============================================================================

-- Enhanced backtesting schema
CREATE SCHEMA IF NOT EXISTS backtesting;

-- Enhanced strategy performance table with better loss tracking
DROP TABLE IF EXISTS backtesting.strategy_performance CASCADE;
CREATE TABLE backtesting.strategy_performance (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    source_book_type VARCHAR(50),
    split_type VARCHAR(50),
    backtest_date DATE NOT NULL,
    
    -- Basic performance metrics
    win_rate DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    roi_per_100 DECIMAL(10,2) NOT NULL DEFAULT 0.0,
    total_bets INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    pushes INTEGER DEFAULT 0,
    
    -- Enhanced profit/loss tracking
    total_profit_loss DECIMAL(12,2) NOT NULL DEFAULT 0.0,
    total_gross_profit DECIMAL(12,2) DEFAULT 0.0, -- Sum of all winning bets
    total_gross_losses DECIMAL(12,2) DEFAULT 0.0, -- Sum of all losing bets (positive number)
    average_winning_bet DECIMAL(10,2) DEFAULT 0.0,
    average_losing_bet DECIMAL(10,2) DEFAULT 0.0,
    
    -- Advanced metrics
    sharpe_ratio DECIMAL(8,4) DEFAULT NULL,
    max_drawdown DECIMAL(8,4) DEFAULT NULL,
    kelly_criterion DECIMAL(8,4) DEFAULT NULL,
    consecutive_wins INTEGER DEFAULT 0,
    consecutive_losses INTEGER DEFAULT 0,
    
    -- Confidence and quality metrics
    confidence_level VARCHAR(20) NOT NULL DEFAULT 'LOW',
    sample_size_adequate BOOLEAN DEFAULT FALSE,
    statistical_significance BOOLEAN DEFAULT FALSE,
    p_value DECIMAL(10,8) DEFAULT 1.0,
    confidence_interval_lower DECIMAL(8,4) DEFAULT NULL,
    confidence_interval_upper DECIMAL(8,4) DEFAULT NULL,
    
    -- Live alignment tracking
    live_recommendations_count INTEGER DEFAULT 0, -- How many live recommendations used this strategy
    live_vs_backtest_win_rate_diff DECIMAL(8,4) DEFAULT 0.0,
    live_vs_backtest_roi_diff DECIMAL(8,4) DEFAULT 0.0,
    alignment_score DECIMAL(5,2) DEFAULT NULL, -- 0-100 score
    
    -- Metadata
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT strategy_performance_unique_key UNIQUE (strategy_name, source_book_type, split_type, backtest_date),
    CONSTRAINT valid_win_rate CHECK (win_rate >= 0 AND win_rate <= 1),
    CONSTRAINT valid_alignment_score CHECK (alignment_score IS NULL OR (alignment_score >= 0 AND alignment_score <= 100))
);

-- Enhanced indexes
CREATE INDEX idx_strategy_performance_name_date ON backtesting.strategy_performance(strategy_name, backtest_date DESC);
CREATE INDEX idx_strategy_performance_roi ON backtesting.strategy_performance(roi_per_100 DESC);
CREATE INDEX idx_strategy_performance_alignment ON backtesting.strategy_performance(alignment_score DESC NULLS LAST);
CREATE INDEX idx_strategy_performance_updated ON backtesting.strategy_performance(last_updated DESC);

-- Backtesting-Live Alignment Analysis table
CREATE TABLE IF NOT EXISTS backtesting.alignment_analysis (
    id SERIAL PRIMARY KEY,
    alignment_date TIMESTAMP WITH TIME ZONE NOT NULL,
    period_analyzed TEXT NOT NULL,
    
    -- Win rate comparison
    backtesting_win_rate DECIMAL(5,4) NOT NULL,
    live_win_rate DECIMAL(5,4) NOT NULL,
    win_rate_difference DECIMAL(5,4) NOT NULL,
    
    -- ROI comparison
    backtesting_roi DECIMAL(8,2) NOT NULL,
    live_roi DECIMAL(8,2) NOT NULL,
    roi_difference DECIMAL(8,2) NOT NULL,
    
    -- Bet count comparison
    backtesting_total_bets INTEGER NOT NULL,
    live_total_bets INTEGER NOT NULL,
    bet_count_difference INTEGER NOT NULL,
    
    -- Enhanced loss comparison
    backtesting_total_losses DECIMAL(12,2) NOT NULL,
    live_total_losses DECIMAL(12,2) NOT NULL,
    loss_difference DECIMAL(12,2) NOT NULL,
    backtesting_avg_loss DECIMAL(10,2) DEFAULT 0.0,
    live_avg_loss DECIMAL(10,2) DEFAULT 0.0,
    
    -- Overall alignment
    alignment_score DECIMAL(5,2) NOT NULL CHECK (alignment_score >= 0 AND alignment_score <= 100),
    
    -- Issues and recommendations
    discrepancies JSONB, -- Array of discrepancy descriptions
    recommendations JSONB, -- Array of recommended actions
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_alignment_analysis_date ON backtesting.alignment_analysis(alignment_date DESC);
CREATE INDEX idx_alignment_analysis_score ON backtesting.alignment_analysis(alignment_score DESC);

-- Unified bet outcomes table for cross-component tracking
CREATE TABLE IF NOT EXISTS backtesting.unified_bet_outcomes (
    id SERIAL PRIMARY KEY,
    outcome_id VARCHAR UNIQUE NOT NULL, -- Links to recommendation_id or generated ID
    
    -- Game information
    game_pk INTEGER NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    
    -- Bet details
    bet_type VARCHAR NOT NULL,
    bet_side VARCHAR NOT NULL,
    bet_amount DECIMAL(10,2) NOT NULL DEFAULT 100.00,
    odds DECIMAL(10,2) NOT NULL DEFAULT -110.00,
    
    -- Outcome
    bet_won BOOLEAN,
    actual_profit_loss DECIMAL(12,2),
    game_final_score VARCHAR,
    outcome_details TEXT,
    
    -- Source tracking
    source_component VARCHAR NOT NULL CHECK (source_component IN ('backtesting', 'live_recommendation', 'diagnostics', 'manual')),
    evaluation_method VARCHAR NOT NULL,
    
    -- Timing
    bet_placed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    outcome_determined_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_unified_bet_outcomes_game_pk ON backtesting.unified_bet_outcomes(game_pk);
CREATE INDEX idx_unified_bet_outcomes_source ON backtesting.unified_bet_outcomes(source_component);
CREATE INDEX idx_unified_bet_outcomes_game_datetime ON backtesting.unified_bet_outcomes(game_datetime DESC);

-- Enhanced notification tracking
CREATE TABLE IF NOT EXISTS tracking.notification_log (
    id SERIAL PRIMARY KEY,
    notification_id VARCHAR UNIQUE NOT NULL,
    game_pk INTEGER NOT NULL,
    notification_type VARCHAR NOT NULL CHECK (notification_type IN ('pre_game_5min', 'pre_game_15min', 'post_game', 'alert')),
    
    -- Timing accuracy
    scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL,
    actual_sent_time TIMESTAMP WITH TIME ZONE,
    timing_difference_seconds INTEGER, -- Positive = late, negative = early
    
    -- Content tracking
    recipient_count INTEGER NOT NULL DEFAULT 0,
    recommendations_count INTEGER NOT NULL DEFAULT 0,
    email_content_hash VARCHAR, -- For deduplication
    
    -- Success tracking
    notification_sent BOOLEAN DEFAULT FALSE,
    delivery_confirmed BOOLEAN DEFAULT FALSE,
    database_logged BOOLEAN DEFAULT FALSE,
    
    -- Error tracking
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Performance metrics
    processing_time_seconds DECIMAL(8,3),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_notification_log_game_pk ON tracking.notification_log(game_pk);
CREATE INDEX idx_notification_log_scheduled_time ON tracking.notification_log(scheduled_time DESC);
CREATE INDEX idx_notification_log_type ON tracking.notification_log(notification_type);

-- ==============================================================================
-- ENHANCED VIEWS FOR ANALYSIS
-- ==============================================================================

-- Enhanced strategy performance summary with live alignment
CREATE OR REPLACE VIEW backtesting.enhanced_strategy_performance_summary AS
SELECT 
    strategy_name,
    source_book_type,
    split_type,
    
    -- Basic performance
    COUNT(*) as total_backtests,
    AVG(win_rate) as avg_win_rate,
    AVG(roi_per_100) as avg_roi_per_100,
    SUM(total_bets) as total_bets_all_time,
    SUM(wins) as total_wins,
    SUM(losses) as total_losses,
    
    -- Enhanced loss tracking
    SUM(total_gross_losses) as total_losses_sum,
    AVG(total_gross_losses) as avg_losses_per_backtest,
    SUM(total_gross_profit) as total_profits_sum,
    SUM(total_profit_loss) as net_profit_loss,
    
    -- Live alignment metrics
    AVG(live_vs_backtest_win_rate_diff) as avg_win_rate_alignment,
    AVG(live_vs_backtest_roi_diff) as avg_roi_alignment,
    AVG(alignment_score) as avg_alignment_score,
    
    -- Advanced metrics
    AVG(sharpe_ratio) as avg_sharpe_ratio,
    AVG(max_drawdown) as avg_max_drawdown,
    MAX(last_updated) as last_analyzed,
    
    -- Performance classification
    CASE 
        WHEN AVG(win_rate) > 0.65 AND AVG(alignment_score) > 85 THEN 'EXCELLENT_ALIGNED'
        WHEN AVG(win_rate) > 0.65 THEN 'EXCELLENT_UNALIGNED'
        WHEN AVG(win_rate) > 0.58 AND AVG(alignment_score) > 80 THEN 'GOOD_ALIGNED'
        WHEN AVG(win_rate) > 0.58 THEN 'GOOD_UNALIGNED'
        WHEN AVG(win_rate) > 0.52 AND AVG(alignment_score) > 70 THEN 'PROFITABLE_ALIGNED'
        WHEN AVG(win_rate) > 0.52 THEN 'PROFITABLE_UNALIGNED'
        ELSE 'UNPROFITABLE'
    END as performance_grade,
    
    -- Activity status
    CASE 
        WHEN MAX(last_updated) > NOW() - INTERVAL '7 days' THEN 'ACTIVE'
        WHEN MAX(last_updated) > NOW() - INTERVAL '30 days' THEN 'INACTIVE'
        ELSE 'STALE'
    END as activity_status
    
FROM backtesting.strategy_performance 
WHERE backtest_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY strategy_name, source_book_type, split_type
HAVING SUM(total_bets) >= 5
ORDER BY avg_roi_per_100 DESC, avg_alignment_score DESC NULLS LAST;

-- Live vs Backtesting Performance Comparison View
CREATE OR REPLACE VIEW backtesting.live_vs_backtest_comparison AS
WITH live_stats AS (
    SELECT 
        backtesting_strategy_used as strategy_name,
        bet_type,
        COUNT(*) as live_total_bets,
        SUM(CASE WHEN bet_won = true THEN 1 ELSE 0 END) as live_wins,
        SUM(CASE WHEN bet_won = false THEN 1 ELSE 0 END) as live_losses,
        AVG(CASE WHEN bet_won = true THEN 1.0 ELSE 0.0 END) as live_win_rate,
        SUM(profit_loss) as live_total_pnl,
        AVG(profit_loss) as live_avg_pnl,
        SUM(CASE WHEN profit_loss < 0 THEN ABS(profit_loss) ELSE 0 END) as live_total_losses
    FROM tracking.pre_game_recommendations 
    WHERE game_completed = true 
      AND bet_won IS NOT NULL
      AND backtesting_strategy_used IS NOT NULL
      AND game_datetime >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY backtesting_strategy_used, bet_type
),
backtest_stats AS (
    SELECT 
        strategy_name,
        split_type as bet_type,
        SUM(total_bets) as backtest_total_bets,
        SUM(wins) as backtest_wins,
        SUM(losses) as backtest_losses,
        AVG(win_rate) as backtest_win_rate,
        SUM(total_profit_loss) as backtest_total_pnl,
        AVG(total_profit_loss) as backtest_avg_pnl,
        SUM(total_gross_losses) as backtest_total_losses
    FROM backtesting.strategy_performance
    WHERE backtest_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY strategy_name, split_type
)
SELECT 
    COALESCE(l.strategy_name, b.strategy_name) as strategy_name,
    COALESCE(l.bet_type, b.bet_type) as bet_type,
    
    -- Live metrics
    COALESCE(l.live_total_bets, 0) as live_total_bets,
    COALESCE(l.live_win_rate, 0) as live_win_rate,
    COALESCE(l.live_total_pnl, 0) as live_total_pnl,
    COALESCE(l.live_total_losses, 0) as live_total_losses,
    
    -- Backtest metrics  
    COALESCE(b.backtest_total_bets, 0) as backtest_total_bets,
    COALESCE(b.backtest_win_rate, 0) as backtest_win_rate,
    COALESCE(b.backtest_total_pnl, 0) as backtest_total_pnl,
    COALESCE(b.backtest_total_losses, 0) as backtest_total_losses,
    
    -- Alignment metrics
    ABS(COALESCE(l.live_win_rate, 0) - COALESCE(b.backtest_win_rate, 0)) as win_rate_difference,
    ABS(COALESCE(l.live_total_pnl, 0) - COALESCE(b.backtest_total_pnl, 0)) as pnl_difference,
    ABS(COALESCE(l.live_total_losses, 0) - COALESCE(b.backtest_total_losses, 0)) as loss_difference,
    
    -- Alignment assessment
    CASE 
        WHEN ABS(COALESCE(l.live_win_rate, 0) - COALESCE(b.backtest_win_rate, 0)) < 0.05 
         AND ABS(COALESCE(l.live_total_losses, 0) - COALESCE(b.backtest_total_losses, 0)) < 100 
        THEN 'WELL_ALIGNED'
        WHEN ABS(COALESCE(l.live_win_rate, 0) - COALESCE(b.backtest_win_rate, 0)) < 0.10 
        THEN 'MODERATELY_ALIGNED'
        ELSE 'POORLY_ALIGNED'
    END as alignment_status
    
FROM live_stats l
FULL OUTER JOIN backtest_stats b ON l.strategy_name = b.strategy_name AND l.bet_type = b.bet_type
ORDER BY win_rate_difference DESC;

-- Notification performance tracking view
CREATE OR REPLACE VIEW tracking.notification_performance_summary AS
SELECT 
    notification_type,
    DATE(scheduled_time) as notification_date,
    
    -- Volume metrics
    COUNT(*) as total_scheduled,
    SUM(CASE WHEN notification_sent = true THEN 1 ELSE 0 END) as successfully_sent,
    SUM(CASE WHEN notification_sent = false THEN 1 ELSE 0 END) as failed_to_send,
    
    -- Timing accuracy
    AVG(timing_difference_seconds) as avg_timing_difference_seconds,
    STDDEV(timing_difference_seconds) as timing_variance_seconds,
    
    -- Content metrics
    AVG(recipient_count) as avg_recipients,
    AVG(recommendations_count) as avg_recommendations,
    
    -- Performance metrics
    AVG(processing_time_seconds) as avg_processing_time,
    
    -- Success rate
    ROUND(
        100.0 * SUM(CASE WHEN notification_sent = true THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) as success_rate_percent
    
FROM tracking.notification_log
WHERE scheduled_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY notification_type, DATE(scheduled_time)
ORDER BY notification_date DESC, notification_type;

-- ==============================================================================
-- ENHANCED FUNCTIONS FOR AUTOMATED PROCESSING
-- ==============================================================================

-- Function to update recommendation outcomes using unified evaluation
CREATE OR REPLACE FUNCTION tracking.update_recommendation_outcomes_unified()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    rec RECORD;
BEGIN
    -- Update recommendations that have completed games but no outcomes
    FOR rec IN (
        SELECT r.recommendation_id, r.game_pk, r.home_team, r.away_team,
               r.recommendation, r.bet_type, r.recommended_odds, r.recommended_bet_size
        FROM tracking.pre_game_recommendations r
        JOIN public.game_outcomes go ON CAST(r.game_pk AS VARCHAR) = go.game_id
        WHERE r.game_completed = FALSE
          AND r.game_datetime >= NOW() - INTERVAL '7 days'
    ) LOOP
        -- Update the recommendation with unified evaluation logic
        -- This would call the same logic used by the enhanced backtesting service
        
        UPDATE tracking.pre_game_recommendations
        SET 
            game_completed = TRUE,
            outcome_updated_at = NOW(),
            updated_at = NOW()
        WHERE recommendation_id = rec.recommendation_id;
        
        updated_count := updated_count + 1;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate alignment scores
CREATE OR REPLACE FUNCTION backtesting.calculate_alignment_scores()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    strategy_rec RECORD;
BEGIN
    -- Calculate alignment scores for strategies with recent live data
    FOR strategy_rec IN (
        SELECT DISTINCT strategy_name, source_book_type, split_type
        FROM backtesting.strategy_performance bp
        WHERE EXISTS (
            SELECT 1 FROM tracking.pre_game_recommendations r
            WHERE r.backtesting_strategy_used = bp.strategy_name
              AND r.game_completed = true
              AND r.game_datetime >= CURRENT_DATE - INTERVAL '30 days'
        )
    ) LOOP
        -- Update alignment metrics for this strategy
        -- This would calculate and update the alignment scores
        
        updated_count := updated_count + 1;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- DATA INTEGRITY AND CONSTRAINTS
-- ==============================================================================

-- Add trigger to automatically update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply update triggers
DROP TRIGGER IF EXISTS update_pre_game_recommendations_updated_at ON tracking.pre_game_recommendations;
CREATE TRIGGER update_pre_game_recommendations_updated_at
    BEFORE UPDATE ON tracking.pre_game_recommendations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_strategy_performance_updated_at ON backtesting.strategy_performance;
CREATE TRIGGER update_strategy_performance_updated_at
    BEFORE UPDATE ON backtesting.strategy_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_unified_bet_outcomes_updated_at ON backtesting.unified_bet_outcomes;
CREATE TRIGGER update_unified_bet_outcomes_updated_at
    BEFORE UPDATE ON backtesting.unified_bet_outcomes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add data validation constraints
ALTER TABLE tracking.pre_game_recommendations 
ADD CONSTRAINT valid_signal_strength CHECK (signal_strength >= 0 AND signal_strength <= 1),
ADD CONSTRAINT valid_profit_loss CHECK (
    (bet_won = true AND profit_loss >= 0) OR 
    (bet_won = false AND profit_loss <= 0) OR 
    bet_won IS NULL
);

-- ==============================================================================
-- MAINTENANCE AND CLEANUP
-- ==============================================================================

-- Enhanced cleanup function
CREATE OR REPLACE FUNCTION backtesting.cleanup_old_data()
RETURNS TEXT AS $$
DECLARE
    result_text TEXT := '';
    deleted_count INTEGER;
BEGIN
    -- Clean up old backtesting performance data (keep 6 months)
    DELETE FROM backtesting.strategy_performance 
    WHERE backtest_date < CURRENT_DATE - INTERVAL '6 months';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result_text := result_text || format('Deleted %s old strategy performance records. ', deleted_count);
    
    -- Clean up old alignment analysis (keep 3 months)
    DELETE FROM backtesting.alignment_analysis 
    WHERE alignment_date < NOW() - INTERVAL '3 months';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result_text := result_text || format('Deleted %s old alignment analysis records. ', deleted_count);
    
    -- Clean up old unified outcomes (keep 1 year)
    DELETE FROM backtesting.unified_bet_outcomes 
    WHERE game_datetime < NOW() - INTERVAL '1 year';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result_text := result_text || format('Deleted %s old unified outcome records. ', deleted_count);
    
    -- Clean up old notification logs (keep 90 days)
    DELETE FROM tracking.notification_log 
    WHERE scheduled_time < NOW() - INTERVAL '90 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result_text := result_text || format('Deleted %s old notification log records. ', deleted_count);
    
    -- Clean up old completed recommendations (keep 1 year)
    DELETE FROM tracking.pre_game_recommendations 
    WHERE game_datetime < NOW() - INTERVAL '1 year' AND game_completed = true;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result_text := result_text || format('Deleted %s old recommendation records.', deleted_count);
    
    RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA tracking TO PUBLIC;
GRANT USAGE ON SCHEMA backtesting TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA tracking TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA backtesting TO PUBLIC;

-- Grant write permissions to application role (adjust as needed)
-- GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tracking TO app_role;
-- GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA backtesting TO app_role;

COMMENT ON SCHEMA tracking IS 'Enhanced tracking schema for live recommendations and notifications with backtesting alignment';
COMMENT ON SCHEMA backtesting IS 'Enhanced backtesting schema with live recommendation alignment and unified loss tracking';

COMMENT ON TABLE tracking.pre_game_recommendations IS 'Enhanced pre-game recommendations with comprehensive tracking and backtesting alignment';
COMMENT ON TABLE backtesting.strategy_performance IS 'Enhanced strategy performance with live alignment metrics and detailed loss tracking';
COMMENT ON TABLE backtesting.alignment_analysis IS 'Analysis of alignment between backtesting results and live recommendations';
COMMENT ON TABLE backtesting.unified_bet_outcomes IS 'Unified bet outcomes across all system components for consistency analysis';
COMMENT ON TABLE tracking.notification_log IS 'Comprehensive tracking of email notifications with timing accuracy metrics'; 