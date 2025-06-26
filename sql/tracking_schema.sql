-- Tracking Schema for Betting Performance Reports
-- This schema tracks pre-game betting recommendations and their outcomes

CREATE SCHEMA IF NOT EXISTS tracking;

-- Pre-Game Recommendations Table
CREATE TABLE IF NOT EXISTS tracking.pre_game_recommendations (
    recommendation_id VARCHAR PRIMARY KEY,
    game_pk VARCHAR NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    recommendation TEXT NOT NULL,
    bet_type VARCHAR NOT NULL CHECK (bet_type IN ('moneyline', 'spread', 'total')),
    confidence_level VARCHAR NOT NULL CHECK (confidence_level IN ('LOW', 'MODERATE', 'HIGH')),
    signal_source VARCHAR NOT NULL,
    signal_strength DOUBLE PRECISION DEFAULT 0.0,
    recommended_at TIMESTAMP WITH TIME ZONE NOT NULL,
    email_sent BOOLEAN DEFAULT TRUE,
    
    -- Outcome tracking (filled in after game completion)
    game_completed BOOLEAN DEFAULT FALSE,
    bet_won BOOLEAN DEFAULT NULL,
    actual_outcome TEXT DEFAULT NULL,
    profit_loss DOUBLE PRECISION DEFAULT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Strategy source tracking
    strategy_source_id VARCHAR(200),
    auto_integrated BOOLEAN DEFAULT false
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_game_date 
ON tracking.pre_game_recommendations(DATE(game_datetime));

CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_game_pk 
ON tracking.pre_game_recommendations(game_pk);

CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_bet_type 
ON tracking.pre_game_recommendations(bet_type);

CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_confidence 
ON tracking.pre_game_recommendations(confidence_level);

CREATE INDEX IF NOT EXISTS idx_pre_game_recommendations_completed 
ON tracking.pre_game_recommendations(game_completed, bet_won);

CREATE INDEX IF NOT EXISTS idx_pregame_strategy_source 
ON tracking.pre_game_recommendations(strategy_source_id, auto_integrated);

-- Game Outcomes Table (if not exists in main schema)
CREATE TABLE IF NOT EXISTS game_outcomes (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR NOT NULL UNIQUE,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    
    -- Betting outcomes
    over BOOLEAN NOT NULL,
    home_win BOOLEAN NOT NULL,
    home_cover_spread BOOLEAN DEFAULT NULL,
    
    -- Additional context
    total_line DOUBLE PRECISION DEFAULT NULL,
    home_spread_line DOUBLE PRECISION DEFAULT NULL,
    game_date TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for game outcomes
CREATE INDEX IF NOT EXISTS idx_game_outcomes_game_date 
ON game_outcomes(DATE(game_date));

CREATE INDEX IF NOT EXISTS idx_game_outcomes_teams 
ON game_outcomes(home_team, away_team);

-- Performance Summary View
CREATE OR REPLACE VIEW tracking.betting_performance_summary AS
SELECT 
    DATE(game_datetime) as game_date,
    COUNT(*) as total_recommendations,
    COUNT(CASE WHEN game_completed THEN 1 END) as completed_games,
    COUNT(CASE WHEN bet_won = true THEN 1 END) as wins,
    COUNT(CASE WHEN bet_won = false THEN 1 END) as losses,
    COUNT(CASE WHEN bet_won IS NULL AND game_completed THEN 1 END) as pushes,
    
    -- Win rates by bet type
    COUNT(CASE WHEN bet_type = 'moneyline' AND bet_won = true THEN 1 END) as ml_wins,
    COUNT(CASE WHEN bet_type = 'moneyline' AND bet_won = false THEN 1 END) as ml_losses,
    COUNT(CASE WHEN bet_type = 'spread' AND bet_won = true THEN 1 END) as spread_wins,
    COUNT(CASE WHEN bet_type = 'spread' AND bet_won = false THEN 1 END) as spread_losses,
    COUNT(CASE WHEN bet_type = 'total' AND bet_won = true THEN 1 END) as total_wins,
    COUNT(CASE WHEN bet_type = 'total' AND bet_won = false THEN 1 END) as total_losses,
    
    -- Performance by confidence
    COUNT(CASE WHEN confidence_level = 'HIGH' AND bet_won = true THEN 1 END) as high_conf_wins,
    COUNT(CASE WHEN confidence_level = 'HIGH' AND bet_won = false THEN 1 END) as high_conf_losses,
    COUNT(CASE WHEN confidence_level = 'MODERATE' AND bet_won = true THEN 1 END) as mod_conf_wins,
    COUNT(CASE WHEN confidence_level = 'MODERATE' AND bet_won = false THEN 1 END) as mod_conf_losses,
    COUNT(CASE WHEN confidence_level = 'LOW' AND bet_won = true THEN 1 END) as low_conf_wins,
    COUNT(CASE WHEN confidence_level = 'LOW' AND bet_won = false THEN 1 END) as low_conf_losses,
    
    -- Financial metrics
    SUM(COALESCE(profit_loss, 0)) as total_profit_loss,
    AVG(COALESCE(profit_loss, 0)) as avg_profit_loss,
    
    -- Win percentages
    CASE 
        WHEN COUNT(CASE WHEN game_completed THEN 1 END) > 0 
        THEN COUNT(CASE WHEN bet_won = true THEN 1 END)::DECIMAL / COUNT(CASE WHEN game_completed THEN 1 END) * 100
        ELSE 0 
    END as win_percentage
    
FROM tracking.pre_game_recommendations
WHERE game_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(game_datetime)
ORDER BY game_date DESC;

-- Function to update recommendation outcomes
CREATE OR REPLACE FUNCTION tracking.update_recommendation_outcomes()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    rec RECORD;
    outcome RECORD;
    bet_result BOOLEAN;
    profit_amount DOUBLE PRECISION;
BEGIN
    -- Loop through incomplete recommendations
    FOR rec IN 
        SELECT r.recommendation_id, r.game_pk, r.home_team, r.away_team, 
               r.recommendation, r.bet_type, r.game_datetime
        FROM tracking.pre_game_recommendations r
        WHERE r.game_completed = FALSE
          AND r.game_datetime < NOW() - INTERVAL '3 hours'  -- Game should be finished
    LOOP
        -- Try to find game outcome
        SELECT g.home_score, g.away_score, g.home_win, g.over, g.home_cover_spread
        INTO outcome
        FROM game_outcomes g
        WHERE g.game_id = rec.game_pk
           OR (g.home_team = rec.home_team 
               AND g.away_team = rec.away_team 
               AND DATE(g.game_date) = DATE(rec.game_datetime));
        
        IF FOUND THEN
            -- Determine bet result based on recommendation and outcome
            bet_result := NULL;
            profit_amount := 0;
            
            -- Parse recommendation and determine if it won
            IF rec.bet_type = 'moneyline' THEN
                IF rec.recommendation LIKE '%' || rec.home_team || '%' THEN
                    bet_result := outcome.home_win;
                ELSIF rec.recommendation LIKE '%' || rec.away_team || '%' THEN
                    bet_result := NOT outcome.home_win;
                END IF;
            ELSIF rec.bet_type = 'total' THEN
                IF rec.recommendation LIKE '%OVER%' THEN
                    bet_result := outcome.over;
                ELSIF rec.recommendation LIKE '%UNDER%' THEN
                    bet_result := NOT outcome.over;
                END IF;
            ELSIF rec.bet_type = 'spread' THEN
                IF outcome.home_cover_spread IS NOT NULL THEN
                    IF rec.recommendation LIKE '%' || rec.home_team || '%' THEN
                        bet_result := outcome.home_cover_spread;
                    ELSIF rec.recommendation LIKE '%' || rec.away_team || '%' THEN
                        bet_result := NOT outcome.home_cover_spread;
                    END IF;
                END IF;
            END IF;
            
            -- Calculate profit/loss (assuming $100 bet at -110 odds)
            IF bet_result = TRUE THEN
                profit_amount := 90.91;  -- Win $90.91 on $100 bet at -110
            ELSIF bet_result = FALSE THEN
                profit_amount := -100.0; -- Lose $100
            END IF;
            
            -- Update the recommendation
            UPDATE tracking.pre_game_recommendations
            SET 
                game_completed = TRUE,
                bet_won = bet_result,
                actual_outcome = format('%s won %s-%s', 
                    CASE WHEN outcome.home_win THEN rec.home_team ELSE rec.away_team END,
                    outcome.home_score, outcome.away_score),
                profit_loss = profit_amount,
                updated_at = NOW()
            WHERE recommendation_id = rec.recommendation_id;
            
            updated_count := updated_count + 1;
        END IF;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON SCHEMA tracking IS 'Schema for tracking betting recommendations and performance analytics';
COMMENT ON TABLE tracking.pre_game_recommendations IS 'Stores betting recommendations sent via pre-game emails';
COMMENT ON TABLE game_outcomes IS 'Stores final game results for performance calculation';
COMMENT ON VIEW tracking.betting_performance_summary IS 'Daily summary of betting recommendation performance';
COMMENT ON FUNCTION tracking.update_recommendation_outcomes() IS 'Function to update recommendation outcomes after games complete';

-- ============================================================================
-- High-ROI Strategy Auto-Integration Tables
-- ============================================================================

-- Cache for strategy performance results (populated by backtesting service)
CREATE TABLE IF NOT EXISTS tracking.strategy_performance_cache (
    id SERIAL PRIMARY KEY,
    source_book_type VARCHAR(100) NOT NULL,
    split_type VARCHAR(50) NOT NULL,
    strategy_variant VARCHAR(100) NOT NULL,
    total_bets INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    win_rate DECIMAL(5,2) NOT NULL,
    roi_per_100_unit DECIMAL(8,2) NOT NULL,
    confidence_level VARCHAR(20),
    avg_odds DECIMAL(8,2),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(source_book_type, split_type, strategy_variant, last_updated)
);

-- Active high-ROI strategies that are integrated into live system
CREATE TABLE IF NOT EXISTS tracking.active_high_roi_strategies (
    strategy_id VARCHAR(200) PRIMARY KEY,
    source_book_type VARCHAR(100) NOT NULL,
    split_type VARCHAR(50) NOT NULL,
    strategy_variant VARCHAR(100) NOT NULL,
    total_bets INTEGER NOT NULL,
    win_rate DECIMAL(5,2) NOT NULL,
    roi_per_100_unit DECIMAL(8,2) NOT NULL,
    confidence_level VARCHAR(20) NOT NULL,
    min_threshold DECIMAL(6,2) NOT NULL,
    high_threshold DECIMAL(6,2) NOT NULL,
    avg_odds DECIMAL(8,2),
    last_backtesting_update TIMESTAMP WITH TIME ZONE,
    integration_status VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, ACTIVE, PAUSED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Configuration for active strategies
CREATE TABLE IF NOT EXISTS tracking.active_strategy_configs (
    strategy_id VARCHAR(200) PRIMARY KEY,
    configuration JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Log of strategy integration events
CREATE TABLE IF NOT EXISTS tracking.strategy_integration_log (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(200) NOT NULL,
    action VARCHAR(50) NOT NULL,  -- INTEGRATED, UPDATED, PAUSED, REACTIVATED
    roi_per_100_unit DECIMAL(8,2),
    total_bets INTEGER,
    min_threshold DECIMAL(6,2),
    high_threshold DECIMAL(6,2),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_strategy_cache_performance 
ON tracking.strategy_performance_cache(roi_per_100_unit DESC, total_bets DESC, last_updated DESC);

CREATE INDEX IF NOT EXISTS idx_active_strategies_roi 
ON tracking.active_high_roi_strategies(roi_per_100_unit DESC, integration_status);

CREATE INDEX IF NOT EXISTS idx_integration_log_strategy 
ON tracking.strategy_integration_log(strategy_id, created_at DESC); 