-- ML Opportunity Detection System Database Schema
-- Migration: 035_create_ml_opportunity_tables.sql
-- Purpose: Create database schema for AI-powered opportunity detection system

-- Create ML analysis schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS analysis;

-- Betting strategies table
CREATE TABLE IF NOT EXISTS analysis.betting_strategies (
    strategy_id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL UNIQUE,
    strategy_type VARCHAR(50) NOT NULL, -- 'sharp_action', 'line_movement', etc.
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Strategy results tracking
CREATE TABLE IF NOT EXISTS analysis.strategy_results (
    result_id BIGSERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES analysis.betting_strategies(strategy_id),
    game_id VARCHAR(100) NOT NULL,
    bet_type VARCHAR(50) NOT NULL, -- 'spread', 'moneyline', 'total'
    recommended_side VARCHAR(20) NOT NULL, -- 'home', 'away', 'over', 'under'
    confidence_score DECIMAL(5,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 100),
    
    -- Betting details
    stake DECIMAL(10,2) DEFAULT 100.00, -- Standardized unit stake
    odds_taken DECIMAL(8,2), -- American odds format
    
    -- Results tracking
    outcome VARCHAR(10), -- 'WIN', 'LOSS', 'PUSH', 'VOID'
    profit DECIMAL(10,2), -- Actual profit/loss
    
    -- Metadata
    bet_placed_at TIMESTAMP WITH TIME ZONE,
    game_start_time TIMESTAMP WITH TIME ZONE,
    result_determined_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'PENDING', -- 'PENDING', 'COMPLETED', 'CANCELLED'
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ML opportunity scores table
CREATE TABLE IF NOT EXISTS analysis.ml_opportunity_scores (
    score_id BIGSERIAL PRIMARY KEY,
    opportunity_id VARCHAR(100) NOT NULL UNIQUE,
    game_id VARCHAR(100) NOT NULL,
    
    -- Scoring components
    composite_score DECIMAL(5,2) NOT NULL CHECK (composite_score >= 0 AND composite_score <= 100),
    tier VARCHAR(20) NOT NULL, -- 'PREMIUM', 'HIGH_VALUE', 'MODERATE', 'LOW'
    risk_profile VARCHAR(20) NOT NULL, -- 'CONSERVATIVE', 'MODERATE', 'AGGRESSIVE'
    
    -- Individual factor scores
    sharp_action_score DECIMAL(5,2),
    line_movement_score DECIMAL(5,2),
    consensus_divergence_score DECIMAL(5,2),
    historical_patterns_score DECIMAL(5,2),
    timing_factors_score DECIMAL(5,2),
    market_efficiency_score DECIMAL(5,2),
    confidence_level_score DECIMAL(5,2),
    
    -- Performance tracking
    discovery_time_ms DECIMAL(8,2),
    cache_hit BOOLEAN DEFAULT false,
    
    -- Metadata
    user_id VARCHAR(100), -- Optional user context
    ml_model_version VARCHAR(20),
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ML detected patterns table
CREATE TABLE IF NOT EXISTS analysis.ml_detected_patterns (
    pattern_id BIGSERIAL PRIMARY KEY,
    opportunity_id VARCHAR(100) REFERENCES analysis.ml_opportunity_scores(opportunity_id),
    game_id VARCHAR(100) NOT NULL,
    
    -- Pattern details
    pattern_type VARCHAR(30) NOT NULL, -- 'ANOMALY', 'TEMPORAL_CLUSTER', etc.
    confidence VARCHAR(10) NOT NULL, -- 'HIGH', 'MEDIUM', 'LOW'
    strength DECIMAL(4,3) NOT NULL CHECK (strength >= 0 AND strength <= 1),
    
    -- Pattern data
    description TEXT,
    supporting_signals JSONB, -- Array of signal IDs that support this pattern
    pattern_metadata JSONB, -- Additional pattern-specific data
    
    -- Statistical metrics
    statistical_significance DECIMAL(4,3),
    anomaly_score DECIMAL(8,4), -- For anomaly patterns
    cluster_size INTEGER, -- For temporal cluster patterns
    correlation_coefficient DECIMAL(4,3), -- For correlation patterns
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ML explanations table
CREATE TABLE IF NOT EXISTS analysis.ml_explanations (
    explanation_id BIGSERIAL PRIMARY KEY,
    opportunity_id VARCHAR(100) REFERENCES analysis.ml_opportunity_scores(opportunity_id),
    user_id VARCHAR(100),
    
    -- User context
    experience_level VARCHAR(20) NOT NULL, -- 'BEGINNER', 'INTERMEDIATE', 'EXPERT'
    preferred_language VARCHAR(5) DEFAULT 'en',
    
    -- Explanation content
    summary TEXT NOT NULL,
    detailed_explanation JSONB, -- Structured explanation data
    recommendations JSONB, -- Action recommendations
    
    -- Explanation metadata
    explanation_format VARCHAR(20) DEFAULT 'STRUCTURED', -- 'STRUCTURED', 'NARRATIVE'
    generation_time_ms DECIMAL(8,2),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ML performance metrics table
CREATE TABLE IF NOT EXISTS analysis.ml_performance_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    
    -- Opportunity detection metrics
    opportunities_discovered INTEGER DEFAULT 0,
    opportunities_cached INTEGER DEFAULT 0,
    patterns_detected INTEGER DEFAULT 0,
    explanations_generated INTEGER DEFAULT 0,
    
    -- Performance metrics
    avg_discovery_time_ms DECIMAL(8,2),
    cache_hit_rate DECIMAL(4,3),
    pattern_detection_accuracy DECIMAL(4,3),
    explanation_satisfaction_score DECIMAL(4,3),
    
    -- System metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_mb INTEGER,
    active_user_sessions INTEGER,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure one record per day
    UNIQUE(metric_date)
);

-- ML model performance tracking
CREATE TABLE IF NOT EXISTS analysis.ml_model_performance (
    model_performance_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(50) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    
    -- Performance metrics
    accuracy DECIMAL(5,4),
    precision_score DECIMAL(5,4),
    recall DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    
    -- Betting-specific metrics
    win_rate DECIMAL(5,4),
    roi DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    sharpe_ratio DECIMAL(8,4),
    
    -- Evaluation period
    evaluation_start_date DATE NOT NULL,
    evaluation_end_date DATE NOT NULL,
    total_predictions INTEGER NOT NULL,
    
    -- Metadata
    training_data_size INTEGER,
    feature_count INTEGER,
    hyperparameters JSONB,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Unique constraint for model version and evaluation period
    UNIQUE(model_name, model_version, evaluation_start_date, evaluation_end_date)
);

-- Create indexes for performance

-- Strategy results indexes
CREATE INDEX IF NOT EXISTS idx_strategy_results_strategy_id ON analysis.strategy_results(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_results_game_id ON analysis.strategy_results(game_id);
CREATE INDEX IF NOT EXISTS idx_strategy_results_created_at ON analysis.strategy_results(created_at);
CREATE INDEX IF NOT EXISTS idx_strategy_results_status ON analysis.strategy_results(status);
CREATE INDEX IF NOT EXISTS idx_strategy_results_outcome ON analysis.strategy_results(outcome);
CREATE INDEX IF NOT EXISTS idx_strategy_results_performance ON analysis.strategy_results(strategy_id, outcome, created_at) WHERE status = 'COMPLETED';

-- ML opportunity scores indexes
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_game_id ON analysis.ml_opportunity_scores(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_tier ON analysis.ml_opportunity_scores(tier);
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_score ON analysis.ml_opportunity_scores(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_generated_at ON analysis.ml_opportunity_scores(generated_at);
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_expires_at ON analysis.ml_opportunity_scores(expires_at);
CREATE INDEX IF NOT EXISTS idx_ml_opportunity_scores_user_id ON analysis.ml_opportunity_scores(user_id) WHERE user_id IS NOT NULL;

-- ML detected patterns indexes
CREATE INDEX IF NOT EXISTS idx_ml_detected_patterns_opportunity_id ON analysis.ml_detected_patterns(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_ml_detected_patterns_game_id ON analysis.ml_detected_patterns(game_id);
CREATE INDEX IF NOT EXISTS idx_ml_detected_patterns_type ON analysis.ml_detected_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_ml_detected_patterns_confidence ON analysis.ml_detected_patterns(confidence);

-- ML explanations indexes
CREATE INDEX IF NOT EXISTS idx_ml_explanations_opportunity_id ON analysis.ml_explanations(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_ml_explanations_user_id ON analysis.ml_explanations(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ml_explanations_experience_level ON analysis.ml_explanations(experience_level);

-- Performance metrics indexes
CREATE INDEX IF NOT EXISTS idx_ml_performance_metrics_date ON analysis.ml_performance_metrics(metric_date);

-- Model performance indexes
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_model ON analysis.ml_model_performance(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_ml_model_performance_dates ON analysis.ml_model_performance(evaluation_start_date, evaluation_end_date);

-- Insert default betting strategies
INSERT INTO analysis.betting_strategies (strategy_name, strategy_type, description) VALUES
('sharp_action', 'sharp_money', 'Track professional bettor (sharp money) movements and line impacts'),
('line_movement', 'line_analysis', 'Analyze betting line movements and their predictive value'),
('book_conflict', 'arbitrage', 'Identify disagreements between sportsbooks on line pricing'),
('hybrid_sharp', 'combined', 'Combined analysis using multiple sharp money indicators'),
('consensus', 'public', 'Track consensus betting patterns and public money flow'),
('timing_based', 'temporal', 'Time-sensitive betting signals and late money detection'),
('rlm_strategy', 'reverse_movement', 'Reverse line movement detection and analysis'),
('steam_move', 'momentum', 'Rapid line movement detection indicating coordinated betting'),
('late_flip', 'late_action', 'Last-minute line movements before game start'),
('contrarian', 'fade_public', 'Contrarian plays that fade public betting sentiment')
ON CONFLICT (strategy_name) DO NOTHING;

-- Create views for common queries

-- Strategy performance summary view
CREATE OR REPLACE VIEW analysis.strategy_performance_summary AS
SELECT 
    bs.strategy_name,
    bs.strategy_type,
    COUNT(*) as total_bets,
    COUNT(CASE WHEN sr.outcome = 'WIN' THEN 1 END) as wins,
    COUNT(CASE WHEN sr.outcome = 'LOSS' THEN 1 END) as losses,
    COUNT(CASE WHEN sr.outcome = 'PUSH' THEN 1 END) as pushes,
    ROUND(AVG(CASE WHEN sr.outcome = 'WIN' THEN 1.0 ELSE 0.0 END) * 100, 2) as win_rate,
    ROUND(SUM(COALESCE(sr.profit, 0)), 2) as total_profit,
    ROUND(AVG(COALESCE(sr.profit, 0)), 2) as avg_profit_per_bet,
    ROUND(AVG(sr.confidence_score), 2) as avg_confidence,
    MIN(sr.created_at) as first_bet_date,
    MAX(sr.created_at) as last_bet_date
FROM analysis.betting_strategies bs
LEFT JOIN analysis.strategy_results sr ON bs.strategy_id = sr.strategy_id 
    AND sr.status = 'COMPLETED' 
    AND sr.created_at >= NOW() - INTERVAL '6 months'
WHERE bs.is_active = true
GROUP BY bs.strategy_id, bs.strategy_name, bs.strategy_type
ORDER BY total_profit DESC NULLS LAST;

-- Recent opportunities view
CREATE OR REPLACE VIEW analysis.recent_opportunities AS
SELECT 
    mos.opportunity_id,
    mos.game_id,
    mos.composite_score,
    mos.tier,
    mos.risk_profile,
    mos.discovery_time_ms,
    mos.generated_at,
    COUNT(mdp.pattern_id) as pattern_count,
    ARRAY_AGG(mdp.pattern_type) FILTER (WHERE mdp.pattern_type IS NOT NULL) as pattern_types
FROM analysis.ml_opportunity_scores mos
LEFT JOIN analysis.ml_detected_patterns mdp ON mos.opportunity_id = mdp.opportunity_id
WHERE mos.generated_at >= NOW() - INTERVAL '24 hours'
GROUP BY mos.opportunity_id, mos.game_id, mos.composite_score, mos.tier, 
         mos.risk_profile, mos.discovery_time_ms, mos.generated_at
ORDER BY mos.composite_score DESC;

-- Add comments for documentation
COMMENT ON SCHEMA analysis IS 'ML and strategy analysis schema for opportunity detection';
COMMENT ON TABLE analysis.betting_strategies IS 'Catalog of betting strategies with metadata';
COMMENT ON TABLE analysis.strategy_results IS 'Historical performance tracking for betting strategies';
COMMENT ON TABLE analysis.ml_opportunity_scores IS 'AI-generated opportunity scores and analysis';
COMMENT ON TABLE analysis.ml_detected_patterns IS 'ML-detected patterns in betting data';
COMMENT ON TABLE analysis.ml_explanations IS 'Natural language explanations for opportunities';
COMMENT ON TABLE analysis.ml_performance_metrics IS 'System performance metrics tracking';
COMMENT ON TABLE analysis.ml_model_performance IS 'ML model performance evaluation results';

-- Migration completed
SELECT 'ML Opportunity Detection tables created successfully' as migration_status;