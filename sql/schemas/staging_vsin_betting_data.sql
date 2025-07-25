-- =============================================================================
-- STAGING VSIN Betting Data Schema  
-- =============================================================================
-- Purpose: Store VSIN betting splits data with MLB Stats API integration
-- Data Source: VSIN Vegas Stats & Information Network
--
-- Key Features:
-- - Unified betting splits with handle and bet count percentages
-- - Cross-system integration with MLB Stats API game IDs
-- - Sharp action detection and consensus tracking
-- =============================================================================

DROP TABLE IF EXISTS staging.vsin_betting_data CASCADE;

CREATE TABLE staging.vsin_betting_data (
    id BIGSERIAL PRIMARY KEY,
    
    -- =============================================================================
    -- GAME AND SOURCE IDENTIFICATION
    -- =============================================================================
    external_matchup_id VARCHAR(255),      -- VSIN's internal game identifier
    mlb_stats_api_game_id VARCHAR(50),     -- Cross-system integration
    
    -- Game Details
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_team_normalized VARCHAR(100),
    away_team_normalized VARCHAR(100),
    
    -- Game Timing
    game_date DATE,
    game_time TIME,
    
    -- =============================================================================
    -- SPORTSBOOK INFORMATION
    -- =============================================================================
    sportsbook_name VARCHAR(100) NOT NULL, -- dk, circa, fanduel, mgm, caesars
    sportsbook_id INTEGER,                 -- Internal sportsbook mapping
    
    -- =============================================================================
    -- MONEYLINE BETTING DATA
    -- =============================================================================
    moneyline_home_odds INTEGER,
    moneyline_away_odds INTEGER,
    moneyline_home_handle_percent DECIMAL(5,2),    -- % of money on home team
    moneyline_away_handle_percent DECIMAL(5,2),    -- % of money on away team
    moneyline_home_bets_percent DECIMAL(5,2),      -- % of bets on home team
    moneyline_away_bets_percent DECIMAL(5,2),      -- % of bets on away team
    
    -- =============================================================================
    -- TOTALS (OVER/UNDER) BETTING DATA
    -- =============================================================================
    total_line DECIMAL(4,1),                       -- Over/under line (e.g., 8.5)
    total_over_odds INTEGER,
    total_under_odds INTEGER,
    total_over_handle_percent DECIMAL(5,2),        -- % of money on over
    total_under_handle_percent DECIMAL(5,2),       -- % of money on under
    total_over_bets_percent DECIMAL(5,2),          -- % of bets on over
    total_under_bets_percent DECIMAL(5,2),         -- % of bets on under
    
    -- =============================================================================
    -- RUNLINE (SPREAD) BETTING DATA
    -- =============================================================================
    runline_spread DECIMAL(4,1),                   -- Runline spread (typically +/-1.5)
    runline_home_odds INTEGER,
    runline_away_odds INTEGER,
    runline_home_handle_percent DECIMAL(5,2),      -- % of money on home runline
    runline_away_handle_percent DECIMAL(5,2),      -- % of money on away runline
    runline_home_bets_percent DECIMAL(5,2),        -- % of bets on home runline
    runline_away_bets_percent DECIMAL(5,2),        -- % of bets on away runline
    
    -- =============================================================================
    -- SHARP ACTION INDICATORS
    -- =============================================================================
    moneyline_sharp_side VARCHAR(10),              -- home, away, or null
    total_sharp_side VARCHAR(10),                  -- over, under, or null
    runline_sharp_side VARCHAR(10),                -- home, away, or null
    
    -- Sharp action confidence (0.0 to 1.0)
    sharp_confidence DECIMAL(3,2) DEFAULT 0.0,
    
    -- Reverse Line Movement indicators
    moneyline_rlm_detected BOOLEAN DEFAULT FALSE,
    total_rlm_detected BOOLEAN DEFAULT FALSE,
    runline_rlm_detected BOOLEAN DEFAULT FALSE,
    
    -- =============================================================================
    -- DATA QUALITY AND LINEAGE
    -- =============================================================================
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(50) DEFAULT 'valid',
    parsing_errors TEXT[],                         -- Array of parsing error messages
    
    -- Source tracking
    source_url TEXT,                               -- VSIN URL used for collection
    vsin_view VARCHAR(50),                         -- dk, circa, fanduel, etc.
    
    -- Lineage tracking
    raw_data_id BIGINT,                           -- References raw_data.vsin_raw_data(id)
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- =============================================================================
    -- CONSTRAINTS
    -- =============================================================================
    -- Unique constraint to prevent duplicate records
    UNIQUE(external_matchup_id, sportsbook_name, game_date, processed_at),
    
    -- Validate percentage values (0-100)
    CONSTRAINT valid_handle_percentages CHECK (
        (moneyline_home_handle_percent IS NULL OR (moneyline_home_handle_percent >= 0 AND moneyline_home_handle_percent <= 100)) AND
        (moneyline_away_handle_percent IS NULL OR (moneyline_away_handle_percent >= 0 AND moneyline_away_handle_percent <= 100)) AND
        (total_over_handle_percent IS NULL OR (total_over_handle_percent >= 0 AND total_over_handle_percent <= 100)) AND
        (total_under_handle_percent IS NULL OR (total_under_handle_percent >= 0 AND total_under_handle_percent <= 100)) AND
        (runline_home_handle_percent IS NULL OR (runline_home_handle_percent >= 0 AND runline_home_handle_percent <= 100)) AND
        (runline_away_handle_percent IS NULL OR (runline_away_handle_percent >= 0 AND runline_away_handle_percent <= 100))
    ),
    
    CONSTRAINT valid_bet_percentages CHECK (
        (moneyline_home_bets_percent IS NULL OR (moneyline_home_bets_percent >= 0 AND moneyline_home_bets_percent <= 100)) AND
        (moneyline_away_bets_percent IS NULL OR (moneyline_away_bets_percent >= 0 AND moneyline_away_bets_percent <= 100)) AND
        (total_over_bets_percent IS NULL OR (total_over_bets_percent >= 0 AND total_over_bets_percent <= 100)) AND
        (total_under_bets_percent IS NULL OR (total_under_bets_percent >= 0 AND total_under_bets_percent <= 100)) AND
        (runline_home_bets_percent IS NULL OR (runline_home_bets_percent >= 0 AND runline_home_bets_percent <= 100)) AND
        (runline_away_bets_percent IS NULL OR (runline_away_bets_percent >= 0 AND runline_away_bets_percent <= 100))
    ),
    
    -- Validate sharp sides
    CONSTRAINT valid_sharp_sides CHECK (
        (moneyline_sharp_side IS NULL OR moneyline_sharp_side IN ('home', 'away')) AND
        (total_sharp_side IS NULL OR total_sharp_side IN ('over', 'under')) AND
        (runline_sharp_side IS NULL OR runline_sharp_side IN ('home', 'away'))
    )
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Primary lookup indexes
CREATE INDEX idx_vsin_betting_external_id ON staging.vsin_betting_data(external_matchup_id);
CREATE INDEX idx_vsin_betting_mlb_id ON staging.vsin_betting_data(mlb_stats_api_game_id);
CREATE INDEX idx_vsin_betting_teams ON staging.vsin_betting_data(home_team_normalized, away_team_normalized);
CREATE INDEX idx_vsin_betting_game_date ON staging.vsin_betting_data(game_date);

-- Sportsbook analysis indexes
CREATE INDEX idx_vsin_betting_sportsbook ON staging.vsin_betting_data(sportsbook_name, sportsbook_id);
CREATE INDEX idx_vsin_betting_sbook_date ON staging.vsin_betting_data(sportsbook_name, game_date);

-- Sharp action analysis indexes
CREATE INDEX idx_vsin_betting_sharp_action ON staging.vsin_betting_data(moneyline_sharp_side, total_sharp_side, runline_sharp_side);
CREATE INDEX idx_vsin_betting_sharp_confidence ON staging.vsin_betting_data(sharp_confidence) WHERE sharp_confidence > 0.5;
CREATE INDEX idx_vsin_betting_rlm ON staging.vsin_betting_data(moneyline_rlm_detected, total_rlm_detected, runline_rlm_detected);

-- Data quality indexes
CREATE INDEX idx_vsin_betting_quality ON staging.vsin_betting_data(data_quality_score, validation_status);
CREATE INDEX idx_vsin_betting_processed_at ON staging.vsin_betting_data(processed_at);

-- Composite indexes for common queries
CREATE INDEX idx_vsin_betting_game_sbook ON staging.vsin_betting_data(external_matchup_id, sportsbook_name);
CREATE INDEX idx_vsin_betting_mlb_sbook ON staging.vsin_betting_data(mlb_stats_api_game_id, sportsbook_name);

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

-- View for sharp action analysis
CREATE OR REPLACE VIEW staging.v_vsin_sharp_action AS
SELECT 
    external_matchup_id,
    mlb_stats_api_game_id,
    home_team_normalized,
    away_team_normalized,
    game_date,
    sportsbook_name,
    
    -- Sharp action summary
    CASE 
        WHEN moneyline_sharp_side IS NOT NULL OR total_sharp_side IS NOT NULL OR runline_sharp_side IS NOT NULL 
        THEN TRUE ELSE FALSE 
    END as has_sharp_action,
    
    moneyline_sharp_side,
    total_sharp_side, 
    runline_sharp_side,
    sharp_confidence,
    
    -- RLM indicators
    (moneyline_rlm_detected OR total_rlm_detected OR runline_rlm_detected) as has_rlm,
    moneyline_rlm_detected,
    total_rlm_detected,
    runline_rlm_detected,
    
    processed_at
FROM staging.vsin_betting_data
WHERE data_quality_score >= 0.8 AND validation_status = 'valid';

-- View for consensus analysis
CREATE OR REPLACE VIEW staging.v_vsin_consensus AS
SELECT 
    external_matchup_id,
    mlb_stats_api_game_id,
    home_team_normalized,
    away_team_normalized,
    game_date,
    sportsbook_name,
    
    -- Moneyline consensus
    moneyline_home_handle_percent,
    moneyline_home_bets_percent,
    (moneyline_home_handle_percent - moneyline_home_bets_percent) as moneyline_home_sharp_indicator,
    
    -- Totals consensus
    total_over_handle_percent,
    total_over_bets_percent,
    (total_over_handle_percent - total_over_bets_percent) as total_over_sharp_indicator,
    
    -- Runline consensus
    runline_home_handle_percent,
    runline_home_bets_percent,
    (runline_home_handle_percent - runline_home_bets_percent) as runline_home_sharp_indicator,
    
    processed_at
FROM staging.vsin_betting_data
WHERE data_quality_score >= 0.8 AND validation_status = 'valid';

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Function to detect sharp action based on handle vs bets discrepancy
CREATE OR REPLACE FUNCTION staging.detect_vsin_sharp_action(
    p_handle_percent DECIMAL(5,2),
    p_bets_percent DECIMAL(5,2),
    p_threshold DECIMAL(5,2) DEFAULT 10.0
)
RETURNS BOOLEAN AS $$
BEGIN
    -- Sharp action indicated when handle % significantly exceeds bet %
    -- (professional money moving lines with fewer but larger bets)
    IF p_handle_percent IS NULL OR p_bets_percent IS NULL THEN
        RETURN FALSE;
    END IF;
    
    RETURN ABS(p_handle_percent - p_bets_percent) >= p_threshold;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate sharp confidence score
CREATE OR REPLACE FUNCTION staging.calculate_vsin_sharp_confidence(
    p_moneyline_handle DECIMAL(5,2),
    p_moneyline_bets DECIMAL(5,2),
    p_total_handle DECIMAL(5,2),
    p_total_bets DECIMAL(5,2),
    p_runline_handle DECIMAL(5,2),
    p_runline_bets DECIMAL(5,2)
)
RETURNS DECIMAL(3,2) AS $$
DECLARE
    sharp_indicators INTEGER := 0;
    total_markets INTEGER := 0;
BEGIN
    -- Count markets with sharp action indicators
    IF p_moneyline_handle IS NOT NULL AND p_moneyline_bets IS NOT NULL THEN
        total_markets := total_markets + 1;
        IF staging.detect_vsin_sharp_action(p_moneyline_handle, p_moneyline_bets) THEN
            sharp_indicators := sharp_indicators + 1;
        END IF;
    END IF;
    
    IF p_total_handle IS NOT NULL AND p_total_bets IS NOT NULL THEN
        total_markets := total_markets + 1;
        IF staging.detect_vsin_sharp_action(p_total_handle, p_total_bets) THEN
            sharp_indicators := sharp_indicators + 1;
        END IF;
    END IF;
    
    IF p_runline_handle IS NOT NULL AND p_runline_bets IS NOT NULL THEN
        total_markets := total_markets + 1;
        IF staging.detect_vsin_sharp_action(p_runline_handle, p_runline_bets) THEN
            sharp_indicators := sharp_indicators + 1;
        END IF;
    END IF;
    
    -- Return confidence as ratio of sharp markets to total markets
    IF total_markets = 0 THEN
        RETURN 0.0;
    END IF;
    
    RETURN ROUND((sharp_indicators::DECIMAL / total_markets::DECIMAL), 2);
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- DOCUMENTATION AND COMMENTS
-- =============================================================================

COMMENT ON TABLE staging.vsin_betting_data IS 
'VSIN betting splits data with consensus tracking and sharp action detection. Includes handle/bet percentages for moneyline, totals, and runline markets.';

COMMENT ON COLUMN staging.vsin_betting_data.moneyline_home_handle_percent IS 
'Percentage of total money wagered on home team moneyline';

COMMENT ON COLUMN staging.vsin_betting_data.moneyline_home_bets_percent IS 
'Percentage of total bets placed on home team moneyline';

COMMENT ON COLUMN staging.vsin_betting_data.sharp_confidence IS 
'Confidence score (0.0-1.0) for sharp action detection based on handle vs bets discrepancy';

COMMENT ON VIEW staging.v_vsin_sharp_action IS 
'Consolidated view of sharp action indicators across all VSIN betting markets';

COMMENT ON VIEW staging.v_vsin_consensus IS 
'Public vs sharp money analysis showing handle/bets discrepancies indicating professional action';

COMMENT ON FUNCTION staging.detect_vsin_sharp_action IS 
'Detects sharp action when handle percentage significantly exceeds bet percentage';

COMMENT ON FUNCTION staging.calculate_vsin_sharp_confidence IS 
'Calculates overall sharp confidence score based on multiple market indicators';

-- =============================================================================
-- EXAMPLE QUERIES FOR VALIDATION
-- =============================================================================

/*
-- Example 1: Find games with strong sharp action
SELECT * FROM staging.v_vsin_sharp_action 
WHERE sharp_confidence >= 0.7 AND has_sharp_action = TRUE
ORDER BY game_date DESC, sharp_confidence DESC;

-- Example 2: Analyze consensus for specific team
SELECT * FROM staging.v_vsin_consensus 
WHERE (home_team_normalized = 'Yankees' OR away_team_normalized = 'Yankees')
  AND game_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY game_date DESC;

-- Example 3: Cross-reference with MLB Stats API
SELECT v.*, mlb.official_game_id 
FROM staging.v_vsin_sharp_action v
JOIN staging.action_network_games mlb ON v.mlb_stats_api_game_id = mlb.mlb_stats_api_game_id
WHERE v.has_sharp_action = TRUE;
*/