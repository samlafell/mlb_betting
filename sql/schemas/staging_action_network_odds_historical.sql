-- =============================================================================
-- STAGING Action Network Historical Odds Schema  
-- =============================================================================
-- Purpose: Capture complete line movement history with exact timestamps
-- Key Feature: Every line change gets its own record with updated_at from JSON
--
-- Example Data Flow:
-- JSON: "history": [{"odds": -105, "updated_at": "2025-07-21T17:39:30.195056Z"}]
-- →  Record: game_123 | DK | total | under | -105 | 8.0 | 2025-07-21T17:39:30.195056Z
--
-- Matching Logic: Find closest timestamps between over/under (or home/away)
-- Example: Under at 17:39:30.195056Z matches Over at 17:39:30.200068Z (5ms diff)
-- =============================================================================

DROP TABLE IF EXISTS staging.action_network_odds_historical CASCADE;

CREATE TABLE staging.action_network_odds_historical (
    id BIGSERIAL PRIMARY KEY,
    
    -- =============================================================================
    -- GAME AND SPORTSBOOK IDENTIFICATION
    -- =============================================================================
    external_game_id VARCHAR(255) NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),  -- Cross-system integration
    sportsbook_external_id VARCHAR(50) NOT NULL,
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(255),
    
    -- =============================================================================
    -- MARKET AND SIDE IDENTIFICATION
    -- =============================================================================
    market_type VARCHAR(20) NOT NULL,   -- moneyline, spread, total
    side VARCHAR(10) NOT NULL,          -- home, away, over, under
    
    -- =============================================================================
    -- ODDS DATA
    -- =============================================================================
    odds INTEGER NOT NULL,              -- The actual odds value from history
    line_value DECIMAL(4,1),            -- Spread/total value, NULL for moneyline
    
    -- =============================================================================
    -- CRITICAL TIMING INFORMATION
    -- =============================================================================
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,  -- From JSON history.updated_at
    data_collection_time TIMESTAMP WITH TIME ZONE, -- When we pulled from API  
    data_processing_time TIMESTAMP WITH TIME ZONE, -- When we processed this record
    
    -- =============================================================================
    -- LINE STATUS AND METADATA
    -- =============================================================================
    line_status VARCHAR(50),            -- opener, normal, suspended (from JSON)
    is_current_odds BOOLEAN DEFAULT FALSE,  -- TRUE if this is the latest odds
    
    -- Action Network metadata
    market_id BIGINT,                   -- Action Network market ID
    outcome_id BIGINT,                  -- Action Network outcome ID
    period VARCHAR(50) DEFAULT 'event',
    
    -- =============================================================================
    -- DATA QUALITY AND LINEAGE
    -- =============================================================================
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(50) DEFAULT 'valid',
    
    -- Lineage tracking
    raw_data_id BIGINT REFERENCES raw_data.action_network_odds(id),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at_record TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- =============================================================================
    -- CONSTRAINTS
    -- =============================================================================
    -- One record per timestamp per side per market per sportsbook
    UNIQUE(external_game_id, sportsbook_external_id, market_type, side, updated_at),
    
    -- Validate market types and sides
    CONSTRAINT valid_market_types CHECK (market_type IN ('moneyline', 'spread', 'total')),
    CONSTRAINT valid_sides CHECK (side IN ('home', 'away', 'over', 'under')),
    
    -- Ensure side/market type compatibility
    CONSTRAINT side_market_compatibility CHECK (
        (market_type = 'moneyline' AND side IN ('home', 'away')) OR
        (market_type = 'spread' AND side IN ('home', 'away')) OR  
        (market_type = 'total' AND side IN ('over', 'under'))
    ),
    
    -- Line value logic
    CONSTRAINT line_value_logic CHECK (
        (market_type = 'moneyline' AND line_value IS NULL) OR
        (market_type IN ('spread', 'total') AND line_value IS NOT NULL)
    )
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Primary lookup indexes
CREATE INDEX idx_historical_odds_game_id ON staging.action_network_odds_historical(external_game_id);
CREATE INDEX idx_historical_odds_mlb_id ON staging.action_network_odds_historical(mlb_stats_api_game_id);
CREATE INDEX idx_historical_odds_sportsbook ON staging.action_network_odds_historical(sportsbook_external_id, sportsbook_name);

-- Market analysis indexes  
CREATE INDEX idx_historical_odds_market_side ON staging.action_network_odds_historical(market_type, side);
CREATE INDEX idx_historical_odds_market_combo ON staging.action_network_odds_historical(external_game_id, market_type, side);

-- Critical timing indexes
CREATE INDEX idx_historical_odds_updated_at ON staging.action_network_odds_historical(updated_at);
CREATE INDEX idx_historical_odds_timing_combo ON staging.action_network_odds_historical(external_game_id, market_type, updated_at);
CREATE INDEX idx_historical_odds_collection_time ON staging.action_network_odds_historical(data_collection_time);

-- Current odds tracking
CREATE INDEX idx_historical_odds_current ON staging.action_network_odds_historical(external_game_id, market_type, is_current_odds);

-- Composite indexes for common queries
CREATE INDEX idx_historical_odds_game_market_time ON staging.action_network_odds_historical(external_game_id, market_type, side, updated_at);
CREATE INDEX idx_historical_odds_sbook_market_time ON staging.action_network_odds_historical(sportsbook_name, market_type, updated_at);

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

-- View to find closest timestamp matches between sides
CREATE OR REPLACE VIEW staging.v_closest_odds_pairs AS
WITH side_pairs AS (
    SELECT 
        h1.external_game_id,
        h1.sportsbook_name,
        h1.market_type,
        h1.side as side1,
        h2.side as side2,
        h1.updated_at as time1,
        h2.updated_at as time2,
        h1.odds as odds1,
        h2.odds as odds2,
        h1.line_value,
        ABS(EXTRACT(EPOCH FROM (h1.updated_at - h2.updated_at))) as time_diff_seconds,
        ABS(EXTRACT(MICROSECONDS FROM (h1.updated_at - h2.updated_at))) as time_diff_microseconds
    FROM staging.action_network_odds_historical h1
    JOIN staging.action_network_odds_historical h2 ON (
        h1.external_game_id = h2.external_game_id AND
        h1.sportsbook_external_id = h2.sportsbook_external_id AND
        h1.market_type = h2.market_type AND
        h1.side != h2.side
    )
    WHERE 
        -- Only match compatible sides
        (h1.market_type IN ('moneyline', 'spread') AND h1.side IN ('home', 'away') AND h2.side IN ('home', 'away')) OR
        (h1.market_type = 'total' AND h1.side IN ('over', 'under') AND h2.side IN ('over', 'under'))
),
ranked_pairs AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY external_game_id, sportsbook_name, market_type, side1, time1 
            ORDER BY time_diff_microseconds ASC
        ) as rn
    FROM side_pairs
)
SELECT 
    external_game_id,
    sportsbook_name, 
    market_type,
    side1,
    side2,
    time1,
    time2,
    odds1,
    odds2,
    line_value,
    time_diff_seconds,
    time_diff_microseconds,
    CASE 
        WHEN time_diff_microseconds < 1000000 THEN 'Simultaneous (< 1s)'
        WHEN time_diff_microseconds < 10000000 THEN 'Near-simultaneous (< 10s)'  
        WHEN time_diff_microseconds < 60000000 THEN 'Close (< 1m)'
        ELSE 'Distant (> 1m)'
    END as timing_category
FROM ranked_pairs 
WHERE rn = 1;  -- Only closest match for each record

-- View for line movement analysis
CREATE OR REPLACE VIEW staging.v_line_movements AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    line_value,
    odds,
    updated_at,
    LAG(odds) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at
    ) as previous_odds,
    LAG(updated_at) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at  
    ) as previous_updated_at,
    odds - LAG(odds) OVER (
        PARTITION BY external_game_id, sportsbook_name, market_type, side 
        ORDER BY updated_at
    ) as odds_change,
    EXTRACT(EPOCH FROM (
        updated_at - LAG(updated_at) OVER (
            PARTITION BY external_game_id, sportsbook_name, market_type, side 
            ORDER BY updated_at
        )
    )) as seconds_since_last_change
FROM staging.action_network_odds_historical
ORDER BY external_game_id, sportsbook_name, market_type, side, updated_at;

-- Current odds view (latest timestamp per market/side/sportsbook)
CREATE OR REPLACE VIEW staging.v_current_odds_from_historical AS
WITH latest_odds AS (
    SELECT 
        external_game_id,
        sportsbook_name,
        market_type,
        side,
        MAX(updated_at) as latest_updated_at
    FROM staging.action_network_odds_historical
    GROUP BY external_game_id, sportsbook_name, market_type, side
)
SELECT h.*
FROM staging.action_network_odds_historical h
JOIN latest_odds l ON (
    h.external_game_id = l.external_game_id AND
    h.sportsbook_name = l.sportsbook_name AND
    h.market_type = l.market_type AND
    h.side = l.side AND
    h.updated_at = l.latest_updated_at
);

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Function to find closest timestamp matches
CREATE OR REPLACE FUNCTION staging.find_closest_odds_at_time(
    p_external_game_id VARCHAR(255),
    p_market_type VARCHAR(20),
    p_target_timestamp TIMESTAMP WITH TIME ZONE,
    p_max_time_diff_seconds INTEGER DEFAULT 300  -- 5 minute window
)
RETURNS TABLE(
    sportsbook_name TEXT,
    side TEXT,
    odds INTEGER,
    line_value DECIMAL(4,1),
    updated_at TIMESTAMP WITH TIME ZONE,
    time_diff_seconds NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.sportsbook_name::TEXT,
        h.side::TEXT,
        h.odds,
        h.line_value,
        h.updated_at,
        ABS(EXTRACT(EPOCH FROM (h.updated_at - p_target_timestamp))) as time_diff_seconds
    FROM staging.action_network_odds_historical h
    WHERE h.external_game_id = p_external_game_id
      AND h.market_type = p_market_type
      AND ABS(EXTRACT(EPOCH FROM (h.updated_at - p_target_timestamp))) <= p_max_time_diff_seconds
    ORDER BY ABS(EXTRACT(EPOCH FROM (h.updated_at - p_target_timestamp))) ASC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- DOCUMENTATION AND COMMENTS
-- =============================================================================

COMMENT ON TABLE staging.action_network_odds_historical IS 
'Historical odds data with complete line movement history and exact timestamps from Action Network API. Each record represents a specific point in time when odds changed.';

COMMENT ON COLUMN staging.action_network_odds_historical.updated_at IS 
'Exact timestamp when this specific odds value was active, extracted from JSON history.updated_at field';

COMMENT ON COLUMN staging.action_network_odds_historical.side IS 
'Market side: home/away for moneyline and spread, over/under for totals';

COMMENT ON COLUMN staging.action_network_odds_historical.is_current_odds IS 
'TRUE if this represents the most recent odds for this market/side/sportsbook combination';

COMMENT ON COLUMN staging.action_network_odds_historical.line_status IS 
'Line status from JSON: opener, normal, suspended, etc.';

COMMENT ON VIEW staging.v_closest_odds_pairs IS 
'Finds closest timestamp matches between market sides (over/under, home/away) for synchronized odds analysis';

COMMENT ON VIEW staging.v_line_movements IS 
'Tracks line movements over time showing odds changes and timing between movements';

COMMENT ON FUNCTION staging.find_closest_odds_at_time IS 
'Utility function to find odds closest to a specific timestamp within a time window';

-- =============================================================================
-- EXAMPLE QUERIES FOR VALIDATION
-- =============================================================================

/*
-- Example 1: Find all historical odds for a specific market
SELECT sportsbook_name, side, odds, line_value, updated_at, line_status
FROM staging.action_network_odds_historical 
WHERE external_game_id = '258064' AND market_type = 'total'
ORDER BY sportsbook_name, side, updated_at;

-- Example 2: Find closest timestamp matches (like the user's example)
SELECT * FROM staging.v_closest_odds_pairs 
WHERE external_game_id = '258064' AND market_type = 'total' 
  AND time_diff_microseconds < 10000000  -- Within 10 seconds
ORDER BY time_diff_microseconds;

-- Example 3: Track line movements for sharp betting analysis
SELECT * FROM staging.v_line_movements
WHERE external_game_id = '258064' AND market_type = 'total' AND side = 'under'
  AND odds_change IS NOT NULL
ORDER BY updated_at;

-- Example 4: Find odds at specific time (like the user's timestamp)
SELECT * FROM staging.find_closest_odds_at_time(
    '258064', 
    'total', 
    '2025-07-21T17:39:30.195056Z'::timestamp with time zone,
    10  -- 10 second window
);

-- Example 5: Current odds (latest for each market/side/sportsbook)
SELECT * FROM staging.v_current_odds_from_historical
WHERE external_game_id = '258064' AND market_type = 'total';
*/