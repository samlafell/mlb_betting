-- Migration 018: Add Betting Percentages to Action Network Staging
-- Purpose: Capture bet% and money% data from Action Network History responses
-- Issue: Betting percentage data is collected but not processed into staging tables
-- Date: 2025-07-25

-- ================================
-- Phase 1: Add Betting Percentage Columns
-- ================================

-- Add betting percentage columns to the staging table
ALTER TABLE staging.action_network_odds_historical 
ADD COLUMN IF NOT EXISTS bet_percent_tickets INTEGER,     -- Ticket percentage (0-100)
ADD COLUMN IF NOT EXISTS bet_percent_money INTEGER,       -- Money percentage (0-100)  
ADD COLUMN IF NOT EXISTS bet_value_tickets BIGINT,        -- Actual ticket count (if available)
ADD COLUMN IF NOT EXISTS bet_value_money BIGINT,          -- Actual money amount (if available)
ADD COLUMN IF NOT EXISTS bet_info_available BOOLEAN DEFAULT FALSE;  -- Indicates if bet data exists

-- ================================
-- Phase 2: Add Data Validation Constraints
-- ================================

-- Ensure percentage values are within valid ranges
ALTER TABLE staging.action_network_odds_historical 
ADD CONSTRAINT IF NOT EXISTS valid_bet_percent_tickets 
    CHECK (bet_percent_tickets IS NULL OR (bet_percent_tickets >= 0 AND bet_percent_tickets <= 100));

ALTER TABLE staging.action_network_odds_historical 
ADD CONSTRAINT IF NOT EXISTS valid_bet_percent_money 
    CHECK (bet_percent_money IS NULL OR (bet_percent_money >= 0 AND bet_percent_money <= 100));

-- Logical constraint: if betting info is available, at least one percentage should exist
ALTER TABLE staging.action_network_odds_historical 
ADD CONSTRAINT IF NOT EXISTS bet_info_consistency
    CHECK (
        (bet_info_available = FALSE) OR 
        (bet_info_available = TRUE AND (bet_percent_tickets IS NOT NULL OR bet_percent_money IS NOT NULL))
    );

-- ================================
-- Phase 3: Add Performance Indexes
-- ================================

-- Index for betting percentage analysis
CREATE INDEX IF NOT EXISTS idx_odds_historical_bet_percentages 
ON staging.action_network_odds_historical(external_game_id, market_type, bet_percent_tickets, bet_percent_money)
WHERE bet_info_available = TRUE;

-- Index for sharp action detection (divergence between tickets and money)
CREATE INDEX IF NOT EXISTS idx_odds_historical_sharp_indicators
ON staging.action_network_odds_historical(external_game_id, sportsbook_name, market_type, bet_percent_tickets, bet_percent_money)
WHERE bet_info_available = TRUE AND ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) >= 10;

-- Composite index for betting analysis queries
CREATE INDEX IF NOT EXISTS idx_odds_historical_betting_analysis
ON staging.action_network_odds_historical(external_game_id, market_type, side, updated_at, bet_info_available)
WHERE bet_info_available = TRUE;

-- ================================
-- Phase 4: Create Betting Analysis Views
-- ================================

-- View for analyzing ticket vs money divergence (sharp action indicator)
CREATE OR REPLACE VIEW staging.v_bet_money_divergence AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    bet_percent_tickets,
    bet_percent_money,
    ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) as percentage_divergence,
    CASE 
        WHEN bet_percent_money > COALESCE(bet_percent_tickets, 50) + 15 THEN 'Sharp Money Heavy'
        WHEN bet_percent_tickets > COALESCE(bet_percent_money, 50) + 15 THEN 'Public Money Heavy'  
        WHEN ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) <= 5 THEN 'Aligned'
        ELSE 'Moderate Divergence'
    END as sharp_indicator,
    updated_at,
    bet_info_available
FROM staging.action_network_odds_historical
WHERE bet_info_available = TRUE
ORDER BY percentage_divergence DESC;

-- View for public vs sharp action summary
CREATE OR REPLACE VIEW staging.v_betting_action_summary AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    
    -- Current betting percentages
    bet_percent_tickets as public_percent,
    bet_percent_money as money_percent,
    
    -- Sharp action classification
    CASE 
        WHEN bet_percent_money > COALESCE(bet_percent_tickets, 50) + 15 THEN 'Sharp Fade Public'
        WHEN bet_percent_tickets > COALESCE(bet_percent_money, 50) + 15 THEN 'Public Heavy'
        WHEN ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) <= 5 THEN 'Balanced Action'
        ELSE 'Moderate Split'
    END as action_type,
    
    -- Divergence strength for sorting
    ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) as divergence_strength,
    
    -- Timing context
    updated_at,
    bet_info_available
    
FROM staging.action_network_odds_historical
WHERE bet_info_available = TRUE
ORDER BY divergence_strength DESC;

-- ================================
-- Phase 5: Update Comments and Documentation
-- ================================

-- Add column comments for clarity
COMMENT ON COLUMN staging.action_network_odds_historical.bet_percent_tickets IS 
'Percentage of tickets on this side (0-100), represents public betting patterns';

COMMENT ON COLUMN staging.action_network_odds_historical.bet_percent_money IS 
'Percentage of money on this side (0-100), represents sharp/professional betting patterns';

COMMENT ON COLUMN staging.action_network_odds_historical.bet_value_tickets IS 
'Actual number of tickets (if provided by Action Network)';

COMMENT ON COLUMN staging.action_network_odds_historical.bet_value_money IS 
'Actual dollar amount (if provided by Action Network)';

COMMENT ON COLUMN staging.action_network_odds_historical.bet_info_available IS 
'TRUE if betting percentage data is available for this record';

-- View comments
COMMENT ON VIEW staging.v_bet_money_divergence IS 
'Analysis of divergence between ticket % and money % to identify sharp action patterns';

COMMENT ON VIEW staging.v_betting_action_summary IS 
'Summary of betting action classification based on public vs sharp money patterns';

-- ================================
-- Phase 6: Create Validation Function
-- ================================

-- Function to validate betting percentage data quality
CREATE OR REPLACE FUNCTION staging.validate_betting_percentage_data()
RETURNS TABLE (
    total_records BIGINT,
    records_with_betting_data BIGINT,
    coverage_percentage NUMERIC,
    avg_ticket_percent NUMERIC,
    avg_money_percent NUMERIC,
    high_divergence_count BIGINT,
    validation_status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE bet_info_available = TRUE) as records_with_betting_data,
        ROUND(
            (COUNT(*) FILTER (WHERE bet_info_available = TRUE)::NUMERIC / COUNT(*)) * 100, 
            2
        ) as coverage_percentage,
        ROUND(AVG(bet_percent_tickets) FILTER (WHERE bet_info_available = TRUE), 1) as avg_ticket_percent,
        ROUND(AVG(bet_percent_money) FILTER (WHERE bet_info_available = TRUE), 1) as avg_money_percent,
        COUNT(*) FILTER (WHERE bet_info_available = TRUE AND ABS(COALESCE(bet_percent_tickets, 50) - COALESCE(bet_percent_money, 50)) >= 15) as high_divergence_count,
        CASE 
            WHEN COUNT(*) FILTER (WHERE bet_info_available = TRUE) > 0
            THEN 'PASS: Betting percentage data available and valid'
            ELSE 'FAIL: No betting percentage data found'
        END as validation_status
    FROM staging.action_network_odds_historical
    WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days';  -- Recent data validation
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Migration Completion Notes  
-- ================================

/*
WHAT THIS MIGRATION DOES:
1. ✅ Adds 5 new columns to capture betting percentage data:
   - bet_percent_tickets: Public betting percentage (tickets)
   - bet_percent_money: Sharp betting percentage (money)
   - bet_value_tickets: Actual ticket count (if available)
   - bet_value_money: Actual money amount (if available)
   - bet_info_available: Flag indicating betting data availability

2. ✅ Adds validation constraints to ensure data quality:
   - Percentage values must be 0-100
   - Logical consistency between flag and data presence

3. ✅ Creates performance indexes for betting analysis:
   - General betting percentage queries
   - Sharp action divergence analysis
   - Composite analysis indexes

4. ✅ Creates analytical views:
   - v_bet_money_divergence: Sharp action identification
   - v_betting_action_summary: Action classification

5. ✅ Adds validation function for data quality monitoring

NEXT STEPS:
1. Update staging processor to populate these columns
2. Enhance line movement views to include betting data
3. Integrate with sharp action detection algorithms

POST-MIGRATION VERIFICATION:
-- Check that new columns exist:
\d staging.action_network_odds_historical

-- Validate constraints and indexes:
SELECT conname, contype FROM pg_constraint 
WHERE conrelid = 'staging.action_network_odds_historical'::regclass
AND conname LIKE '%bet_%';

-- Test validation function:
SELECT * FROM staging.validate_betting_percentage_data();

EXPECTED IMPACT:
- Storage: ~5-10% increase in staging table size
- Performance: Negligible impact on existing queries
- Analytics: Enables enhanced sharp action detection
- Data Quality: Structured betting percentage analysis

The schema is now ready to capture betting percentage data.
Next: Update the history processor to populate these columns.
*/