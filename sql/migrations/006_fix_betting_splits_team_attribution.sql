-- Migration: Fix Betting Splits Team Attribution 
-- Purpose: Add team_side column to clearly specify which team betting percentages refer to
-- Date: 2025-07-22
-- Issue: Ambiguous betting percentages - unclear if for home or away team

-- ================================
-- Add team_side column to staging.betting_splits
-- ================================

-- Add column to specify which team/side the percentages are for
ALTER TABLE staging.betting_splits 
ADD COLUMN IF NOT EXISTS team_side VARCHAR(10);

-- Add constraint to ensure valid values
ALTER TABLE staging.betting_splits 
ADD CONSTRAINT check_team_side_valid 
CHECK (team_side IN ('home', 'away', 'over', 'under'));

-- Create index for team_side lookups
CREATE INDEX IF NOT EXISTS idx_staging_betting_splits_team_side 
ON staging.betting_splits(team_side);

-- Create compound index for efficient team-specific queries
CREATE INDEX IF NOT EXISTS idx_staging_betting_splits_game_team_side 
ON staging.betting_splits(game_id, team_side, bet_type);

-- ================================
-- Update existing data to have proper team_side attribution
-- ================================

-- For now, mark existing ambiguous records as 'unknown' 
-- (They will be reprocessed from raw data)
UPDATE staging.betting_splits 
SET team_side = 'unknown' 
WHERE team_side IS NULL;

-- ================================
-- Create improved staging processing function
-- ================================

CREATE OR REPLACE FUNCTION process_sbd_splits_with_team_sides()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    raw_record RECORD;
    splits_data JSONB;
    moneyline_data JSONB;
    spread_data JSONB;
    total_data JSONB;
BEGIN
    -- Process new raw SBD records with team data
    FOR raw_record IN
        SELECT DISTINCT ON (external_matchup_id) 
            id, external_matchup_id, raw_response, home_team, away_team, 
            home_team_abbr, away_team_abbr, game_name, collected_at
        FROM raw_data.sbd_betting_splits
        WHERE home_team IS NOT NULL
        AND created_at >= NOW() - INTERVAL '24 hours'  -- Only recent data
        ORDER BY external_matchup_id, collected_at DESC
    LOOP
        BEGIN
            -- Extract betting splits data
            splits_data := raw_record.raw_response->'game_data'->'betting_splits';
            
            IF splits_data IS NOT NULL THEN
                
                -- Process moneyline splits (home and away)
                moneyline_data := splits_data->'moneyline';
                IF moneyline_data IS NOT NULL THEN
                    -- Home team moneyline
                    IF moneyline_data ? 'home' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL, -- Will be resolved later via game matching
                            'SBD',
                            'moneyline',
                            'home',
                            (moneyline_data->'home'->>'betsPercentage')::DECIMAL / 100.0,
                            (moneyline_data->'home'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                    
                    -- Away team moneyline
                    IF moneyline_data ? 'away' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL,
                            'SBD',
                            'moneyline',
                            'away',
                            (moneyline_data->'away'->>'betsPercentage')::DECIMAL / 100.0,
                            (moneyline_data->'away'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                END IF;
                
                -- Process spread splits (home and away)
                spread_data := splits_data->'spread';
                IF spread_data IS NOT NULL THEN
                    -- Home team spread
                    IF spread_data ? 'home' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL,
                            'SBD',
                            'spread',
                            'home',
                            (spread_data->'home'->>'betsPercentage')::DECIMAL / 100.0,
                            (spread_data->'home'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                    
                    -- Away team spread  
                    IF spread_data ? 'away' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL,
                            'SBD',
                            'spread',
                            'away',
                            (spread_data->'away'->>'betsPercentage')::DECIMAL / 100.0,
                            (spread_data->'away'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                END IF;
                
                -- Process total splits (over and under)
                total_data := splits_data->'total';
                IF total_data IS NOT NULL THEN
                    -- Over
                    IF total_data ? 'over' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL,
                            'SBD',
                            'totals',
                            'over',
                            (total_data->'over'->>'betsPercentage')::DECIMAL / 100.0,
                            (total_data->'over'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                    
                    -- Under
                    IF total_data ? 'under' THEN
                        INSERT INTO staging.betting_splits (
                            game_id, sportsbook_name, bet_type, team_side,
                            public_bet_percentage, public_money_percentage,
                            home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                            processed_at
                        ) VALUES (
                            NULL,
                            'SBD',
                            'totals',
                            'under',
                            (total_data->'under'->>'betsPercentage')::DECIMAL / 100.0,
                            (total_data->'under'->>'stakePercentage')::DECIMAL / 100.0,
                            raw_record.home_team,
                            raw_record.away_team,
                            raw_record.home_team_abbr,
                            raw_record.away_team_abbr,
                            raw_record.game_name,
                            NOW()
                        )
                        ON CONFLICT DO NOTHING;
                        processed_count := processed_count + 1;
                    END IF;
                END IF;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing SBD staging record %: %', raw_record.external_matchup_id, SQLERRM;
            CONTINUE;
        END;
    END LOOP;
    
    RETURN processed_count;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Create validation function for betting splits
-- ================================

CREATE OR REPLACE FUNCTION validate_betting_splits_totals()
RETURNS TABLE(
    game_name TEXT,
    bet_type TEXT,
    total_bet_percentage DECIMAL,
    total_money_percentage DECIMAL,
    is_valid BOOLEAN,
    issue TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH split_totals AS (
        SELECT 
            bs.game_name,
            bs.bet_type,
            SUM(bs.public_bet_percentage) as total_bet_pct,
            SUM(bs.public_money_percentage) as total_money_pct
        FROM staging.betting_splits bs
        WHERE bs.team_side IN ('home', 'away') 
        AND bs.game_name IS NOT NULL
        AND bs.bet_type IN ('moneyline', 'spread')
        GROUP BY bs.game_name, bs.bet_type
        
        UNION ALL
        
        SELECT 
            bs.game_name,
            bs.bet_type,
            SUM(bs.public_bet_percentage) as total_bet_pct,
            SUM(bs.public_money_percentage) as total_money_pct
        FROM staging.betting_splits bs
        WHERE bs.team_side IN ('over', 'under') 
        AND bs.game_name IS NOT NULL
        AND bs.bet_type = 'totals'
        GROUP BY bs.game_name, bs.bet_type
    )
    SELECT 
        st.game_name::TEXT,
        st.bet_type::TEXT,
        st.total_bet_pct,
        st.total_money_pct,
        (st.total_bet_pct BETWEEN 0.95 AND 1.05 AND st.total_money_pct BETWEEN 0.95 AND 1.05) as is_valid,
        CASE 
            WHEN st.total_bet_pct < 0.95 OR st.total_bet_pct > 1.05 THEN 'Bet percentage total outside expected range'
            WHEN st.total_money_pct < 0.95 OR st.total_money_pct > 1.05 THEN 'Money percentage total outside expected range'
            ELSE 'Valid'
        END::TEXT as issue
    FROM split_totals st;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- Create enhanced view for betting splits analysis
-- ================================

CREATE OR REPLACE VIEW staging.betting_splits_analysis AS
SELECT 
    bs.*,
    -- Add helpful flags for analysis
    CASE bs.team_side
        WHEN 'home' THEN bs.home_team_abbr
        WHEN 'away' THEN bs.away_team_abbr
        WHEN 'over' THEN 'OVER'
        WHEN 'under' THEN 'UNDER'
        ELSE 'UNKNOWN'
    END as side_display,
    
    -- Calculate sharp action indicators
    CASE 
        WHEN bs.public_money_percentage > bs.public_bet_percentage + 0.15 
        THEN 'SHARP_MONEY'
        WHEN bs.public_bet_percentage > bs.public_money_percentage + 0.15 
        THEN 'PUBLIC_HEAVY'
        ELSE 'BALANCED'
    END as money_pattern,
    
    -- Add percentage formatting for display
    ROUND(bs.public_bet_percentage * 100, 1) as bet_pct_display,
    ROUND(bs.public_money_percentage * 100, 1) as money_pct_display
    
FROM staging.betting_splits bs
WHERE bs.team_side IS NOT NULL 
AND bs.team_side != 'unknown';

-- ================================
-- Run initial processing of existing data
-- ================================

-- Process recent SBD data with new team-side structure
SELECT process_sbd_splits_with_team_sides() as records_processed;

-- ================================
-- Grant permissions
-- ================================

GRANT SELECT ON staging.betting_splits_analysis TO PUBLIC;
GRANT EXECUTE ON FUNCTION process_sbd_splits_with_team_sides() TO PUBLIC;
GRANT EXECUTE ON FUNCTION validate_betting_splits_totals() TO PUBLIC;

-- ================================
-- Add comments for documentation
-- ================================

COMMENT ON COLUMN staging.betting_splits.team_side IS 
'Specifies which team/side the betting percentages refer to: home, away, over, under';

COMMENT ON FUNCTION process_sbd_splits_with_team_sides() IS
'Processes SBD raw data into staging with proper team-side attribution for each betting percentage';

COMMENT ON FUNCTION validate_betting_splits_totals() IS
'Validates that betting splits percentages add up correctly (home+away ≈ 100%, over+under ≈ 100%)';

COMMENT ON VIEW staging.betting_splits_analysis IS
'Enhanced view of betting splits with team-side attribution and analysis fields for easy querying';

-- ================================
-- Summary
-- ================================

-- This migration adds:
-- 1. team_side column to clearly specify which team betting percentages refer to
-- 2. Function to process SBD data with proper team-side attribution  
-- 3. Validation function to ensure percentages add up correctly
-- 4. Enhanced view for easier analysis and querying
-- 5. Proper indexes for efficient team-side lookups
-- 6. Initial processing of existing raw data with new structure