-- Migration: Add Team Information to Raw Data Tables
-- Purpose: Add team names and identifiers to raw tables for better data completeness
-- Date: 2025-07-22
-- Issue: staging.betting_splits missing team names for SBD records

-- ================================
-- Add Team Information to raw_data.sbd_betting_splits
-- ================================

-- Add columns for team information extracted from competitors section
ALTER TABLE raw_data.sbd_betting_splits 
ADD COLUMN IF NOT EXISTS home_team VARCHAR(100),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(100), 
ADD COLUMN IF NOT EXISTS home_team_abbr VARCHAR(10),
ADD COLUMN IF NOT EXISTS away_team_abbr VARCHAR(10),
ADD COLUMN IF NOT EXISTS home_team_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS away_team_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS game_name VARCHAR(150);

-- Add index for team lookups
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_teams 
ON raw_data.sbd_betting_splits(home_team_abbr, away_team_abbr);

-- Add index for game name lookups
CREATE INDEX IF NOT EXISTS idx_sbd_betting_splits_game_name 
ON raw_data.sbd_betting_splits(game_name);

-- ================================
-- Update staging.betting_splits to include team information
-- ================================

-- Add team columns to staging table
ALTER TABLE staging.betting_splits
ADD COLUMN IF NOT EXISTS home_team VARCHAR(100),
ADD COLUMN IF NOT EXISTS away_team VARCHAR(100),
ADD COLUMN IF NOT EXISTS home_team_abbr VARCHAR(10), 
ADD COLUMN IF NOT EXISTS away_team_abbr VARCHAR(10),
ADD COLUMN IF NOT EXISTS game_name VARCHAR(150);

-- Add index for team lookups in staging
CREATE INDEX IF NOT EXISTS idx_staging_betting_splits_teams
ON staging.betting_splits(home_team_abbr, away_team_abbr);

-- ================================
-- Create view for easy team information access
-- ================================

CREATE OR REPLACE VIEW staging.betting_splits_with_teams AS
SELECT 
    bs.*,
    COALESCE(bs.home_team, g.home_team_normalized) as resolved_home_team,
    COALESCE(bs.away_team, g.away_team_normalized) as resolved_away_team,
    COALESCE(bs.game_name, 
             CONCAT(COALESCE(bs.away_team_abbr, g.away_team_normalized), 
                   ' @ ', 
                   COALESCE(bs.home_team_abbr, g.home_team_normalized))
    ) as resolved_game_name
FROM staging.betting_splits bs
LEFT JOIN staging.games g ON bs.game_id = g.id;

-- Add comment to view
COMMENT ON VIEW staging.betting_splits_with_teams IS 
'View providing betting splits with resolved team information from multiple sources';

-- ================================  
-- Create function to extract team info from raw SBD data
-- ================================

CREATE OR REPLACE FUNCTION extract_team_info_from_sbd_raw()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
    raw_record RECORD;
    competitors_data JSONB;
    home_team_data JSONB;
    away_team_data JSONB;
BEGIN
    -- Process records that don't have team info yet
    FOR raw_record IN 
        SELECT id, raw_response 
        FROM raw_data.sbd_betting_splits 
        WHERE home_team IS NULL 
        AND raw_response IS NOT NULL
    LOOP
        BEGIN
            -- Extract competitors data from raw response
            competitors_data := raw_record.raw_response->'game_data'->'competitors';
            
            IF competitors_data IS NOT NULL THEN
                home_team_data := competitors_data->'home';
                away_team_data := competitors_data->'away';
                
                -- Update record with team information
                UPDATE raw_data.sbd_betting_splits 
                SET 
                    home_team = home_team_data->>'name',
                    away_team = away_team_data->>'name',
                    home_team_abbr = home_team_data->>'abbreviation', 
                    away_team_abbr = away_team_data->>'abbreviation',
                    home_team_id = home_team_data->>'id',
                    away_team_id = away_team_data->>'id',
                    game_name = CONCAT(away_team_data->>'abbreviation', ' @ ', home_team_data->>'abbreviation')
                WHERE id = raw_record.id;
                
                updated_count := updated_count + 1;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            -- Log error but continue processing
            RAISE NOTICE 'Error processing SBD raw record ID %: %', raw_record.id, SQLERRM;
            CONTINUE;
        END;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Add comment to function
COMMENT ON FUNCTION extract_team_info_from_sbd_raw() IS 
'Extracts team information from existing SBD raw data and populates team columns';

-- ================================
-- Run initial data backfill
-- ================================

-- Extract team info from existing raw data
SELECT extract_team_info_from_sbd_raw() as records_updated;

-- ================================
-- Create improved staging pipeline for SBD data
-- ================================

CREATE OR REPLACE FUNCTION process_sbd_to_staging()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    raw_record RECORD;
    betting_records JSONB;
    betting_record JSONB;
    splits_data JSONB;
BEGIN
    -- Process new raw SBD records
    FOR raw_record IN
        SELECT DISTINCT ON (external_matchup_id) 
            id, external_matchup_id, raw_response, home_team, away_team, 
            home_team_abbr, away_team_abbr, game_name, collected_at
        FROM raw_data.sbd_betting_splits
        WHERE home_team IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM staging.betting_splits 
            WHERE sportsbook_name = 'SBD' 
            AND processed_at >= raw_record.collected_at - INTERVAL '1 hour'
            AND COALESCE(home_team_abbr, '') = COALESCE(raw_record.home_team_abbr, '')
            AND COALESCE(away_team_abbr, '') = COALESCE(raw_record.away_team_abbr, '')
        )
        ORDER BY external_matchup_id, collected_at DESC
    LOOP
        BEGIN
            -- Extract betting splits data
            splits_data := raw_record.raw_response->'game_data'->'betting_splits';
            
            IF splits_data IS NOT NULL THEN
                -- Insert moneyline splits
                IF splits_data ? 'moneyLine' THEN
                    INSERT INTO staging.betting_splits (
                        game_id, sportsbook_name, bet_type,
                        public_bet_percentage, public_money_percentage,
                        home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                        processed_at
                    ) VALUES (
                        NULL, -- Will be resolved later via game matching
                        'SBD',
                        'moneyline',
                        (splits_data->'moneyLine'->'home'->>'betsPercentage')::DECIMAL / 100.0,
                        (splits_data->'moneyLine'->'home'->>'stakePercentage')::DECIMAL / 100.0,
                        raw_record.home_team,
                        raw_record.away_team,
                        raw_record.home_team_abbr,
                        raw_record.away_team_abbr,
                        raw_record.game_name,
                        NOW()
                    );
                    processed_count := processed_count + 1;
                END IF;
                
                -- Insert spread splits (if available)
                IF splits_data ? 'spread' THEN
                    INSERT INTO staging.betting_splits (
                        game_id, sportsbook_name, bet_type,
                        public_bet_percentage, public_money_percentage,
                        home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                        processed_at
                    ) VALUES (
                        NULL,
                        'SBD', 
                        'spread',
                        (splits_data->'spread'->'home'->>'betsPercentage')::DECIMAL / 100.0,
                        (splits_data->'spread'->'home'->>'stakePercentage')::DECIMAL / 100.0,
                        raw_record.home_team,
                        raw_record.away_team,
                        raw_record.home_team_abbr,
                        raw_record.away_team_abbr,
                        raw_record.game_name,
                        NOW()
                    );
                    processed_count := processed_count + 1;
                END IF;
                
                -- Insert total splits (if available)
                IF splits_data ? 'total' THEN
                    INSERT INTO staging.betting_splits (
                        game_id, sportsbook_name, bet_type,
                        public_bet_percentage, public_money_percentage,
                        home_team, away_team, home_team_abbr, away_team_abbr, game_name,
                        processed_at
                    ) VALUES (
                        NULL,
                        'SBD',
                        'totals', 
                        (splits_data->'total'->'over'->>'betsPercentage')::DECIMAL / 100.0,
                        (splits_data->'total'->'over'->>'stakePercentage')::DECIMAL / 100.0,
                        raw_record.home_team,
                        raw_record.away_team, 
                        raw_record.home_team_abbr,
                        raw_record.away_team_abbr,
                        raw_record.game_name,
                        NOW()
                    );
                    processed_count := processed_count + 1;
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

-- Add comment to function
COMMENT ON FUNCTION process_sbd_to_staging() IS
'Processes SBD raw data to staging.betting_splits with complete team information';

-- ================================
-- Grant permissions
-- ================================

GRANT SELECT ON staging.betting_splits_with_teams TO PUBLIC;
GRANT EXECUTE ON FUNCTION extract_team_info_from_sbd_raw() TO PUBLIC;
GRANT EXECUTE ON FUNCTION process_sbd_to_staging() TO PUBLIC;

-- ================================
-- Summary
-- ================================

-- This migration adds:
-- 1. Team information columns to raw_data.sbd_betting_splits 
-- 2. Team information columns to staging.betting_splits
-- 3. Indexes for efficient team lookups
-- 4. View for resolved team information access
-- 5. Function to backfill existing raw data
-- 6. Function to process raw SBD data to staging with team info
-- 7. Initial backfill of existing data

COMMENT ON TABLE raw_data.sbd_betting_splits IS 
'Raw SBD betting splits data with team information extracted from competitors section';

COMMENT ON TABLE staging.betting_splits IS
'Processed betting splits with team information for easy identification';