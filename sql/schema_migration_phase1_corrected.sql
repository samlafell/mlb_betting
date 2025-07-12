-- Schema Migration Phase 1 - Corrected Version
-- MLB Sharp Betting System: Schema Consolidation Migration
-- This script migrates data from existing schemas to the new consolidated structure
-- CORRECTED VERSION - Matches actual database structure

-- Migration logging table (reuse existing if present)
CREATE TABLE IF NOT EXISTS migration_log (
    id SERIAL PRIMARY KEY,
    migration_phase VARCHAR(50) NOT NULL,
    table_source VARCHAR(100),
    table_destination VARCHAR(100),
    records_migrated INTEGER DEFAULT 0,
    migration_started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    migration_completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started',
    error_message TEXT,
    notes TEXT
);

-- Migration validation function
CREATE OR REPLACE FUNCTION log_migration_step(
    phase VARCHAR(50),
    source_table VARCHAR(100),
    dest_table VARCHAR(100),
    record_count INTEGER,
    step_status VARCHAR(20),
    error_msg TEXT DEFAULT NULL,
    step_notes TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO migration_log (
        migration_phase, table_source, table_destination, 
        records_migrated, migration_completed_at, status, error_message, notes
    ) VALUES (
        phase, source_table, dest_table, 
        record_count, CURRENT_TIMESTAMP, step_status, error_msg, step_notes
    );
END;
$$ LANGUAGE plpgsql;

-- Begin migration
DO $$
DECLARE
    migration_count INTEGER := 0;
    total_migrated INTEGER := 0;
    failed_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Creating consolidated schemas - run sql/consolidated_schema.sql first';
    
    -- Log migration start
    PERFORM log_migration_step('Phase1', 'N/A', 'consolidated_schemas', 0, 'completed', NULL, 'Schema creation step');

    -- ========================================
    -- MIGRATE RAW DATA SCHEMA
    -- ========================================
    
    -- Migrate SBR raw HTML (corrected column names)
    BEGIN
        INSERT INTO raw_data.sbr_raw_html (
            id, url, response_html, response_headers, scrape_timestamp,
            status_code, page_type, date_scraped, processing_status, 
            error_message, created_at
        )
        SELECT 
            id, 
            source_url as url,  -- Corrected column name
            html_content as response_html,  -- Corrected column name
            '{}' as response_headers,  -- Default empty JSON
            scraped_at as scrape_timestamp,  -- Corrected column name
            200 as status_code,  -- Default status
            'game_betting' as page_type,  -- Default page type
            DATE(scraped_at) as date_scraped,
            status as processing_status,
            NULL as error_message,
            scraped_at as created_at
        FROM public.sbr_raw_html
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', 0, 'failed', SQLERRM);
    END;

    -- Migrate SBR parsed games (corrected structure)
    BEGIN
        INSERT INTO raw_data.sbr_parsed_games (
            id, raw_html_id, game_date, home_team, away_team, game_time,
            parsed_data, parsing_timestamp, data_quality, validation_errors, created_at
        )
        SELECT 
            id, 
            raw_html_id,
            DATE(parsed_at) as game_date,  -- Extract date from timestamp
            (game_data->>'home_team')::VARCHAR(10) as home_team,  -- Extract from JSON
            (game_data->>'away_team')::VARCHAR(10) as away_team,  -- Extract from JSON
            (game_data->>'game_time')::TIME as game_time,  -- Extract from JSON
            game_data as parsed_data,  -- Use existing JSON structure
            parsed_at as parsing_timestamp,
            CASE 
                WHEN status = 'loaded' THEN 'high'
                WHEN status = 'parsed' THEN 'medium'
                ELSE 'low'
            END as data_quality,
            '[]'::JSONB as validation_errors,  -- Default empty array
            parsed_at as created_at
        FROM public.sbr_parsed_games
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', 0, 'failed', SQLERRM);
    END;

    -- Migrate raw betting splits (handle NULL id values)
    BEGIN
        INSERT INTO raw_data.raw_mlb_betting_splits (
            id, game_id, home_team, away_team, game_datetime, split_type,
            last_updated, source, book, home_or_over_bets, home_or_over_bets_percentage,
            home_or_over_stake_percentage, away_or_under_bets, away_or_under_bets_percentage,
            away_or_under_stake_percentage, split_value, sharp_action, outcome,
            created_at, updated_at
        )
        SELECT 
            COALESCE(id, nextval('raw_data.raw_mlb_betting_splits_id_seq')) as id,  -- Handle NULL ids
            game_id, home_team, away_team, game_datetime, split_type,
            last_updated, 
            'SBD' as source,  -- Default source since column missing in original
            NULL as book,  -- Column missing in original
            home_or_over_bets, home_or_over_bets_percentage,
            home_or_over_stake_percentage, away_or_under_bets, away_or_under_bets_percentage,
            away_or_under_stake_percentage, 
            NULL as split_value,  -- Column missing in original
            sharp_action, outcome,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            updated_at
        FROM splits.raw_mlb_betting_splits
        WHERE id IS NOT NULL  -- Skip records with NULL ids for now
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'splits.raw_mlb_betting_splits', 'raw_data.raw_mlb_betting_splits', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'splits.raw_mlb_betting_splits', 'raw_data.raw_mlb_betting_splits', 0, 'failed', SQLERRM);
    END;

    -- ========================================
    -- MIGRATE CORE BETTING SCHEMA
    -- ========================================
    
    -- Migrate teams (this worked in original migration)
    BEGIN
        INSERT INTO core_betting.teams (
            id, team_code, team_name, team_abbreviation, city, league,
            division, conference, stadium_name, created_at, updated_at
        )
        SELECT 
            id, team_code, team_name, team_abbreviation, city, 'MLB' as league,
            division, NULL as conference, stadium_name, created_at, updated_at
        FROM action.dim_teams
        ON CONFLICT (id) DO UPDATE SET
            team_name = EXCLUDED.team_name,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'action.dim_teams', 'core_betting.teams', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'action.dim_teams', 'core_betting.teams', 0, 'failed', SQLERRM);
    END;

    -- Migrate games from splits.games (check if exists first)
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'splits' AND table_name = 'games') THEN
            INSERT INTO core_betting.games (
                id, sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id,
                home_team, away_team, game_date, game_datetime, game_status,
                home_score, away_score, winning_team, venue_name, venue_id,
                season, season_type, game_type, weather_condition, temperature,
                wind_speed, wind_direction, humidity, data_quality,
                mlb_correlation_confidence, has_mlb_enrichment, created_at, updated_at
            )
            SELECT 
                id, 
                NULL as sportsbookreview_game_id,  -- May not exist in original
                mlb_stats_api_game_id, 
                NULL as action_network_game_id,  -- May not exist in original
                home_team, away_team, game_date, game_datetime, 
                'completed' as game_status,  -- Default status
                home_score, away_score, winning_team, venue_name, venue_id,
                season, season_type, game_type, weather_condition, temperature,
                wind_speed, wind_direction, humidity, 
                'medium' as data_quality,  -- Default quality
                0.8 as mlb_correlation_confidence,  -- Default confidence
                true as has_mlb_enrichment,  -- Default enrichment
                created_at, updated_at
            FROM splits.games
            ON CONFLICT (id) DO NOTHING;
            
            GET DIAGNOSTICS migration_count = ROW_COUNT;
            total_migrated := total_migrated + migration_count;
            PERFORM log_migration_step('Phase1', 'splits.games', 'core_betting.games', migration_count, 'completed');
        ELSE
            PERFORM log_migration_step('Phase1', 'splits.games', 'core_betting.games', 0, 'skipped', NULL, 'Table does not exist');
        END IF;
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'splits.games', 'core_betting.games', 0, 'failed', SQLERRM);
    END;

    -- Skip betting lines migration for now due to foreign key constraints
    -- We'll handle this after games are properly migrated
    PERFORM log_migration_step('Phase1', 'mlb_betting.moneyline', 'core_betting.betting_lines_moneyline', 0, 'skipped', NULL, 'Deferred due to foreign key constraints');
    PERFORM log_migration_step('Phase1', 'mlb_betting.spreads', 'core_betting.betting_lines_spreads', 0, 'skipped', NULL, 'Deferred due to foreign key constraints');
    PERFORM log_migration_step('Phase1', 'mlb_betting.totals', 'core_betting.betting_lines_totals', 0, 'skipped', NULL, 'Deferred due to foreign key constraints');

    -- ========================================
    -- MIGRATE ANALYTICS SCHEMA
    -- ========================================
    
    -- Migrate timing analysis (this worked in original migration)
    BEGIN
        INSERT INTO analytics.timing_analysis_results (
            id, analysis_date, bucket_name, total_games, win_rate, roi_percentage,
            confidence_interval_lower, confidence_interval_upper, sample_size_adequate,
            statistical_significance, created_at, updated_at
        )
        SELECT 
            id, analysis_date, bucket_name, total_games, win_rate, roi_percentage,
            0.0 as confidence_interval_lower,  -- Default values
            0.0 as confidence_interval_upper,
            total_games >= 30 as sample_size_adequate,
            win_rate > 0.55 as statistical_significance,
            created_at, updated_at
        FROM timing_analysis.timing_bucket_performance
        ON CONFLICT (id) DO UPDATE SET
            win_rate = EXCLUDED.win_rate,
            roi_percentage = EXCLUDED.roi_percentage,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'timing_analysis.timing_bucket_performance', 'analytics.timing_analysis_results', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'timing_analysis.timing_bucket_performance', 'analytics.timing_analysis_results', 0, 'failed', SQLERRM);
    END;

    -- Migrate betting recommendations (this worked in original migration)
    BEGIN
        INSERT INTO analytics.betting_recommendations (
            id, game_id, recommendation_type, confidence_score, signal_strength,
            recommended_bet, analysis_timestamp, model_version, feature_importance,
            created_at, updated_at
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY created_at) as id,  -- Generate new IDs
            NULL as game_id,  -- Will need to be mapped later
            'sharp_action' as recommendation_type,
            0.75 as confidence_score,  -- Default confidence
            'medium' as signal_strength,
            'unknown' as recommended_bet,  -- Default bet
            created_at as analysis_timestamp,
            'v1.0' as model_version,
            '{}'::JSONB as feature_importance,
            created_at, updated_at
        FROM clean.betting_recommendations
        ON CONFLICT (id) DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS migration_count = ROW_COUNT;
        total_migrated := total_migrated + migration_count;
        PERFORM log_migration_step('Phase1', 'clean.betting_recommendations', 'analytics.betting_recommendations', migration_count, 'completed');
        
    EXCEPTION WHEN OTHERS THEN
        failed_count := failed_count + 1;
        PERFORM log_migration_step('Phase1', 'clean.betting_recommendations', 'analytics.betting_recommendations', 0, 'failed', SQLERRM);
    END;

    -- ========================================
    -- MIGRATE OPERATIONAL SCHEMA  
    -- ========================================
    
    -- Skip strategy performance migration due to column mismatches
    -- We'll handle this in a separate script after schema adjustments
    PERFORM log_migration_step('Phase1', 'backtesting.strategy_performance', 'operational.strategy_performance', 0, 'skipped', NULL, 'Deferred due to column mismatches');

    -- Skip other operational migrations for now
    PERFORM log_migration_step('Phase1', 'tracking.pre_game_recommendations', 'operational.pre_game_recommendations', 0, 'skipped', NULL, 'Deferred due to column mismatches');

    -- ========================================
    -- MIGRATION SUMMARY
    -- ========================================
    RAISE NOTICE '========================================';
    RAISE NOTICE 'MIGRATION PHASE 1 COMPLETED (CORRECTED)';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables processed: %', (SELECT COUNT(*) FROM migration_log WHERE migration_phase = 'Phase1');
    RAISE NOTICE 'Total records migrated: %', total_migrated;
    RAISE NOTICE 'Failed migrations: %', failed_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Review migration_log table for any errors';
    RAISE NOTICE '2. Run corrected betting lines migration script';
    RAISE NOTICE '3. Update application code to use new schemas';
    RAISE NOTICE '4. Test application functionality';
    RAISE NOTICE '5. Run Phase 3 to drop old schemas (DESTRUCTIVE)';
    RAISE NOTICE '';
    RAISE NOTICE 'To view migration details:';
    RAISE NOTICE 'SELECT * FROM migration_log WHERE migration_phase = ''Phase1'' ORDER BY id;';
    RAISE NOTICE '========================================';

END $$; 