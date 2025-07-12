-- ==================================================================================
-- MLB Sharp Betting System - Phase 4B: Data Migration for Remaining Schemas
-- ==================================================================================
-- 
-- This script migrates data from the remaining 6 legacy schemas to the consolidated
-- 4-schema structure created in Phase 4A.
--
-- Source Schemas: public, action, splits, tracking, validation, backtesting
-- Target Schemas: raw_data, core_betting, analytics, operational
--
-- PHASE 4B: NON-DESTRUCTIVE - Migrates data but preserves source tables
-- ==================================================================================

-- Enable detailed logging
\set ON_ERROR_STOP on

-- Create migration tracking
CREATE TABLE IF NOT EXISTS operational.phase4b_migration_log (
    id SERIAL PRIMARY KEY,
    migration_step VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    records_migrated INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started',
    error_message TEXT,
    notes TEXT
);

-- Migration logging function
CREATE OR REPLACE FUNCTION log_phase4b_migration(
    p_step VARCHAR(100),
    p_source VARCHAR(100),
    p_target VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO operational.phase4b_migration_log (
        migration_step, source_table, target_table, 
        records_migrated, completed_at, status, error_message, notes
    ) VALUES (
        p_step, p_source, p_target, 
        p_records, NOW(), p_status, p_error, p_notes
    );
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Starting Phase 4B: Data Migration for Remaining Schema Consolidation';
    RAISE NOTICE 'Timestamp: %', NOW();
END $$;

-- ==================================================================================
-- MIGRATE PUBLIC SCHEMA DATA
-- ==================================================================================

-- Migrate SportsbookReview raw HTML data
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    -- Check if source table exists and has data
    SELECT COUNT(*) INTO record_count FROM public.sbr_raw_html;
    
    IF record_count > 0 THEN
        -- Migrate data with conflict resolution
        INSERT INTO raw_data.sbr_raw_html (
            id, source_url, html_content, scraped_at, status, created_at, updated_at
        )
        SELECT 
            id,
            source_url,
            html_content,
            scraped_at,
            status,
            COALESCE(scraped_at, NOW()) as created_at,
            NOW() as updated_at
        FROM public.sbr_raw_html
        ON CONFLICT (id) DO UPDATE SET
            source_url = EXCLUDED.source_url,
            html_content = EXCLUDED.html_content,
            scraped_at = EXCLUDED.scraped_at,
            status = EXCLUDED.status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('public_sbr_raw_html', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', record_count);
        RAISE NOTICE 'Migrated % records from public.sbr_raw_html to raw_data.sbr_raw_html', record_count;
    ELSE
        PERFORM log_phase4b_migration('public_sbr_raw_html', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table public.sbr_raw_html is empty, skipping migration';
    END IF;
END $$;

-- Migrate SportsbookReview parsed games
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.sbr_parsed_games;
    
    IF record_count > 0 THEN
        INSERT INTO raw_data.sbr_parsed_games (
            id, raw_html_id, game_data, parsed_at, status, created_at, updated_at
        )
        SELECT 
            id,
            raw_html_id,
            game_data,
            parsed_at,
            status,
            COALESCE(parsed_at, NOW()) as created_at,
            NOW() as updated_at
        FROM public.sbr_parsed_games
        ON CONFLICT (id) DO UPDATE SET
            raw_html_id = EXCLUDED.raw_html_id,
            game_data = EXCLUDED.game_data,
            parsed_at = EXCLUDED.parsed_at,
            status = EXCLUDED.status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('public_sbr_parsed_games', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', record_count);
        RAISE NOTICE 'Migrated % records from public.sbr_parsed_games to raw_data.sbr_parsed_games', record_count;
    ELSE
        PERFORM log_phase4b_migration('public_sbr_parsed_games', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table public.sbr_parsed_games is empty, skipping migration';
    END IF;
END $$;

-- Migrate game outcomes (enhance existing core_betting.game_outcomes)
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.game_outcomes;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.game_outcomes (
            game_id, home_team, away_team, home_score, away_score, game_date,
            game_datetime, home_pitcher, away_pitcher, weather_conditions,
            temperature, wind_speed, wind_direction, game_status, created_at, updated_at
        )
        SELECT 
            game_id,
            home_team,
            away_team,
            home_score,
            away_score,
            game_date,
            game_datetime,
            home_pitcher,
            away_pitcher,
            weather_conditions,
            temperature,
            wind_speed,
            wind_direction,
            COALESCE(game_status, 'completed') as game_status,
            NOW() as created_at,
            NOW() as updated_at
        FROM public.game_outcomes
        ON CONFLICT (game_id) DO UPDATE SET
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            game_date = EXCLUDED.game_date,
            game_datetime = EXCLUDED.game_datetime,
            home_pitcher = EXCLUDED.home_pitcher,
            away_pitcher = EXCLUDED.away_pitcher,
            weather_conditions = EXCLUDED.weather_conditions,
            temperature = EXCLUDED.temperature,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            game_status = EXCLUDED.game_status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('public_game_outcomes', 'public.game_outcomes', 'core_betting.game_outcomes', record_count);
        RAISE NOTICE 'Migrated % records from public.game_outcomes to core_betting.game_outcomes', record_count;
    ELSE
        PERFORM log_phase4b_migration('public_game_outcomes', 'public.game_outcomes', 'core_betting.game_outcomes', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table public.game_outcomes is empty, skipping migration';
    END IF;
END $$;

-- Migrate migration logs to operational schema
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM public.migration_log;
    
    IF record_count > 0 THEN
        INSERT INTO operational.migration_log (
            id, migration_phase, table_source, table_destination, records_migrated,
            migration_started_at, migration_completed_at, status, error_message, notes
        )
        SELECT 
            id,
            migration_phase,
            table_source,
            table_destination,
            records_migrated,
            migration_started_at,
            migration_completed_at,
            status,
            error_message,
            notes
        FROM public.migration_log
        ON CONFLICT (id) DO UPDATE SET
            migration_phase = EXCLUDED.migration_phase,
            table_source = EXCLUDED.table_source,
            table_destination = EXCLUDED.table_destination,
            records_migrated = EXCLUDED.records_migrated,
            migration_started_at = EXCLUDED.migration_started_at,
            migration_completed_at = EXCLUDED.migration_completed_at,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            notes = EXCLUDED.notes;
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('public_migration_log', 'public.migration_log', 'operational.migration_log', record_count);
        RAISE NOTICE 'Migrated % records from public.migration_log to operational.migration_log', record_count;
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE ACTION SCHEMA DATA
-- ==================================================================================

-- Migrate Action Network games
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM action.fact_games;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.action_network_games (
            id_action, id_mlbstatsapi, dim_home_team_actionid, dim_away_team_actionid,
            game_date, game_time, game_datetime, status, created_at, updated_at
        )
        SELECT 
            id_action,
            id_mlbstatsapi,
            dim_home_team_actionid,
            dim_away_team_actionid,
            dim_date as game_date,
            dim_time as game_time,
            (dim_date + dim_time) as game_datetime,
            COALESCE(status, 'active') as status,
            NOW() as created_at,
            NOW() as updated_at
        FROM action.fact_games
        ON CONFLICT (id_action) DO UPDATE SET
            id_mlbstatsapi = EXCLUDED.id_mlbstatsapi,
            dim_home_team_actionid = EXCLUDED.dim_home_team_actionid,
            dim_away_team_actionid = EXCLUDED.dim_away_team_actionid,
            game_date = EXCLUDED.game_date,
            game_time = EXCLUDED.game_time,
            game_datetime = EXCLUDED.game_datetime,
            status = EXCLUDED.status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('action_fact_games', 'action.fact_games', 'core_betting.action_network_games', record_count);
        RAISE NOTICE 'Migrated % records from action.fact_games to core_betting.action_network_games', record_count;
    END IF;
END $$;

-- Migrate enhanced Action Network games with team details
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM action.games_with_teams;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.action_network_games_enhanced (
            id_action, id_mlbstatsapi, game_date, game_time, game_datetime,
            home_team_name, away_team_name, home_team_short, away_team_short,
            home_team_actionid, away_team_actionid, venue_name, venue_city, venue_state,
            weather_conditions, temperature, created_at, updated_at
        )
        SELECT 
            id_action,
            id_mlbstatsapi,
            dim_date as game_date,
            dim_time as game_time,
            (dim_date + dim_time) as game_datetime,
            home_team_full_name as home_team_name,
            away_team_full_name as away_team_name,
            home_team_short_name as home_team_short,
            away_team_short_name as away_team_short,
            home_team_actionid,
            away_team_actionid,
            venue_name,
            venue_city,
            venue_state,
            weather_conditions,
            temperature,
            NOW() as created_at,
            NOW() as updated_at
        FROM action.games_with_teams
        ON CONFLICT (id_action) DO UPDATE SET
            id_mlbstatsapi = EXCLUDED.id_mlbstatsapi,
            game_date = EXCLUDED.game_date,
            game_time = EXCLUDED.game_time,
            game_datetime = EXCLUDED.game_datetime,
            home_team_name = EXCLUDED.home_team_name,
            away_team_name = EXCLUDED.away_team_name,
            home_team_short = EXCLUDED.home_team_short,
            away_team_short = EXCLUDED.away_team_short,
            home_team_actionid = EXCLUDED.home_team_actionid,
            away_team_actionid = EXCLUDED.away_team_actionid,
            venue_name = EXCLUDED.venue_name,
            venue_city = EXCLUDED.venue_city,
            venue_state = EXCLUDED.venue_state,
            weather_conditions = EXCLUDED.weather_conditions,
            temperature = EXCLUDED.temperature,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('action_games_with_teams', 'action.games_with_teams', 'core_betting.action_network_games_enhanced', record_count);
        RAISE NOTICE 'Migrated % records from action.games_with_teams to core_betting.action_network_games_enhanced', record_count;
    END IF;
END $$;

-- Enhance core_betting.teams with Action Network team data
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    -- Add Action Network specific columns to teams table if they don't exist
    ALTER TABLE core_betting.teams 
    ADD COLUMN IF NOT EXISTS action_network_id INTEGER,
    ADD COLUMN IF NOT EXISTS action_network_data JSONB;
    
    -- Update existing teams with Action Network data
    UPDATE core_betting.teams 
    SET 
        action_network_id = action.dim_teams.team_id,
        action_network_data = jsonb_build_object(
            'full_name', action.dim_teams.full_name,
            'display_name', action.dim_teams.display_name,
            'short_name', action.dim_teams.short_name,
            'location', action.dim_teams.location,
            'nickname', action.dim_teams.nickname
        )
    FROM action.dim_teams
    WHERE core_betting.teams.team_name = action.dim_teams.full_name 
       OR core_betting.teams.team_abbreviation = action.dim_teams.short_name;
    
    GET DIAGNOSTICS record_count = ROW_COUNT;
    PERFORM log_phase4b_migration('action_team_enhancement', 'action.dim_teams', 'core_betting.teams', record_count, 'completed', NULL, 'Enhanced existing teams with Action Network data');
    RAISE NOTICE 'Enhanced % teams with Action Network data', record_count;
END $$;

-- ==================================================================================
-- MIGRATE SPLITS SCHEMA DATA
-- ==================================================================================

-- Migrate supplementary games data
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM splits.games;
    
    IF record_count > 0 THEN
        INSERT INTO core_betting.supplementary_games (
            id, game_id, home_team, away_team, game_datetime, source,
            status, home_pitcher, away_pitcher, venue, created_at, updated_at
        )
        SELECT 
            id,
            game_id,
            home_team,
            away_team,
            game_datetime,
            'splits' as source,
            COALESCE(status, 'active') as status,
            home_pitcher,
            away_pitcher,
            venue,
            NOW() as created_at,
            NOW() as updated_at
        FROM splits.games
        ON CONFLICT (id) DO UPDATE SET
            game_id = EXCLUDED.game_id,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            game_datetime = EXCLUDED.game_datetime,
            source = EXCLUDED.source,
            status = EXCLUDED.status,
            home_pitcher = EXCLUDED.home_pitcher,
            away_pitcher = EXCLUDED.away_pitcher,
            venue = EXCLUDED.venue,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('splits_games', 'splits.games', 'core_betting.supplementary_games', record_count);
        RAISE NOTICE 'Migrated % records from splits.games to core_betting.supplementary_games', record_count;
    END IF;
END $$;

-- Migrate sharp action indicators
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM splits.sharp_actions;
    
    IF record_count > 0 THEN
        INSERT INTO analytics.sharp_action_indicators (
            id, game_id, split_type, direction, overall_confidence,
            signal_strength, detected_at, source, created_at, updated_at
        )
        SELECT 
            id,
            game_id,
            split_type,
            direction,
            overall_confidence,
            NULL as signal_strength, -- Not available in source
            NOW() as detected_at,
            'splits_analysis' as source,
            NOW() as created_at,
            NOW() as updated_at
        FROM splits.sharp_actions
        ON CONFLICT (id) DO UPDATE SET
            game_id = EXCLUDED.game_id,
            split_type = EXCLUDED.split_type,
            direction = EXCLUDED.direction,
            overall_confidence = EXCLUDED.overall_confidence,
            detected_at = EXCLUDED.detected_at,
            source = EXCLUDED.source,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('splits_sharp_actions', 'splits.sharp_actions', 'analytics.sharp_action_indicators', record_count);
        RAISE NOTICE 'Migrated % records from splits.sharp_actions to analytics.sharp_action_indicators', record_count;
    ELSE
        PERFORM log_phase4b_migration('splits_sharp_actions', 'splits.sharp_actions', 'analytics.sharp_action_indicators', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table splits.sharp_actions is empty, skipping migration';
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE TRACKING SCHEMA DATA
-- ==================================================================================

-- Migrate active high ROI strategies
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM tracking.active_high_roi_strategies;
    
    IF record_count > 0 THEN
        INSERT INTO operational.active_strategies (
            strategy_id, source_book_type, split_type, strategy_variant,
            total_bets, roi_per_100_unit, win_rate, confidence_level,
            last_updated, status, created_at, updated_at
        )
        SELECT 
            strategy_id,
            source_book_type,
            split_type,
            strategy_variant,
            total_bets,
            roi_per_100_unit,
            win_rate,
            confidence_level,
            last_updated,
            'active' as status,
            NOW() as created_at,
            NOW() as updated_at
        FROM tracking.active_high_roi_strategies
        ON CONFLICT (strategy_id) DO UPDATE SET
            source_book_type = EXCLUDED.source_book_type,
            split_type = EXCLUDED.split_type,
            strategy_variant = EXCLUDED.strategy_variant,
            total_bets = EXCLUDED.total_bets,
            roi_per_100_unit = EXCLUDED.roi_per_100_unit,
            win_rate = EXCLUDED.win_rate,
            confidence_level = EXCLUDED.confidence_level,
            last_updated = EXCLUDED.last_updated,
            status = EXCLUDED.status,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('tracking_active_strategies', 'tracking.active_high_roi_strategies', 'operational.active_strategies', record_count);
        RAISE NOTICE 'Migrated % records from tracking.active_high_roi_strategies to operational.active_strategies', record_count;
    END IF;
END $$;

-- Migrate strategy configurations
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM tracking.active_strategy_configs;
    
    IF record_count > 0 THEN
        INSERT INTO operational.strategy_configurations (
            strategy_id, strategy_name, configuration, enabled, created_at, updated_at
        )
        SELECT 
            strategy_id,
            strategy_id as strategy_name, -- Use strategy_id as name if no separate name field
            configuration,
            COALESCE(enabled, true) as enabled,
            COALESCE(created_at, NOW()) as created_at,
            COALESCE(updated_at, NOW()) as updated_at
        FROM tracking.active_strategy_configs
        ON CONFLICT (strategy_id) DO UPDATE SET
            strategy_name = EXCLUDED.strategy_name,
            configuration = EXCLUDED.configuration,
            enabled = EXCLUDED.enabled,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('tracking_strategy_configs', 'tracking.active_strategy_configs', 'operational.strategy_configurations', record_count);
        RAISE NOTICE 'Migrated % records from tracking.active_strategy_configs to operational.strategy_configurations', record_count;
    END IF;
END $$;

-- Migrate strategy integration log
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM tracking.strategy_integration_log;
    
    IF record_count > 0 THEN
        INSERT INTO operational.strategy_integration_log (
            id, strategy_id, action, roi_per_100_unit, total_bets, win_rate, timestamp, notes, created_at
        )
        SELECT 
            id,
            strategy_id,
            action,
            roi_per_100_unit,
            total_bets,
            win_rate,
            timestamp,
            notes,
            NOW() as created_at
        FROM tracking.strategy_integration_log
        ON CONFLICT (id) DO UPDATE SET
            strategy_id = EXCLUDED.strategy_id,
            action = EXCLUDED.action,
            roi_per_100_unit = EXCLUDED.roi_per_100_unit,
            total_bets = EXCLUDED.total_bets,
            win_rate = EXCLUDED.win_rate,
            timestamp = EXCLUDED.timestamp,
            notes = EXCLUDED.notes;
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('tracking_integration_log', 'tracking.strategy_integration_log', 'operational.strategy_integration_log', record_count);
        RAISE NOTICE 'Migrated % records from tracking.strategy_integration_log to operational.strategy_integration_log', record_count;
    END IF;
END $$;

-- Migrate pre-game recommendations
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM tracking.pre_game_recommendations;
    
    IF record_count > 0 THEN
        INSERT INTO operational.pre_game_recommendations (
            recommendation_id, game_pk, home_team, away_team, game_datetime,
            bet_type, recommendation, confidence_level, roi_projection, strategy_id, created_at, updated_at
        )
        SELECT 
            recommendation_id,
            game_pk,
            home_team,
            away_team,
            game_datetime,
            bet_type,
            recommendation,
            confidence_level,
            roi_projection,
            strategy_id,
            COALESCE(created_at, NOW()) as created_at,
            COALESCE(updated_at, NOW()) as updated_at
        FROM tracking.pre_game_recommendations
        ON CONFLICT (recommendation_id) DO UPDATE SET
            game_pk = EXCLUDED.game_pk,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            game_datetime = EXCLUDED.game_datetime,
            bet_type = EXCLUDED.bet_type,
            recommendation = EXCLUDED.recommendation,
            confidence_level = EXCLUDED.confidence_level,
            roi_projection = EXCLUDED.roi_projection,
            strategy_id = EXCLUDED.strategy_id,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('tracking_pre_game_recommendations', 'tracking.pre_game_recommendations', 'operational.pre_game_recommendations', record_count);
        RAISE NOTICE 'Migrated % records from tracking.pre_game_recommendations to operational.pre_game_recommendations', record_count;
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE VALIDATION SCHEMA DATA
-- ==================================================================================

-- Migrate strategy validation records
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM validation.strategy_records;
    
    IF record_count > 0 THEN
        INSERT INTO operational.strategy_validation_records (
            id, strategy_name, validation_date, roi_per_100, win_rate, total_bets, validation_status, created_at
        )
        SELECT 
            id,
            strategy_name,
            validation_date,
            roi_per_100,
            win_rate,
            total_bets,
            COALESCE(validation_status, 'pending') as validation_status,
            NOW() as created_at
        FROM validation.strategy_records
        ON CONFLICT (id) DO UPDATE SET
            strategy_name = EXCLUDED.strategy_name,
            validation_date = EXCLUDED.validation_date,
            roi_per_100 = EXCLUDED.roi_per_100,
            win_rate = EXCLUDED.win_rate,
            total_bets = EXCLUDED.total_bets,
            validation_status = EXCLUDED.validation_status;
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('validation_strategy_records', 'validation.strategy_records', 'operational.strategy_validation_records', record_count);
        RAISE NOTICE 'Migrated % records from validation.strategy_records to operational.strategy_validation_records', record_count;
    ELSE
        PERFORM log_phase4b_migration('validation_strategy_records', 'validation.strategy_records', 'operational.strategy_validation_records', 0, 'completed', NULL, 'Source table empty');
        RAISE NOTICE 'Source table validation.strategy_records is empty, skipping migration';
    END IF;
END $$;

-- ==================================================================================
-- MIGRATE BACKTESTING SCHEMA DATA
-- ==================================================================================

-- Note: Many backtesting tables already exist in operational schema from previous migrations
-- We'll focus on consolidating data from the remaining backtesting tables

-- Migrate threshold configurations
DO $$
DECLARE
    record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO record_count FROM backtesting.threshold_configurations;
    
    IF record_count > 0 THEN
        INSERT INTO operational.threshold_configurations (
            id, source, strategy_type, high_confidence_threshold, moderate_confidence_threshold,
            low_confidence_threshold, roi_threshold, win_rate_threshold, created_at, updated_at
        )
        SELECT 
            id,
            source,
            strategy_type,
            high_confidence_threshold,
            moderate_confidence_threshold,
            low_confidence_threshold,
            roi_threshold,
            win_rate_threshold,
            COALESCE(created_at, NOW()) as created_at,
            COALESCE(updated_at, NOW()) as updated_at
        FROM backtesting.threshold_configurations
        ON CONFLICT (id) DO UPDATE SET
            source = EXCLUDED.source,
            strategy_type = EXCLUDED.strategy_type,
            high_confidence_threshold = EXCLUDED.high_confidence_threshold,
            moderate_confidence_threshold = EXCLUDED.moderate_confidence_threshold,
            low_confidence_threshold = EXCLUDED.low_confidence_threshold,
            roi_threshold = EXCLUDED.roi_threshold,
            win_rate_threshold = EXCLUDED.win_rate_threshold,
            updated_at = NOW();
            
        GET DIAGNOSTICS record_count = ROW_COUNT;
        PERFORM log_phase4b_migration('backtesting_threshold_configs', 'backtesting.threshold_configurations', 'operational.threshold_configurations', record_count);
        RAISE NOTICE 'Migrated % records from backtesting.threshold_configurations to operational.threshold_configurations', record_count;
    END IF;
END $$;

-- ==================================================================================
-- MIGRATION COMPLETION SUMMARY
-- ==================================================================================

DO $$ 
DECLARE
    total_migrated INTEGER;
    migration_summary TEXT;
BEGIN
    -- Calculate total migrated records
    SELECT SUM(records_migrated) INTO total_migrated 
    FROM operational.phase4b_migration_log 
    WHERE status = 'completed';
    
    -- Generate summary
    SELECT string_agg(
        migration_step || ': ' || records_migrated || ' records', 
        E'\n'
    ) INTO migration_summary
    FROM operational.phase4b_migration_log 
    WHERE status = 'completed'
    ORDER BY id;
    
    RAISE NOTICE 'Phase 4B Data Migration completed successfully!';
    RAISE NOTICE 'Total records migrated: %', COALESCE(total_migrated, 0);
    RAISE NOTICE 'Migration summary:';
    RAISE NOTICE '%', migration_summary;
    RAISE NOTICE 'Completion timestamp: %', NOW();
END $$; 