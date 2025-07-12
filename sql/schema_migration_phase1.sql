-- MLB Sharp Betting System - Schema Consolidation Migration Phase 1
-- This script migrates data from the old 9+ schema structure to the new 4-schema structure
-- 
-- Migration Strategy:
-- Phase 1: Create new schemas and migrate core data (NON-DESTRUCTIVE)
-- Phase 2: Update application code to use new schemas  
-- Phase 3: Drop old schemas after verification
--
-- IMPORTANT: This script is NON-DESTRUCTIVE - it creates new tables and copies data
-- The old tables remain untouched until Phase 3

-- ==============================================================================
-- PRE-MIGRATION CHECKS AND SETUP
-- ==============================================================================

-- Create a migration log table
CREATE TABLE IF NOT EXISTS migration_log (
    id SERIAL PRIMARY KEY,
    migration_phase VARCHAR(20) NOT NULL,
    table_source VARCHAR(100) NOT NULL,
    table_destination VARCHAR(100) NOT NULL,
    records_migrated INTEGER DEFAULT 0,
    migration_started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    migration_completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started' CHECK (status IN ('started', 'completed', 'failed')),
    error_message TEXT,
    notes TEXT
);

-- Function to log migration progress
CREATE OR REPLACE FUNCTION log_migration(
    p_phase VARCHAR(20),
    p_source VARCHAR(100),
    p_destination VARCHAR(100),
    p_records INTEGER DEFAULT 0,
    p_status VARCHAR(20) DEFAULT 'completed',
    p_error TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO migration_log (
        migration_phase, table_source, table_destination, 
        records_migrated, migration_completed_at, status, error_message, notes
    ) VALUES (
        p_phase, p_source, p_destination, 
        p_records, NOW(), p_status, p_error, p_notes
    );
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- STEP 1: CREATE NEW CONSOLIDATED SCHEMAS
-- ==============================================================================

DO $$
BEGIN
    -- Source the consolidated schema (this should be run first)
    RAISE NOTICE 'Creating consolidated schemas - run sql/consolidated_schema.sql first';
    PERFORM log_migration('Phase1', 'N/A', 'consolidated_schemas', 0, 'completed', NULL, 'Schema creation step');
END $$;

-- ==============================================================================
-- STEP 2: MIGRATE RAW_DATA SCHEMA
-- ==============================================================================

-- Migrate raw HTML from public schema to raw_data
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    -- Check if source table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sbr_raw_html') THEN
        INSERT INTO raw_data.sbr_raw_html (
            id, url, response_html, response_headers, scrape_timestamp,
            status_code, page_type, date_scraped, processing_status, 
            error_message, created_at
        )
        SELECT 
            id, url, response_html, response_headers, scrape_timestamp,
            status_code, page_type, date_scraped, processing_status,
            error_message, created_at
        FROM public.sbr_raw_html
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', row_count);
        RAISE NOTICE 'Migrated % rows from public.sbr_raw_html to raw_data.sbr_raw_html', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'public.sbr_raw_html', 'raw_data.sbr_raw_html', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate parsed games from public schema to raw_data
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sbr_parsed_games') THEN
        INSERT INTO raw_data.sbr_parsed_games (
            id, raw_html_id, game_date, home_team, away_team, game_time,
            parsed_data, parsing_timestamp, data_quality, validation_errors, created_at
        )
        SELECT 
            id, raw_html_id, game_date, home_team, away_team, game_time,
            parsed_data, parsing_timestamp, data_quality, validation_errors, created_at
        FROM public.sbr_parsed_games
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', row_count);
        RAISE NOTICE 'Migrated % rows from public.sbr_parsed_games to raw_data.sbr_parsed_games', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'public.sbr_parsed_games', 'raw_data.sbr_parsed_games', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate raw MLB betting splits from splits schema to raw_data
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'splits' AND table_name = 'raw_mlb_betting_splits') THEN
        INSERT INTO raw_data.raw_mlb_betting_splits (
            id, game_id, home_team, away_team, game_datetime, split_type,
            last_updated, source, book, home_or_over_bets, home_or_over_bets_percentage,
            home_or_over_stake_percentage, away_or_under_bets, away_or_under_bets_percentage,
            away_or_under_stake_percentage, split_value, sharp_action, outcome,
            created_at, updated_at
        )
        SELECT 
            id, game_id, home_team, away_team, game_datetime, split_type,
            last_updated, source, book, home_or_over_bets, home_or_over_bets_percentage,
            home_or_over_stake_percentage, away_or_under_bets, away_or_under_bets_percentage,
            away_or_under_stake_percentage, split_value, sharp_action, outcome,
            created_at, updated_at
        FROM splits.raw_mlb_betting_splits
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'splits.raw_mlb_betting_splits', 'raw_data.raw_mlb_betting_splits', row_count);
        RAISE NOTICE 'Migrated % rows from splits.raw_mlb_betting_splits to raw_data.raw_mlb_betting_splits', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'splits.raw_mlb_betting_splits', 'raw_data.raw_mlb_betting_splits', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- ==============================================================================
-- STEP 3: MIGRATE CORE_BETTING SCHEMA
-- ==============================================================================

-- Migrate games from public/splits to core_betting (unified games table)
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    -- First migrate from public.games if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'games') THEN
        INSERT INTO core_betting.games (
            id, sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id,
            home_team, away_team, game_date, game_datetime, game_status,
            home_score, away_score, winning_team, venue_name, venue_id,
            season, season_type, game_type, weather_condition, temperature,
            wind_speed, wind_direction, humidity, data_quality,
            mlb_correlation_confidence, has_mlb_enrichment, created_at, updated_at
        )
        SELECT 
            id, sportsbookreview_game_id, mlb_stats_api_game_id, action_network_game_id,
            home_team, away_team, game_date, game_datetime, game_status,
            home_score, away_score, winning_team, venue_name, venue_id,
            season, season_type, game_type, weather_condition, temperature,
            wind_speed, wind_direction, humidity, data_quality,
            mlb_correlation_confidence, has_mlb_enrichment, created_at, updated_at
        FROM public.games
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'public.games', 'core_betting.games', row_count);
        RAISE NOTICE 'Migrated % rows from public.games to core_betting.games', row_count;
    END IF;

    -- Then migrate from splits.games if it exists and has additional data
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'splits' AND table_name = 'games') THEN
        INSERT INTO core_betting.games (
            home_team, away_team, game_date, game_datetime, created_at, updated_at
        )
        SELECT DISTINCT
            home_team, away_team, game_date::DATE, 
            COALESCE(game_datetime, game_date::TIMESTAMP WITH TIME ZONE),
            created_at, updated_at
        FROM splits.games sg
        WHERE NOT EXISTS (
            SELECT 1 FROM core_betting.games cbg 
            WHERE cbg.home_team = sg.home_team 
            AND cbg.away_team = sg.away_team 
            AND cbg.game_date = sg.game_date::DATE
        );
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'splits.games', 'core_betting.games', row_count, 'completed', NULL, 'Additional games from splits schema');
        RAISE NOTICE 'Migrated % additional rows from splits.games to core_betting.games', row_count;
    END IF;
END $$;

-- Migrate game outcomes from public to core_betting
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'game_outcomes') THEN
        INSERT INTO core_betting.game_outcomes (
            game_id, home_team, away_team, home_score, away_score,
            home_win, over, home_cover_spread, total_line, home_spread_line,
            game_date, created_at, updated_at
        )
        SELECT 
            -- Try to map game_id to new core_betting.games table
            COALESCE(cbg.id, go.id::INTEGER) as game_id,
            go.home_team, go.away_team, go.home_score, go.away_score,
            go.home_win, go.over, go.home_cover_spread, go.total_line, go.home_spread_line,
            go.game_date, go.created_at, go.updated_at
        FROM public.game_outcomes go
        LEFT JOIN core_betting.games cbg ON (
            cbg.home_team = go.home_team 
            AND cbg.away_team = go.away_team 
            AND cbg.game_date = DATE(go.game_date)
        )
        ON CONFLICT (game_id) DO UPDATE SET
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_win = EXCLUDED.home_win,
            over = EXCLUDED.over,
            home_cover_spread = EXCLUDED.home_cover_spread,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'public.game_outcomes', 'core_betting.game_outcomes', row_count);
        RAISE NOTICE 'Migrated % rows from public.game_outcomes to core_betting.game_outcomes', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'public.game_outcomes', 'core_betting.game_outcomes', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate teams from action.dim_teams to core_betting.teams
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'action' AND table_name = 'dim_teams') THEN
        INSERT INTO core_betting.teams (
            team_id, full_name, display_name, short_name, location, abbr,
            logo, primary_color, secondary_color, conference_type, division_type,
            url_slug, created_at, updated_at
        )
        SELECT 
            team_id, full_name, display_name, short_name, location, abbr,
            logo, primary_color, secondary_color, conference_type, division_type,
            url_slug, created_at, updated_at
        FROM action.dim_teams
        ON CONFLICT (team_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            display_name = EXCLUDED.display_name,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'action.dim_teams', 'core_betting.teams', row_count);
        RAISE NOTICE 'Migrated % rows from action.dim_teams to core_betting.teams', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'action.dim_teams', 'core_betting.teams', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate betting lines from mlb_betting schema to core_betting
-- Moneyline
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'mlb_betting' AND table_name = 'moneyline') THEN
        INSERT INTO core_betting.betting_lines_moneyline (
            game_id, sportsbook, home_ml, away_ml, odds_timestamp,
            opening_home_ml, opening_away_ml, closing_home_ml, closing_away_ml,
            home_bets_count, away_bets_count, home_bets_percentage, away_bets_percentage,
            home_money_percentage, away_money_percentage, sharp_action,
            reverse_line_movement, steam_move, winning_side, profit_loss,
            source, data_quality, created_at, updated_at
        )
        SELECT 
            game_id, sportsbook, home_ml, away_ml, odds_timestamp,
            opening_home_ml, opening_away_ml, closing_home_ml, closing_away_ml,
            home_bets_count, away_bets_count, home_bets_percentage, away_bets_percentage,
            home_money_percentage, away_money_percentage, sharp_action,
            reverse_line_movement, steam_move, winning_side, profit_loss,
            source, data_quality, created_at, updated_at
        FROM mlb_betting.moneyline;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'mlb_betting.moneyline', 'core_betting.betting_lines_moneyline', row_count);
        RAISE NOTICE 'Migrated % rows from mlb_betting.moneyline to core_betting.betting_lines_moneyline', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'mlb_betting.moneyline', 'core_betting.betting_lines_moneyline', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Spreads
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'mlb_betting' AND table_name = 'spreads') THEN
        INSERT INTO core_betting.betting_lines_spreads (
            game_id, sportsbook, home_spread, away_spread, home_spread_price, away_spread_price,
            odds_timestamp, opening_home_spread, opening_away_spread, opening_home_spread_price,
            opening_away_spread_price, closing_home_spread, closing_away_spread,
            closing_home_spread_price, closing_away_spread_price, home_bets_count,
            away_bets_count, home_bets_percentage, away_bets_percentage,
            home_money_percentage, away_money_percentage, sharp_action,
            reverse_line_movement, steam_move, winning_side, profit_loss,
            home_cover, margin_of_victory, source, data_quality, created_at, updated_at
        )
        SELECT 
            game_id, sportsbook, home_spread, away_spread, home_spread_price, away_spread_price,
            odds_timestamp, opening_home_spread, opening_away_spread, opening_home_spread_price,
            opening_away_spread_price, closing_home_spread, closing_away_spread,
            closing_home_spread_price, closing_away_spread_price, home_bets_count,
            away_bets_count, home_bets_percentage, away_bets_percentage,
            home_money_percentage, away_money_percentage, sharp_action,
            reverse_line_movement, steam_move, winning_side, profit_loss,
            home_cover, margin_of_victory, source, data_quality, created_at, updated_at
        FROM mlb_betting.spreads;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'mlb_betting.spreads', 'core_betting.betting_lines_spreads', row_count);
        RAISE NOTICE 'Migrated % rows from mlb_betting.spreads to core_betting.betting_lines_spreads', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'mlb_betting.spreads', 'core_betting.betting_lines_spreads', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Totals
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'mlb_betting' AND table_name = 'totals') THEN
        INSERT INTO core_betting.betting_lines_totals (
            game_id, sportsbook, total_line, over_price, under_price, odds_timestamp,
            opening_total, opening_over_price, opening_under_price, closing_total,
            closing_over_price, closing_under_price, over_bets_count, under_bets_count,
            over_bets_percentage, under_bets_percentage, over_money_percentage,
            under_money_percentage, sharp_action, reverse_line_movement, steam_move,
            winning_side, profit_loss, total_score, source, data_quality, created_at, updated_at
        )
        SELECT 
            game_id, sportsbook, total_line, over_price, under_price, odds_timestamp,
            opening_total, opening_over_price, opening_under_price, closing_total,
            closing_over_price, closing_under_price, over_bets_count, under_bets_count,
            over_bets_percentage, under_bets_percentage, over_money_percentage,
            under_money_percentage, sharp_action, reverse_line_movement, steam_move,
            winning_side, profit_loss, total_score, source, data_quality, created_at, updated_at
        FROM mlb_betting.totals;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'mlb_betting.totals', 'core_betting.betting_lines_totals', row_count);
        RAISE NOTICE 'Migrated % rows from mlb_betting.totals to core_betting.betting_lines_totals', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'mlb_betting.totals', 'core_betting.betting_lines_totals', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- ==============================================================================
-- STEP 4: MIGRATE ANALYTICS SCHEMA
-- ==============================================================================

-- Migrate timing analysis results from timing_analysis schema to analytics
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'timing_analysis' AND table_name = 'timing_bucket_performance') THEN
        INSERT INTO analytics.timing_analysis_results (
            analysis_name, timing_bucket, source, book, split_type, strategy_name,
            analysis_start_date, analysis_end_date, total_bets, wins, losses, pushes,
            win_rate, roi_percentage, total_profit_loss, confidence_level,
            sample_size_adequate, created_at, updated_at
        )
        SELECT 
            COALESCE('Historical Analysis ' || id::TEXT, 'Legacy Analysis') as analysis_name,
            timing_bucket, source, book, split_type, strategy_name,
            analysis_start_date, analysis_end_date, total_bets, wins, losses, pushes,
            win_rate, roi_percentage, total_profit_loss, confidence_level,
            CASE WHEN total_bets >= 20 THEN true ELSE false END as sample_size_adequate,
            created_at, updated_at
        FROM timing_analysis.timing_bucket_performance;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'timing_analysis.timing_bucket_performance', 'analytics.timing_analysis_results', row_count);
        RAISE NOTICE 'Migrated % rows from timing_analysis.timing_bucket_performance to analytics.timing_analysis_results', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'timing_analysis.timing_bucket_performance', 'analytics.timing_analysis_results', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate betting recommendations from clean schema to analytics
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'clean' AND table_name = 'betting_recommendations') THEN
        INSERT INTO analytics.betting_recommendations (
            id, game_id, home_team, away_team, game_datetime, market_type,
            source, book, recommended_side, line_value, confidence_score,
            differential, stake_percentage, bet_percentage, minutes_before_game,
            signal_strength, consensus_boost, last_updated, created_at
        )
        SELECT 
            id, 
            -- Try to map game_id from string to integer
            CASE 
                WHEN game_id ~ '^\d+$' THEN game_id::INTEGER
                ELSE (SELECT cbg.id FROM core_betting.games cbg 
                      WHERE cbg.home_team = cr.home_team 
                      AND cbg.away_team = cr.away_team 
                      AND cbg.game_date = DATE(cr.game_datetime)
                      LIMIT 1)
            END as game_id,
            home_team, away_team, game_datetime, market_type,
            source, book, recommended_side, line_value, confidence_score,
            differential, stake_percentage, bet_percentage, minutes_before_game,
            signal_strength, consensus_boost, last_updated, created_at
        FROM clean.betting_recommendations cr
        WHERE EXISTS (
            SELECT 1 FROM core_betting.games cbg 
            WHERE cbg.home_team = cr.home_team 
            AND cbg.away_team = cr.away_team 
            AND cbg.game_date = DATE(cr.game_datetime)
        );
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'clean.betting_recommendations', 'analytics.betting_recommendations', row_count);
        RAISE NOTICE 'Migrated % rows from clean.betting_recommendations to analytics.betting_recommendations', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'clean.betting_recommendations', 'analytics.betting_recommendations', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- ==============================================================================
-- STEP 5: MIGRATE OPERATIONAL SCHEMA
-- ==============================================================================

-- Migrate strategy performance from backtesting schema to operational
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtesting' AND table_name = 'strategy_performance') THEN
        INSERT INTO operational.strategy_performance (
            id, backtest_date, strategy_name, source_book_type, split_type,
            total_bets, wins, win_rate, roi_per_100, sharpe_ratio, max_drawdown,
            confidence_interval_lower, confidence_interval_upper, sample_size_adequate,
            statistical_significance, p_value, seven_day_win_rate, thirty_day_win_rate,
            trend_direction, consecutive_losses, volatility, kelly_criterion,
            created_at, updated_at
        )
        SELECT 
            id, backtest_date, strategy_name, source_book_type, split_type,
            total_bets, wins, win_rate, roi_per_100, sharpe_ratio, max_drawdown,
            confidence_interval_lower, confidence_interval_upper, sample_size_adequate,
            statistical_significance, p_value, seven_day_win_rate, thirty_day_win_rate,
            trend_direction, consecutive_losses, volatility, kelly_criterion,
            created_at, updated_at
        FROM backtesting.strategy_performance
        ON CONFLICT (id) DO UPDATE SET
            total_bets = EXCLUDED.total_bets,
            wins = EXCLUDED.wins,
            win_rate = EXCLUDED.win_rate,
            roi_per_100 = EXCLUDED.roi_per_100,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'backtesting.strategy_performance', 'operational.strategy_performance', row_count);
        RAISE NOTICE 'Migrated % rows from backtesting.strategy_performance to operational.strategy_performance', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'backtesting.strategy_performance', 'operational.strategy_performance', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate pre-game recommendations from tracking schema to operational
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'tracking' AND table_name = 'pre_game_recommendations') THEN
        INSERT INTO operational.pre_game_recommendations (
            recommendation_id, game_pk, home_team, away_team, game_datetime,
            recommendation, bet_type, confidence_level, signal_source, signal_strength,
            recommended_at, email_sent, game_completed, bet_won, actual_outcome,
            profit_loss, created_at, updated_at, strategy_source_id, auto_integrated
        )
        SELECT 
            recommendation_id, game_pk, home_team, away_team, game_datetime,
            recommendation, bet_type, confidence_level, signal_source, signal_strength,
            recommended_at, email_sent, game_completed, bet_won, actual_outcome,
            profit_loss, created_at, updated_at, strategy_source_id, auto_integrated
        FROM tracking.pre_game_recommendations
        ON CONFLICT (recommendation_id) DO UPDATE SET
            game_completed = EXCLUDED.game_completed,
            bet_won = EXCLUDED.bet_won,
            actual_outcome = EXCLUDED.actual_outcome,
            profit_loss = EXCLUDED.profit_loss,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'tracking.pre_game_recommendations', 'operational.pre_game_recommendations', row_count);
        RAISE NOTICE 'Migrated % rows from tracking.pre_game_recommendations to operational.pre_game_recommendations', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'tracking.pre_game_recommendations', 'operational.pre_game_recommendations', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- Migrate orchestrator update triggers from backtesting schema to operational
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtesting' AND table_name = 'orchestrator_update_triggers') THEN
        INSERT INTO operational.orchestrator_update_triggers (
            trigger_type, strategy_name, trigger_data, triggered_at,
            processed_at, status, created_at
        )
        SELECT 
            'legacy_migration' as trigger_type, -- Convert old structure
            COALESCE(strategy_name, 'unknown') as strategy_name,
            '{}' as trigger_data, -- Old table might not have this field
            triggered_at, processed_at, status, created_at
        FROM backtesting.orchestrator_update_triggers;
        
        GET DIAGNOSTICS row_count = ROW_COUNT;
        PERFORM log_migration('Phase1', 'backtesting.orchestrator_update_triggers', 'operational.orchestrator_update_triggers', row_count);
        RAISE NOTICE 'Migrated % rows from backtesting.orchestrator_update_triggers to operational.orchestrator_update_triggers', row_count;
    ELSE
        PERFORM log_migration('Phase1', 'backtesting.orchestrator_update_triggers', 'operational.orchestrator_update_triggers', 0, 'completed', NULL, 'Source table does not exist');
    END IF;
END $$;

-- ==============================================================================
-- POST-MIGRATION VALIDATION AND REPORTING
-- ==============================================================================

-- Create a validation report
DO $$
DECLARE
    total_migrated INTEGER;
    tables_migrated INTEGER;
    failed_migrations INTEGER;
BEGIN
    SELECT 
        SUM(records_migrated),
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'failed')
    INTO total_migrated, tables_migrated, failed_migrations
    FROM migration_log 
    WHERE migration_phase = 'Phase1';
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'MIGRATION PHASE 1 COMPLETED';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables processed: %', tables_migrated;
    RAISE NOTICE 'Total records migrated: %', total_migrated;
    RAISE NOTICE 'Failed migrations: %', failed_migrations;
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Review migration_log table for any errors';
    RAISE NOTICE '2. Update application code to use new schemas';
    RAISE NOTICE '3. Run Phase 2 migration script to update table registry';
    RAISE NOTICE '4. Test application functionality';
    RAISE NOTICE '5. Run Phase 3 to drop old schemas (DESTRUCTIVE)';
    RAISE NOTICE '';
    RAISE NOTICE 'To view migration details:';
    RAISE NOTICE 'SELECT * FROM migration_log WHERE migration_phase = ''Phase1'' ORDER BY id;';
    RAISE NOTICE '========================================';
END $$; 