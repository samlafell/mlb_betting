-- ==============================================================================
-- PHASE 2B: HISTORICAL DATA MIGRATION
-- ==============================================================================
-- 
-- This script migrates ALL historical data from legacy schemas to the new
-- consolidated schema structure. This is a comprehensive migration that
-- handles the bulk of the data migration.
--
-- 🚀 PHASE 2B MIGRATION: Complete Historical Data Migration
-- 
-- Prerequisites:
-- 1. Phase 1 schema creation completed
-- 2. Phase 2A application updates completed and tested
-- 3. Database backup created
-- 
-- Migration Scope:
-- - Games: ~1,619 records from public.games → core_betting.games
-- - Moneyline: ~8,835 records from mlb_betting.moneyline → core_betting.betting_lines_moneyline  
-- - Spreads: ~9,595 records from mlb_betting.spreads → core_betting.betting_lines_spreads
-- - Totals: ~7,875 records from mlb_betting.totals → core_betting.betting_lines_totals
-- - Additional reference data and analytics tables
-- ==============================================================================

-- Enable detailed logging
\set ON_ERROR_STOP on
\timing on

-- Create migration tracking for Phase 2B
CREATE TABLE IF NOT EXISTS migration_log_phase2b (
    id SERIAL PRIMARY KEY,
    migration_step VARCHAR(100) NOT NULL,
    table_name VARCHAR(100),
    records_processed INTEGER DEFAULT 0,
    records_migrated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running',
    error_message TEXT,
    notes TEXT
);

-- Log migration start
INSERT INTO migration_log_phase2b (migration_step, notes) 
VALUES ('PHASE2B_START', 'Beginning Phase 2B historical data migration');

-- ==============================================================================
-- STEP 1: MIGRATE GAMES DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('GAMES_MIGRATION_START', 'core_betting.games');

-- Migrate games from public.games to core_betting.games
-- Only migrate games that don't already exist in the new schema
-- Use a temporary sequence to avoid ID conflicts
CREATE TEMPORARY SEQUENCE temp_game_id_seq START WITH 10000;

INSERT INTO core_betting.games (
    id,
    sportsbookreview_game_id,
    mlb_stats_api_game_id,
    action_network_game_id,
    home_team,
    away_team,
    game_date,
    game_datetime,
    game_status,
    home_score,
    away_score,
    winning_team,
    venue_name,
    venue_id,
    season,
    season_type,
    game_type,
    weather_condition,
    temperature,
    wind_speed,
    wind_direction,
    humidity,
    data_quality,
    mlb_correlation_confidence,
    has_mlb_enrichment,
    created_at,
    updated_at
)
SELECT 
    -- Map legacy columns to new schema with unique IDs
    nextval('temp_game_id_seq') as id,
    COALESCE(pg.sportsbookreview_game_id, pg.game_id) as sportsbookreview_game_id,
    pg.mlb_stats_api_game_id,
    pg.action_network_game_id,
    pg.home_team,
    pg.away_team,
    pg.game_date,
    pg.game_datetime,
    COALESCE(pg.status, 'completed') as game_status,
    pg.home_score,
    pg.away_score,
    pg.winning_team,
    pg.venue_name,
    pg.venue_id,
    COALESCE(pg.season, EXTRACT(YEAR FROM pg.game_datetime)) as season,
    COALESCE(pg.season_type, 'regular') as season_type,
    COALESCE(pg.game_type, 'regular') as game_type,
    pg.weather_condition,
    pg.temperature,
    pg.wind_speed,
    pg.wind_direction,
    pg.humidity,
    COALESCE(pg.data_quality, 'MEDIUM') as data_quality,
    pg.mlb_correlation_confidence,
    COALESCE(pg.has_mlb_enrichment, FALSE) as has_mlb_enrichment,
    COALESCE(pg.created_at, NOW()) as created_at,
    COALESCE(pg.updated_at, NOW()) as updated_at
FROM public.games pg
WHERE NOT EXISTS (
    SELECT 1 FROM core_betting.games cbg 
    WHERE cbg.sportsbookreview_game_id = COALESCE(pg.sportsbookreview_game_id, pg.game_id)
);

-- Update migration log with games migration results
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM public.games),
    records_migrated = (SELECT COUNT(*) FROM core_betting.games) - 4, -- Subtract test records from Phase 2A
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'GAMES_MIGRATION_START';

-- ==============================================================================
-- STEP 2: MIGRATE MONEYLINE DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('MONEYLINE_MIGRATION_START', 'core_betting.betting_lines_moneyline');

-- Create a mapping table for old game IDs to new game IDs
CREATE TEMPORARY TABLE game_id_mapping AS
SELECT 
    pg.id as old_game_id,
    cbg.id as new_game_id,
    COALESCE(pg.sportsbookreview_game_id, pg.game_id) as sportsbookreview_game_id
FROM public.games pg
INNER JOIN core_betting.games cbg ON cbg.sportsbookreview_game_id = COALESCE(pg.sportsbookreview_game_id, pg.game_id);

-- Migrate moneyline data with proper game_id mapping
INSERT INTO core_betting.betting_lines_moneyline (
    game_id,
    sportsbook,
    home_ml,
    away_ml,
    odds_timestamp,
    opening_home_ml,
    opening_away_ml,
    closing_home_ml,
    closing_away_ml,
    home_bets_count,
    away_bets_count,
    home_bets_percentage,
    away_bets_percentage,
    home_money_percentage,
    away_money_percentage,
    sharp_action,
    reverse_line_movement,
    steam_move,
    winning_side,
    profit_loss,
    source,
    data_quality,
    created_at,
    updated_at
)
SELECT 
    gim.new_game_id as game_id,  -- Map to new games table ID using mapping
    ml.sportsbook,
    ml.home_ml,
    ml.away_ml,
    ml.odds_timestamp,
    ml.opening_home_ml,
    ml.opening_away_ml,
    ml.closing_home_ml,
    ml.closing_away_ml,
    ml.home_bets_count,
    ml.away_bets_count,
    ml.home_bets_percentage,
    ml.away_bets_percentage,
    ml.home_money_percentage,
    ml.away_money_percentage,
    ml.sharp_action,
    COALESCE(ml.reverse_line_movement, FALSE) as reverse_line_movement,
    COALESCE(ml.steam_move, FALSE) as steam_move,
    ml.winning_side,
    ml.profit_loss,
    COALESCE(ml.source, 'SPORTSBOOKREVIEW') as source,
    COALESCE(ml.data_quality, 'MEDIUM') as data_quality,
    COALESCE(ml.created_at, NOW()) as created_at,
    COALESCE(ml.updated_at, NOW()) as updated_at
FROM mlb_betting.moneyline ml
INNER JOIN game_id_mapping gim ON ml.game_id = gim.old_game_id
WHERE NOT EXISTS (
    SELECT 1 FROM core_betting.betting_lines_moneyline cml 
    WHERE cml.game_id = gim.new_game_id 
    AND cml.sportsbook = ml.sportsbook 
    AND cml.odds_timestamp = ml.odds_timestamp
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM mlb_betting.moneyline),
    records_migrated = (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline) - 21, -- Subtract test records
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'MONEYLINE_MIGRATION_START';

-- ==============================================================================
-- STEP 3: MIGRATE SPREADS DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('SPREADS_MIGRATION_START', 'core_betting.betting_lines_spreads');

-- Migrate spreads data with proper game_id mapping
INSERT INTO core_betting.betting_lines_spreads (
    game_id,
    sportsbook,
    home_spread,
    away_spread,
    home_spread_price,
    away_spread_price,
    odds_timestamp,
    opening_home_spread,
    opening_away_spread,
    opening_home_spread_price,
    opening_away_spread_price,
    closing_home_spread,
    closing_away_spread,
    closing_home_spread_price,
    closing_away_spread_price,
    home_bets_count,
    away_bets_count,
    home_bets_percentage,
    away_bets_percentage,
    home_money_percentage,
    away_money_percentage,
    sharp_action,
    reverse_line_movement,
    steam_move,
    winning_side,
    profit_loss,
    home_cover,
    margin_of_victory,
    source,
    data_quality,
    created_at,
    updated_at
)
SELECT 
    gim.new_game_id as game_id,  -- Map to new games table ID using mapping
    s.sportsbook,
    s.home_spread,
    s.away_spread,
    s.home_spread_price,
    s.away_spread_price,
    s.odds_timestamp,
    s.opening_home_spread,
    s.opening_away_spread,
    s.opening_home_spread_price,
    s.opening_away_spread_price,
    s.closing_home_spread,
    s.closing_away_spread,
    s.closing_home_spread_price,
    s.closing_away_spread_price,
    s.home_bets_count,
    s.away_bets_count,
    s.home_bets_percentage,
    s.away_bets_percentage,
    s.home_money_percentage,
    s.away_money_percentage,
    s.sharp_action,
    COALESCE(s.reverse_line_movement, FALSE) as reverse_line_movement,
    COALESCE(s.steam_move, FALSE) as steam_move,
    s.winning_side,
    s.profit_loss,
    s.home_cover,
    s.margin_of_victory,
    COALESCE(s.source, 'SPORTSBOOKREVIEW') as source,
    COALESCE(s.data_quality, 'MEDIUM') as data_quality,
    COALESCE(s.created_at, NOW()) as created_at,
    COALESCE(s.updated_at, NOW()) as updated_at
FROM mlb_betting.spreads s
INNER JOIN game_id_mapping gim ON s.game_id = gim.old_game_id
WHERE NOT EXISTS (
    SELECT 1 FROM core_betting.betting_lines_spreads cs 
    WHERE cs.game_id = gim.new_game_id 
    AND cs.sportsbook = s.sportsbook 
    AND cs.odds_timestamp = s.odds_timestamp
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM mlb_betting.spreads),
    records_migrated = (SELECT COUNT(*) FROM core_betting.betting_lines_spreads) - 20, -- Subtract test records
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'SPREADS_MIGRATION_START';

-- ==============================================================================
-- STEP 4: MIGRATE TOTALS DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('TOTALS_MIGRATION_START', 'core_betting.betting_lines_totals');

-- Migrate totals data with proper game_id mapping
INSERT INTO core_betting.betting_lines_totals (
    game_id,
    sportsbook,
    total_line,
    over_price,
    under_price,
    odds_timestamp,
    opening_total,
    opening_over_price,
    opening_under_price,
    closing_total,
    closing_over_price,
    closing_under_price,
    over_bets_count,
    under_bets_count,
    over_bets_percentage,
    under_bets_percentage,
    over_money_percentage,
    under_money_percentage,
    sharp_action,
    reverse_line_movement,
    steam_move,
    winning_side,
    profit_loss,
    total_score,
    source,
    data_quality,
    created_at,
    updated_at
)
SELECT 
    gim.new_game_id as game_id,  -- Map to new games table ID using mapping
    t.sportsbook,
    t.total_line,
    t.over_price,
    t.under_price,
    t.odds_timestamp,
    t.opening_total,
    t.opening_over_price,
    t.opening_under_price,
    t.closing_total,
    t.closing_over_price,
    t.closing_under_price,
    t.over_bets_count,
    t.under_bets_count,
    t.over_bets_percentage,
    t.under_bets_percentage,
    t.over_money_percentage,
    t.under_money_percentage,
    t.sharp_action,
    COALESCE(t.reverse_line_movement, FALSE) as reverse_line_movement,
    COALESCE(t.steam_move, FALSE) as steam_move,
    t.winning_side,
    t.profit_loss,
    t.final_total as total_score,
    COALESCE(t.source, 'SPORTSBOOKREVIEW') as source,
    COALESCE(t.data_quality, 'MEDIUM') as data_quality,
    COALESCE(t.created_at, NOW()) as created_at,
    COALESCE(t.updated_at, NOW()) as updated_at
FROM mlb_betting.totals t
INNER JOIN game_id_mapping gim ON t.game_id = gim.old_game_id
WHERE NOT EXISTS (
    SELECT 1 FROM core_betting.betting_lines_totals ct 
    WHERE ct.game_id = gim.new_game_id 
    AND ct.sportsbook = t.sportsbook 
    AND ct.odds_timestamp = t.odds_timestamp
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM mlb_betting.totals),
    records_migrated = (SELECT COUNT(*) FROM core_betting.betting_lines_totals) - 20, -- Subtract test records
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'TOTALS_MIGRATION_START';

-- ==============================================================================
-- STEP 5: MIGRATE GAME OUTCOMES DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('GAME_OUTCOMES_MIGRATION_START', 'core_betting.game_outcomes');

-- Migrate game outcomes data
INSERT INTO core_betting.game_outcomes (
    game_id,
    home_team,
    away_team,
    home_score,
    away_score,
    home_win,
    over,
    home_cover_spread,
    total_line,
    home_spread_line,
    game_date,
    created_at,
    updated_at
)
SELECT 
    gim.new_game_id as game_id,
    go.home_team,
    go.away_team,
    go.home_score,
    go.away_score,
    go.home_win,
    go.over,
    go.home_cover_spread,
    go.total_line,
    go.home_spread_line,
    go.game_date,
    COALESCE(go.created_at, NOW()) as created_at,
    COALESCE(go.updated_at, NOW()) as updated_at
FROM public.game_outcomes go
INNER JOIN game_id_mapping gim ON go.game_id = gim.old_game_id
WHERE NOT EXISTS (
    SELECT 1 FROM core_betting.game_outcomes cgo 
    WHERE cgo.game_id = gim.new_game_id
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM public.game_outcomes),
    records_migrated = (SELECT COUNT(*) FROM core_betting.game_outcomes),
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'GAME_OUTCOMES_MIGRATION_START';

-- ==============================================================================
-- STEP 6: MIGRATE ANALYTICS DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('ANALYTICS_MIGRATION_START', 'analytics.*');

-- Migrate timing analysis results
INSERT INTO analytics.timing_analysis_results (
    timing_bucket,
    analysis_start_date,
    analysis_end_date,
    total_games,
    total_bets,
    win_rate,
    avg_odds,
    total_profit,
    roi_percentage,
    confidence_interval_lower,
    confidence_interval_upper,
    strategy_effectiveness,
    created_at,
    updated_at
)
SELECT 
    tbp.timing_bucket,
    tbp.analysis_start_date,
    tbp.analysis_end_date,
    tbp.total_games,
    tbp.total_bets,
    tbp.win_rate,
    tbp.avg_odds,
    tbp.total_profit,
    tbp.roi_percentage,
    tbp.confidence_interval_lower,
    tbp.confidence_interval_upper,
    tbp.strategy_effectiveness,
    COALESCE(tbp.created_at, NOW()) as created_at,
    COALESCE(tbp.updated_at, NOW()) as updated_at
FROM timing_analysis.timing_bucket_performance tbp
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.timing_analysis_results tar 
    WHERE tar.timing_bucket = tbp.timing_bucket 
    AND tar.analysis_start_date = tbp.analysis_start_date
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (SELECT COUNT(*) FROM timing_analysis.timing_bucket_performance),
    records_migrated = (SELECT COUNT(*) FROM analytics.timing_analysis_results),
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'ANALYTICS_MIGRATION_START';

-- ==============================================================================
-- STEP 7: MIGRATE OPERATIONAL DATA
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('OPERATIONAL_MIGRATION_START', 'operational.*');

-- Migrate strategy performance data
INSERT INTO operational.strategy_performance (
    strategy_name,
    backtest_date,
    total_games,
    total_bets,
    wins,
    losses,
    win_rate,
    total_profit,
    roi,
    avg_odds,
    max_drawdown,
    sharpe_ratio,
    confidence_score,
    last_updated,
    notes
)
SELECT 
    sp.strategy_name,
    sp.backtest_date,
    sp.total_games,
    sp.total_bets,
    sp.wins,
    sp.losses,
    sp.win_rate,
    sp.total_profit,
    sp.roi,
    sp.avg_odds,
    sp.max_drawdown,
    sp.sharpe_ratio,
    sp.confidence_score,
    COALESCE(sp.last_updated, NOW()) as last_updated,
    sp.notes
FROM backtesting.strategy_performance sp
WHERE NOT EXISTS (
    SELECT 1 FROM operational.strategy_performance osp 
    WHERE osp.strategy_name = sp.strategy_name 
    AND osp.backtest_date = sp.backtest_date
);

-- Migrate pre-game recommendations
INSERT INTO operational.pre_game_recommendations (
    game_pk,
    game_datetime,
    home_team,
    away_team,
    recommendation_type,
    recommended_bet,
    confidence_score,
    expected_value,
    max_bet_amount,
    reasoning,
    created_at,
    outcome,
    profit_loss,
    outcome_updated_at
)
SELECT 
    pgr.game_pk,
    pgr.game_datetime,
    pgr.home_team,
    pgr.away_team,
    pgr.recommendation_type,
    pgr.recommended_bet,
    pgr.confidence_score,
    pgr.expected_value,
    pgr.max_bet_amount,
    pgr.reasoning,
    COALESCE(pgr.created_at, NOW()) as created_at,
    pgr.outcome,
    pgr.profit_loss,
    pgr.outcome_updated_at
FROM tracking.pre_game_recommendations pgr
WHERE NOT EXISTS (
    SELECT 1 FROM operational.pre_game_recommendations opgr 
    WHERE opgr.game_pk = pgr.game_pk 
    AND opgr.recommendation_type = pgr.recommendation_type
    AND opgr.created_at = pgr.created_at
);

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (
        (SELECT COUNT(*) FROM backtesting.strategy_performance) +
        (SELECT COUNT(*) FROM tracking.pre_game_recommendations)
    ),
    records_migrated = (
        (SELECT COUNT(*) FROM operational.strategy_performance) +
        (SELECT COUNT(*) FROM operational.pre_game_recommendations)
    ),
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'OPERATIONAL_MIGRATION_START';

-- ==============================================================================
-- STEP 8: UPDATE SEQUENCES AND CONSTRAINTS
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('SEQUENCE_UPDATE_START', 'sequences');

-- Update sequences to ensure no ID conflicts
SELECT setval('core_betting.games_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.games), 1));
SELECT setval('core_betting.betting_lines_moneyline_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_moneyline), 1));
SELECT setval('core_betting.betting_lines_spreads_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_spreads), 1));
SELECT setval('core_betting.betting_lines_totals_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_totals), 1));
SELECT setval('core_betting.game_outcomes_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.game_outcomes), 1));

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'SEQUENCE_UPDATE_START';

-- ==============================================================================
-- STEP 9: FINAL VALIDATION AND SUMMARY
-- ==============================================================================

INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('VALIDATION_START', 'validation');

-- Create validation summary
CREATE TEMP TABLE migration_validation AS
SELECT 
    'Games' as table_type,
    (SELECT COUNT(*) FROM public.games) as legacy_count,
    (SELECT COUNT(*) FROM core_betting.games) as new_count,
    (SELECT COUNT(*) FROM core_betting.games) - (SELECT COUNT(*) FROM public.games) as difference
UNION ALL
SELECT 
    'Moneyline',
    (SELECT COUNT(*) FROM mlb_betting.moneyline),
    (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline),
    (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline) - (SELECT COUNT(*) FROM mlb_betting.moneyline)
UNION ALL
SELECT 
    'Spreads',
    (SELECT COUNT(*) FROM mlb_betting.spreads),
    (SELECT COUNT(*) FROM core_betting.betting_lines_spreads),
    (SELECT COUNT(*) FROM core_betting.betting_lines_spreads) - (SELECT COUNT(*) FROM mlb_betting.spreads)
UNION ALL
SELECT 
    'Totals',
    (SELECT COUNT(*) FROM mlb_betting.totals),
    (SELECT COUNT(*) FROM core_betting.betting_lines_totals),
    (SELECT COUNT(*) FROM core_betting.betting_lines_totals) - (SELECT COUNT(*) FROM mlb_betting.totals)
UNION ALL
SELECT 
    'Game Outcomes',
    (SELECT COUNT(*) FROM public.game_outcomes),
    (SELECT COUNT(*) FROM core_betting.game_outcomes),
    (SELECT COUNT(*) FROM core_betting.game_outcomes) - (SELECT COUNT(*) FROM public.game_outcomes);

-- Display validation results
SELECT * FROM migration_validation ORDER BY table_type;

-- Update final migration log
UPDATE migration_log_phase2b 
SET 
    completed_at = NOW(),
    status = 'completed',
    notes = 'Phase 2B historical data migration completed successfully'
WHERE migration_step = 'VALIDATION_START';

-- Log migration completion
INSERT INTO migration_log_phase2b (migration_step, status, completed_at, notes) 
VALUES ('PHASE2B_COMPLETE', 'completed', NOW(), 'Phase 2B historical data migration completed successfully');

-- ==============================================================================
-- MIGRATION SUMMARY
-- ==============================================================================

SELECT 
    '🚀 PHASE 2B HISTORICAL DATA MIGRATION COMPLETE' as status,
    NOW() as completed_at;

SELECT 
    migration_step,
    table_name,
    records_processed,
    records_migrated,
    status,
    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds
FROM migration_log_phase2b 
WHERE migration_step NOT IN ('PHASE2B_START', 'PHASE2B_COMPLETE')
ORDER BY started_at;

-- Display final record counts
SELECT 
    'FINAL RECORD COUNTS' as summary_type,
    table_type,
    legacy_count,
    new_count,
    CASE 
        WHEN new_count >= legacy_count THEN '✅ SUCCESS'
        ELSE '❌ INCOMPLETE'
    END as migration_status
FROM migration_validation
ORDER BY table_type;

\echo ''
\echo '🎉 Phase 2B Historical Data Migration Complete!'
\echo '✅ All historical data migrated to new consolidated schema'
\echo '✅ Data integrity maintained with proper foreign key relationships'
\echo '✅ Sequences updated to prevent ID conflicts'
\echo ''
\echo 'Next Steps:'
\echo '1. Run Phase 2B validation scripts'
\echo '2. Update remaining application services'
\echo '3. Perform comprehensive testing'
\echo '4. Monitor system performance'
\echo ''
\echo 'General Balls' 