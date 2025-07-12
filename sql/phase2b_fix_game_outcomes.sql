-- Fix for game outcomes migration - handle text to integer conversion
\set ON_ERROR_STOP on
\timing on

-- Recreate the game_id_mapping table if needed
CREATE TEMPORARY TABLE IF NOT EXISTS game_id_mapping AS
SELECT 
    pg.id as old_game_id,
    cbg.id as new_game_id,
    COALESCE(pg.sportsbookreview_game_id, pg.game_id) as sportsbookreview_game_id
FROM public.games pg
INNER JOIN core_betting.games cbg ON cbg.sportsbookreview_game_id = COALESCE(pg.sportsbookreview_game_id, pg.game_id);

-- Migrate game outcomes data with proper type casting
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
INNER JOIN game_id_mapping gim ON go.game_id::integer = gim.old_game_id  -- Cast text to integer
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

-- Continue with analytics migration
INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('ANALYTICS_MIGRATION_START', 'analytics.*');

-- Check if timing_analysis schema exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'timing_analysis') THEN
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
    END IF;
END $$;

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = COALESCE((SELECT COUNT(*) FROM timing_analysis.timing_bucket_performance), 0),
    records_migrated = (SELECT COUNT(*) FROM analytics.timing_analysis_results),
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'ANALYTICS_MIGRATION_START';

-- Continue with operational migration
INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('OPERATIONAL_MIGRATION_START', 'operational.*');

-- Check if backtesting schema exists and migrate strategy performance
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'backtesting') THEN
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
    END IF;
END $$;

-- Check if tracking schema exists and migrate pre-game recommendations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'tracking') THEN
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
    END IF;
END $$;

-- Update migration log
UPDATE migration_log_phase2b 
SET 
    records_processed = (
        COALESCE((SELECT COUNT(*) FROM backtesting.strategy_performance), 0) +
        COALESCE((SELECT COUNT(*) FROM tracking.pre_game_recommendations), 0)
    ),
    records_migrated = (
        (SELECT COUNT(*) FROM operational.strategy_performance) +
        (SELECT COUNT(*) FROM operational.pre_game_recommendations)
    ),
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'OPERATIONAL_MIGRATION_START';

-- Update sequences
INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('SEQUENCE_UPDATE_START', 'sequences');

SELECT setval('core_betting.games_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.games), 1));
SELECT setval('core_betting.betting_lines_moneyline_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_moneyline), 1));
SELECT setval('core_betting.betting_lines_spreads_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_spreads), 1));
SELECT setval('core_betting.betting_lines_totals_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.betting_lines_totals), 1));
SELECT setval('core_betting.game_outcomes_id_seq', COALESCE((SELECT MAX(id) FROM core_betting.game_outcomes), 1));

UPDATE migration_log_phase2b 
SET 
    completed_at = NOW(),
    status = 'completed'
WHERE migration_step = 'SEQUENCE_UPDATE_START';

-- Final validation
INSERT INTO migration_log_phase2b (migration_step, table_name) 
VALUES ('VALIDATION_START', 'validation');

CREATE TEMP TABLE migration_validation AS
SELECT 
    'Games' as table_type,
    (SELECT COUNT(*) FROM public.games) as legacy_count,
    (SELECT COUNT(*) FROM core_betting.games) as new_count
UNION ALL
SELECT 
    'Moneyline',
    (SELECT COUNT(*) FROM mlb_betting.moneyline),
    (SELECT COUNT(*) FROM core_betting.betting_lines_moneyline)
UNION ALL
SELECT 
    'Spreads',
    (SELECT COUNT(*) FROM mlb_betting.spreads),
    (SELECT COUNT(*) FROM core_betting.betting_lines_spreads)
UNION ALL
SELECT 
    'Totals',
    (SELECT COUNT(*) FROM mlb_betting.totals),
    (SELECT COUNT(*) FROM core_betting.betting_lines_totals)
UNION ALL
SELECT 
    'Game Outcomes',
    (SELECT COUNT(*) FROM public.game_outcomes),
    (SELECT COUNT(*) FROM core_betting.game_outcomes);

SELECT * FROM migration_validation ORDER BY table_type;

UPDATE migration_log_phase2b 
SET 
    completed_at = NOW(),
    status = 'completed',
    notes = 'Phase 2B historical data migration completed successfully'
WHERE migration_step = 'VALIDATION_START';

INSERT INTO migration_log_phase2b (migration_step, status, completed_at, notes) 
VALUES ('PHASE2B_COMPLETE', 'completed', NOW(), 'Phase 2B historical data migration completed successfully');

SELECT '🚀 PHASE 2B HISTORICAL DATA MIGRATION COMPLETE' as status; 