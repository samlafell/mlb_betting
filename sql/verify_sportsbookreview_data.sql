-- SportsbookReview Data Verification Queries
-- 
-- This script contains comprehensive verification queries to check that
-- SportsbookReview data was saved correctly in the PostgreSQL database.
-- 
-- Usage: 
--   psql -h localhost -U samlafell -d mlb_betting -f verify_sportsbookreview_data.sql
--   
-- Or run individual sections by copying queries into your SQL client

-- ==============================================================================
-- SET TARGET DATE FOR VERIFICATION
-- ==============================================================================

-- Modify this date to verify data for a specific day
-- Format: 'YYYY-MM-DD'
\set target_date '2025-07-06'

\echo '============================================================'
\echo 'SPORTSBOOKREVIEW DATA VERIFICATION'
\echo '============================================================'
\echo 'Target Date: ' :'target_date'
\echo ''

-- ==============================================================================
-- 1. BASIC DATA COUNTS
-- ==============================================================================

\echo '1. BASIC DATA COUNTS'
\echo '------------------------------------------------------------'

-- Games count for the target date
SELECT 
    'Games Count' as metric,
    COUNT(*) as count,
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM public.games 
WHERE game_date = :'target_date';

-- Betting records count by type
SELECT 
    'Moneyline Records' as metric,
    COUNT(*) as count,
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM mlb_betting.moneyline m
JOIN public.games g ON m.game_id = g.id
WHERE g.game_date = :'target_date'

UNION ALL

SELECT 
    'Spread Records' as metric,
    COUNT(*) as count,
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM mlb_betting.spreads s
JOIN public.games g ON s.game_id = g.id
WHERE g.game_date = :'target_date'

UNION ALL

SELECT 
    'Total Records' as metric,
    COUNT(*) as count,
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM mlb_betting.totals t
JOIN public.games g ON t.game_id = g.id
WHERE g.game_date = :'target_date';

\echo ''

-- ==============================================================================
-- 2. GAME DATA QUALITY
-- ==============================================================================

\echo '2. GAME DATA QUALITY'
\echo '------------------------------------------------------------'

-- Check for complete game records
SELECT 
    COUNT(*) as total_games,
    COUNT(CASE WHEN home_team IS NOT NULL AND away_team IS NOT NULL THEN 1 END) as games_with_teams,
    COUNT(CASE WHEN game_datetime IS NOT NULL THEN 1 END) as games_with_datetime,
    COUNT(CASE WHEN sportsbookreview_game_id IS NOT NULL THEN 1 END) as games_with_sbr_id,
    COUNT(CASE WHEN home_team IS NULL OR away_team IS NULL THEN 1 END) as games_missing_teams,
    ROUND(
        COUNT(CASE WHEN home_team IS NOT NULL AND away_team IS NOT NULL THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as data_completeness_pct
FROM public.games 
WHERE game_date = :'target_date';

-- List all teams found
SELECT 
    'Teams Found' as category,
    STRING_AGG(DISTINCT team, ', ' ORDER BY team) as teams
FROM (
    SELECT home_team as team FROM public.games WHERE game_date = :'target_date'
    UNION
    SELECT away_team as team FROM public.games WHERE game_date = :'target_date'
) teams;

\echo ''

-- ==============================================================================
-- 3. BETTING DATA QUALITY
-- ==============================================================================

\echo '3. BETTING DATA QUALITY'
\echo '------------------------------------------------------------'

-- Quality metrics by bet type
SELECT 
    'moneyline' as bet_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN home_ml IS NOT NULL AND away_ml IS NOT NULL THEN 1 END) as complete_records,
    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook,
    ROUND(
        COUNT(CASE WHEN home_ml IS NOT NULL AND away_ml IS NOT NULL THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as completeness_pct
FROM mlb_betting.moneyline m
JOIN public.games g ON m.game_id = g.id
WHERE g.game_date = :'target_date'

UNION ALL

SELECT 
    'spreads' as bet_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN home_spread IS NOT NULL AND away_spread IS NOT NULL THEN 1 END) as complete_records,
    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook,
    ROUND(
        COUNT(CASE WHEN home_spread IS NOT NULL AND away_spread IS NOT NULL THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as completeness_pct
FROM mlb_betting.spreads s
JOIN public.games g ON s.game_id = g.id
WHERE g.game_date = :'target_date'

UNION ALL

SELECT 
    'totals' as bet_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN total_line IS NOT NULL THEN 1 END) as complete_records,
    COUNT(CASE WHEN sportsbook IS NOT NULL THEN 1 END) as records_with_sportsbook,
    ROUND(
        COUNT(CASE WHEN total_line IS NOT NULL THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as completeness_pct
FROM mlb_betting.totals t
JOIN public.games g ON t.game_id = g.id
WHERE g.game_date = :'target_date';

\echo ''

-- ==============================================================================
-- 4. SPORTSBOOK COVERAGE
-- ==============================================================================

\echo '4. SPORTSBOOK COVERAGE'
\echo '------------------------------------------------------------'

-- Count records by sportsbook
SELECT 
    sportsbook,
    COUNT(*) as total_records,
    COUNT(CASE WHEN bet_type = 'moneyline' THEN 1 END) as moneyline_records,
    COUNT(CASE WHEN bet_type = 'spreads' THEN 1 END) as spread_records,
    COUNT(CASE WHEN bet_type = 'totals' THEN 1 END) as total_records_count
FROM (
    SELECT sportsbook, 'moneyline' as bet_type FROM mlb_betting.moneyline m
    JOIN public.games g ON m.game_id = g.id
    WHERE g.game_date = :'target_date'
    
    UNION ALL
    
    SELECT sportsbook, 'spreads' as bet_type FROM mlb_betting.spreads s
    JOIN public.games g ON s.game_id = g.id
    WHERE g.game_date = :'target_date'
    
    UNION ALL
    
    SELECT sportsbook, 'totals' as bet_type FROM mlb_betting.totals t
    JOIN public.games g ON t.game_id = g.id
    WHERE g.game_date = :'target_date'
) combined
GROUP BY sportsbook
ORDER BY total_records DESC;

\echo ''

-- ==============================================================================
-- 5. DATA CONSISTENCY CHECKS
-- ==============================================================================

\echo '5. DATA CONSISTENCY CHECKS'
\echo '------------------------------------------------------------'

-- Check for orphaned betting records
SELECT 
    'Orphaned Records Check' as check_name,
    CASE WHEN total_orphaned = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
    total_orphaned as orphaned_count,
    orphaned_moneyline,
    orphaned_spreads,
    orphaned_totals
FROM (
    SELECT 
        (SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_moneyline,
        (SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_spreads,
        (SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_totals
) sub
CROSS JOIN (
    SELECT 
        sub.orphaned_moneyline + sub.orphaned_spreads + sub.orphaned_totals as total_orphaned
    FROM (
        SELECT 
            (SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_moneyline,
            (SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_spreads,
            (SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id NOT IN (SELECT id FROM public.games)) as orphaned_totals
    ) sub
) totals;

-- Check for duplicate game records
SELECT 
    'Duplicate Games Check' as check_name,
    CASE WHEN duplicate_count = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
    duplicate_count
FROM (
    SELECT COUNT(*) - COUNT(DISTINCT sportsbookreview_game_id) as duplicate_count
    FROM public.games
    WHERE game_date = :'target_date'
    AND sportsbookreview_game_id IS NOT NULL
) dup_check;

\echo ''

-- ==============================================================================
-- 6. SAMPLE DATA INSPECTION
-- ==============================================================================

\echo '6. SAMPLE DATA INSPECTION'
\echo '------------------------------------------------------------'

-- Show sample game records
\echo 'Sample Game Records:'
SELECT 
    g.id,
    g.sportsbookreview_game_id,
    g.home_team,
    g.away_team,
    g.game_datetime,
    g.home_score,
    g.away_score,
    g.game_status
FROM public.games g
WHERE g.game_date = :'target_date'
ORDER BY g.game_datetime
LIMIT 5;

\echo ''
\echo 'Sample Moneyline Records:'
-- Show sample betting records
SELECT 
    g.home_team || ' vs ' || g.away_team as matchup,
    m.sportsbook,
    m.home_ml,
    m.away_ml,
    m.odds_timestamp
FROM mlb_betting.moneyline m
JOIN public.games g ON m.game_id = g.id
WHERE g.game_date = :'target_date'
ORDER BY g.game_datetime, m.sportsbook
LIMIT 10;

\echo ''

-- ==============================================================================
-- 7. SUMMARY STATISTICS
-- ==============================================================================

\echo '7. SUMMARY STATISTICS'
\echo '------------------------------------------------------------'

-- Overall summary
SELECT 
    'OVERALL SUMMARY' as section,
    (SELECT COUNT(*) FROM public.games WHERE game_date = :'target_date') as total_games,
    (SELECT COUNT(DISTINCT sportsbook) FROM (
        SELECT sportsbook FROM mlb_betting.moneyline m JOIN public.games g ON m.game_id = g.id WHERE g.game_date = :'target_date'
        UNION
        SELECT sportsbook FROM mlb_betting.spreads s JOIN public.games g ON s.game_id = g.id WHERE g.game_date = :'target_date'
        UNION
        SELECT sportsbook FROM mlb_betting.totals t JOIN public.games g ON t.game_id = g.id WHERE g.game_date = :'target_date'
    ) all_books) as unique_sportsbooks,
    (SELECT COUNT(*) FROM mlb_betting.moneyline m JOIN public.games g ON m.game_id = g.id WHERE g.game_date = :'target_date') +
    (SELECT COUNT(*) FROM mlb_betting.spreads s JOIN public.games g ON s.game_id = g.id WHERE g.game_date = :'target_date') +
    (SELECT COUNT(*) FROM mlb_betting.totals t JOIN public.games g ON t.game_id = g.id WHERE g.game_date = :'target_date') as total_betting_records;

\echo ''

-- ==============================================================================
-- 8. QUICK VERIFICATION CHECKLIST
-- ==============================================================================

\echo '8. QUICK VERIFICATION CHECKLIST'
\echo '------------------------------------------------------------'

-- Generate a quick pass/fail checklist
WITH verification_checks AS (
    SELECT 
        'Games Found' as check_name,
        CASE WHEN (SELECT COUNT(*) FROM public.games WHERE game_date = :'target_date') > 0 
             THEN '✅ PASS' ELSE '❌ FAIL' END as status,
        1 as check_order
    
    UNION ALL
    
    SELECT 
        'Betting Records Found' as check_name,
        CASE WHEN (
            (SELECT COUNT(*) FROM mlb_betting.moneyline m JOIN public.games g ON m.game_id = g.id WHERE g.game_date = :'target_date') +
            (SELECT COUNT(*) FROM mlb_betting.spreads s JOIN public.games g ON s.game_id = g.id WHERE g.game_date = :'target_date') +
            (SELECT COUNT(*) FROM mlb_betting.totals t JOIN public.games g ON t.game_id = g.id WHERE g.game_date = :'target_date')
        ) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
        2 as check_order
    
    UNION ALL
    
    SELECT 
        'Multiple Sportsbooks' as check_name,
        CASE WHEN (SELECT COUNT(DISTINCT sportsbook) FROM (
            SELECT sportsbook FROM mlb_betting.moneyline m JOIN public.games g ON m.game_id = g.id WHERE g.game_date = :'target_date'
            UNION
            SELECT sportsbook FROM mlb_betting.spreads s JOIN public.games g ON s.game_id = g.id WHERE g.game_date = :'target_date'
            UNION
            SELECT sportsbook FROM mlb_betting.totals t JOIN public.games g ON t.game_id = g.id WHERE g.game_date = :'target_date'
        ) all_books) >= 3 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
        3 as check_order
    
    UNION ALL
    
    SELECT 
        'No Orphaned Records' as check_name,
        CASE WHEN (
            (SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id NOT IN (SELECT id FROM public.games)) +
            (SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id NOT IN (SELECT id FROM public.games)) +
            (SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id NOT IN (SELECT id FROM public.games))
        ) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
        4 as check_order
)
SELECT check_name, status
FROM verification_checks
ORDER BY check_order;

\echo ''
\echo 'Verification completed for date: ' :'target_date'
\echo '============================================================' 