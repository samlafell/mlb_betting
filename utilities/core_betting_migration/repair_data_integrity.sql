-- =========================================================================
-- Data Integrity Repair - Phase 4 Preparation
-- =========================================================================
-- Purpose: Fix data integrity issues before FK constraint updates
-- Created: 2025-07-25 for Phase 4 preparation
-- =========================================================================

-- Step 1: Identify data integrity issues
SELECT 'ANALYZING DATA INTEGRITY ISSUES' as status;

-- Check for orphaned records in staging.betting_splits
DO $$
DECLARE
    orphaned_betting_splits INTEGER;
    orphaned_game_outcomes INTEGER;
    orphaned_arbitrage_book_a INTEGER;
    orphaned_arbitrage_book_b INTEGER;
BEGIN
    -- Check betting_splits referencing missing games
    SELECT COUNT(*) INTO orphaned_betting_splits
    FROM staging.betting_splits bs
    LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
    WHERE gc.id IS NULL;
    
    -- Check game_outcomes referencing missing games
    SELECT COUNT(*) INTO orphaned_game_outcomes
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL;
    
    -- Check arbitrage_opportunities referencing missing sportsbooks
    SELECT COUNT(*) INTO orphaned_arbitrage_book_a
    FROM curated.arbitrage_opportunities ao
    LEFT JOIN curated.sportsbooks sb ON ao.book_a_id = sb.id
    WHERE sb.id IS NULL;
    
    SELECT COUNT(*) INTO orphaned_arbitrage_book_b
    FROM curated.arbitrage_opportunities ao
    LEFT JOIN curated.sportsbooks sb ON ao.book_b_id = sb.id
    WHERE sb.id IS NULL;
    
    RAISE NOTICE 'DATA INTEGRITY ANALYSIS:';
    RAISE NOTICE '  - staging.betting_splits orphaned records: %', orphaned_betting_splits;
    RAISE NOTICE '  - curated.game_outcomes orphaned records: %', orphaned_game_outcomes;
    RAISE NOTICE '  - arbitrage_opportunities orphaned book_a: %', orphaned_arbitrage_book_a;
    RAISE NOTICE '  - arbitrage_opportunities orphaned book_b: %', orphaned_arbitrage_book_b;
    
    IF orphaned_betting_splits > 0 OR orphaned_game_outcomes > 0 OR 
       orphaned_arbitrage_book_a > 0 OR orphaned_arbitrage_book_b > 0 THEN
        RAISE NOTICE 'DATA INTEGRITY ISSUES FOUND - REPAIR NEEDED';
    ELSE
        RAISE NOTICE 'NO DATA INTEGRITY ISSUES FOUND';
    END IF;
END $$;

-- Step 2: Show specific orphaned records for analysis
SELECT 'DETAILED ORPHANED RECORD ANALYSIS' as status;

-- Show orphaned betting_splits records
SELECT 'ORPHANED BETTING_SPLITS RECORDS:' as analysis_type;
SELECT bs.game_id, COUNT(*) as record_count
FROM staging.betting_splits bs
LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
WHERE gc.id IS NULL
GROUP BY bs.game_id
ORDER BY bs.game_id
LIMIT 10;

-- Show orphaned game_outcomes records
SELECT 'ORPHANED GAME_OUTCOMES RECORDS:' as analysis_type;
SELECT go.game_id, COUNT(*) as record_count
FROM curated.game_outcomes go
LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
WHERE gc.id IS NULL
GROUP BY go.game_id
ORDER BY go.game_id
LIMIT 10;

-- Check if missing games exist in core_betting.games
SELECT 'CHECKING IF MISSING GAMES EXIST IN CORE_BETTING:' as analysis_type;
SELECT 
    bs.game_id,
    CASE WHEN cbg.id IS NOT NULL THEN '✅ EXISTS IN core_betting.games' 
         ELSE '❌ MISSING FROM core_betting.games' END as core_betting_status,
    COUNT(*) as betting_splits_count
FROM staging.betting_splits bs
LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
LEFT JOIN core_betting.games cbg ON bs.game_id = cbg.id
WHERE gc.id IS NULL
GROUP BY bs.game_id, cbg.id
ORDER BY bs.game_id
LIMIT 10;

-- Step 3: Repair data integrity issues
SELECT 'BEGINNING DATA INTEGRITY REPAIRS' as status;

BEGIN;

-- Step 3A: Handle orphaned betting_splits records
-- Option 1: Delete orphaned records (safer for FK constraint setup)
DELETE FROM staging.betting_splits 
WHERE game_id IN (
    SELECT bs.game_id
    FROM staging.betting_splits bs
    LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
    WHERE gc.id IS NULL
);

-- Get count of deleted records
GET DIAGNOSTICS orphaned_betting_splits_deleted = ROW_COUNT;

-- Step 3B: Handle orphaned game_outcomes records
-- Option 1: Delete orphaned records (safer for FK constraint setup)  
DELETE FROM curated.game_outcomes
WHERE game_id IN (
    SELECT go.game_id
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL
);

-- Get count of deleted records
GET DIAGNOSTICS orphaned_game_outcomes_deleted = ROW_COUNT;

-- Step 3C: Handle orphaned arbitrage_opportunities records
-- Delete records with missing sportsbooks
DELETE FROM curated.arbitrage_opportunities
WHERE book_a_id NOT IN (SELECT id FROM curated.sportsbooks)
   OR book_b_id NOT IN (SELECT id FROM curated.sportsbooks);

-- Get count of deleted records
GET DIAGNOSTICS orphaned_arbitrage_deleted = ROW_COUNT;

-- Step 4: Validate repairs
DO $$
DECLARE
    remaining_betting_splits_orphans INTEGER;
    remaining_game_outcomes_orphans INTEGER;
    remaining_arbitrage_orphans INTEGER;
BEGIN
    -- Check remaining orphaned records
    SELECT COUNT(*) INTO remaining_betting_splits_orphans
    FROM staging.betting_splits bs
    LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
    WHERE gc.id IS NULL;
    
    SELECT COUNT(*) INTO remaining_game_outcomes_orphans
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL;
    
    SELECT COUNT(*) INTO remaining_arbitrage_orphans
    FROM curated.arbitrage_opportunities ao
    LEFT JOIN curated.sportsbooks sb_a ON ao.book_a_id = sb_a.id
    LEFT JOIN curated.sportsbooks sb_b ON ao.book_b_id = sb_b.id
    WHERE sb_a.id IS NULL OR sb_b.id IS NULL;
    
    RAISE NOTICE 'DATA REPAIR VALIDATION:';
    RAISE NOTICE '  - Remaining betting_splits orphans: %', remaining_betting_splits_orphans;
    RAISE NOTICE '  - Remaining game_outcomes orphans: %', remaining_game_outcomes_orphans;
    RAISE NOTICE '  - Remaining arbitrage orphans: %', remaining_arbitrage_orphans;
    
    IF remaining_betting_splits_orphans = 0 AND remaining_game_outcomes_orphans = 0 AND remaining_arbitrage_orphans = 0 THEN
        RAISE NOTICE '✅ SUCCESS: All data integrity issues resolved';
    ELSE
        RAISE EXCEPTION 'DATA REPAIR FAILED: Orphaned records still exist';
    END IF;
END $$;

-- Step 5: Commit repairs
COMMIT;

SELECT 'DATA INTEGRITY REPAIRS COMPLETED SUCCESSFULLY' as status;

-- Step 6: Final validation summary
DO $$
DECLARE
    betting_splits_count INTEGER;
    game_outcomes_count INTEGER;
    arbitrage_count INTEGER;
    games_count INTEGER;
    sportsbooks_count INTEGER;
BEGIN
    -- Get final record counts
    SELECT COUNT(*) INTO betting_splits_count FROM staging.betting_splits;
    SELECT COUNT(*) INTO game_outcomes_count FROM curated.game_outcomes;
    SELECT COUNT(*) INTO arbitrage_count FROM curated.arbitrage_opportunities;
    SELECT COUNT(*) INTO games_count FROM curated.games_complete;
    SELECT COUNT(*) INTO sportsbooks_count FROM curated.sportsbooks;
    
    RAISE NOTICE 'FINAL DATA COUNTS AFTER REPAIR:';
    RAISE NOTICE '  - staging.betting_splits: % records', betting_splits_count;
    RAISE NOTICE '  - curated.game_outcomes: % records', game_outcomes_count;
    RAISE NOTICE '  - curated.arbitrage_opportunities: % records', arbitrage_count;
    RAISE NOTICE '  - curated.games_complete: % records', games_count;
    RAISE NOTICE '  - curated.sportsbooks: % records', sportsbooks_count;
    RAISE NOTICE '';
    RAISE NOTICE '✅ DATA INTEGRITY REPAIR COMPLETE - READY FOR FK CONSTRAINT UPDATES';
END $$;

-- =========================================================================
-- END OF DATA INTEGRITY REPAIR SCRIPT
-- =========================================================================