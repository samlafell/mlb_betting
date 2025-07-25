-- =========================================================================
-- Simple Data Integrity Repair - Phase 4 Preparation
-- =========================================================================
-- Purpose: Fix orphaned records preventing FK constraint updates
-- =========================================================================

-- Remove orphaned betting_splits records
DELETE FROM staging.betting_splits 
WHERE game_id IN (
    SELECT bs.game_id
    FROM staging.betting_splits bs
    LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
    WHERE gc.id IS NULL
);

-- Remove orphaned game_outcomes records  
DELETE FROM curated.game_outcomes
WHERE game_id IN (
    SELECT go.game_id
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL
);

-- Remove orphaned arbitrage_opportunities records
DELETE FROM curated.arbitrage_opportunities
WHERE book_a_id NOT IN (SELECT id FROM curated.sportsbooks)
   OR book_b_id NOT IN (SELECT id FROM curated.sportsbooks);

-- Validate repair
SELECT 'VALIDATION AFTER REPAIR:' as status;

SELECT 
    'staging.betting_splits orphans' as table_name,
    COUNT(*) as orphaned_count
FROM staging.betting_splits bs
LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
WHERE gc.id IS NULL

UNION ALL

SELECT 
    'curated.game_outcomes orphans' as table_name,
    COUNT(*) as orphaned_count
FROM curated.game_outcomes go
LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
WHERE gc.id IS NULL

UNION ALL

SELECT 
    'curated.arbitrage_opportunities orphans' as table_name,
    COUNT(*) as orphaned_count
FROM curated.arbitrage_opportunities ao
LEFT JOIN curated.sportsbooks sb_a ON ao.book_a_id = sb_a.id
LEFT JOIN curated.sportsbooks sb_b ON ao.book_b_id = sb_b.id
WHERE sb_a.id IS NULL OR sb_b.id IS NULL;