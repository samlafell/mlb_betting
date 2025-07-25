-- =========================================================================
-- FK Constraint Updates - Phase 4 Core Betting Schema Cleanup
-- =========================================================================
-- Purpose: Update all FK constraints to reference curated schema instead of core_betting
-- Created: 2025-07-25 for Phase 4 execution
-- =========================================================================

-- Step 1: Pre-validation checks
SELECT 'STARTING FK CONSTRAINT UPDATES FOR PHASE 4' as status;

-- Validate that target tables exist in curated schema
DO $$
DECLARE
    games_complete_exists BOOLEAN;
    sportsbooks_exists BOOLEAN;
    game_outcomes_exists BOOLEAN;
BEGIN
    -- Check for required curated tables
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'games_complete') INTO games_complete_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'sportsbooks') INTO sportsbooks_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'game_outcomes') INTO game_outcomes_exists;
    
    RAISE NOTICE 'TARGET TABLE VALIDATION:';
    RAISE NOTICE '  - curated.games_complete: %', CASE WHEN games_complete_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    RAISE NOTICE '  - curated.sportsbooks: %', CASE WHEN sportsbooks_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    RAISE NOTICE '  - curated.game_outcomes: %', CASE WHEN game_outcomes_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    
    IF NOT games_complete_exists OR NOT sportsbooks_exists OR NOT game_outcomes_exists THEN
        RAISE EXCEPTION 'VALIDATION FAILED: Required curated tables are missing. Cannot proceed with FK updates.';
    END IF;
    
    RAISE NOTICE 'VALIDATION PASSED: All required curated tables exist';
END $$;

-- Step 2: Count current FK constraints to core_betting
DO $$
DECLARE
    constraint_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema != 'core_betting'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    RAISE NOTICE 'CURRENT FK CONSTRAINTS TO CORE_BETTING: %', constraint_count;
END $$;

-- Step 3: Begin transaction for safe FK updates
BEGIN;

SELECT 'UPDATING FK CONSTRAINTS - TRANSACTION STARTED' as status;

-- Step 3A: Update curated.arbitrage_opportunities constraints
SELECT 'UPDATING: curated.arbitrage_opportunities' as status;

-- Drop existing constraints
ALTER TABLE curated.arbitrage_opportunities DROP CONSTRAINT IF EXISTS arbitrage_opportunities_book_a_id_fkey;
ALTER TABLE curated.arbitrage_opportunities DROP CONSTRAINT IF EXISTS arbitrage_opportunities_book_b_id_fkey;

-- Add new constraints pointing to curated schema
ALTER TABLE curated.arbitrage_opportunities 
ADD CONSTRAINT arbitrage_opportunities_book_a_id_fkey 
FOREIGN KEY (book_a_id) REFERENCES curated.sportsbooks(id);

ALTER TABLE curated.arbitrage_opportunities 
ADD CONSTRAINT arbitrage_opportunities_book_b_id_fkey 
FOREIGN KEY (book_b_id) REFERENCES curated.sportsbooks(id);

-- Step 3B: Update curated.game_outcomes constraint
SELECT 'UPDATING: curated.game_outcomes' as status;

-- Drop existing constraint
ALTER TABLE curated.game_outcomes DROP CONSTRAINT IF EXISTS game_outcomes_game_id_fkey;

-- Add new constraint pointing to curated schema
ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Step 3C: Update staging.betting_splits constraint
SELECT 'UPDATING: staging.betting_splits' as status;

-- Drop existing constraint
ALTER TABLE staging.betting_splits DROP CONSTRAINT IF EXISTS betting_splits_game_id_fkey;

-- Add new constraint pointing to curated schema
ALTER TABLE staging.betting_splits 
ADD CONSTRAINT betting_splits_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Step 4: Validate constraint updates
DO $$
DECLARE
    updated_constraint_count INTEGER;
    remaining_core_betting_constraints INTEGER;
BEGIN
    -- Count constraints now pointing to curated schema
    SELECT COUNT(*) INTO updated_constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'curated'
      AND tc.table_schema != 'curated'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Count any remaining constraints pointing to core_betting
    SELECT COUNT(*) INTO remaining_core_betting_constraints
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema != 'core_betting'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    RAISE NOTICE 'FK CONSTRAINT UPDATE VALIDATION:';
    RAISE NOTICE '  - Constraints now pointing to curated: %', updated_constraint_count;
    RAISE NOTICE '  - Constraints still pointing to core_betting: %', remaining_core_betting_constraints;
    
    IF remaining_core_betting_constraints > 0 THEN
        RAISE EXCEPTION 'FK UPDATE FAILED: % constraints still reference core_betting', remaining_core_betting_constraints;
    END IF;
    
    RAISE NOTICE 'FK CONSTRAINT UPDATES SUCCESSFUL!';
END $$;

-- Step 5: Test referential integrity
SELECT 'TESTING REFERENTIAL INTEGRITY' as status;

DO $$
DECLARE
    arbitrage_test_count INTEGER;
    game_outcomes_test_count INTEGER;
    betting_splits_test_count INTEGER;
BEGIN
    -- Test that all foreign key relationships are valid
    
    -- Test arbitrage_opportunities relationships
    SELECT COUNT(*) INTO arbitrage_test_count
    FROM curated.arbitrage_opportunities ao
    LEFT JOIN curated.sportsbooks sb_a ON ao.book_a_id = sb_a.id
    LEFT JOIN curated.sportsbooks sb_b ON ao.book_b_id = sb_b.id
    WHERE sb_a.id IS NULL OR sb_b.id IS NULL;
    
    -- Test game_outcomes relationships
    SELECT COUNT(*) INTO game_outcomes_test_count
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL;
    
    -- Test betting_splits relationships
    SELECT COUNT(*) INTO betting_splits_test_count
    FROM staging.betting_splits bs
    LEFT JOIN curated.games_complete gc ON bs.game_id = gc.id
    WHERE gc.id IS NULL;
    
    RAISE NOTICE 'REFERENTIAL INTEGRITY TEST RESULTS:';
    RAISE NOTICE '  - arbitrage_opportunities orphaned records: %', arbitrage_test_count;
    RAISE NOTICE '  - game_outcomes orphaned records: %', game_outcomes_test_count;
    RAISE NOTICE '  - betting_splits orphaned records: %', betting_splits_test_count;
    
    IF arbitrage_test_count > 0 OR game_outcomes_test_count > 0 OR betting_splits_test_count > 0 THEN
        RAISE EXCEPTION 'REFERENTIAL INTEGRITY VIOLATION: Orphaned records found';
    END IF;
    
    RAISE NOTICE 'REFERENTIAL INTEGRITY TEST PASSED!';
END $$;

-- Step 6: Commit transaction
COMMIT;

SELECT 'FK CONSTRAINT UPDATES COMMITTED SUCCESSFULLY' as status;

-- Step 7: Final validation - verify core_betting schema can be dropped
SELECT 'FINAL VALIDATION FOR CORE_BETTING SCHEMA DROP' as status;

DO $$
DECLARE
    remaining_dependencies INTEGER;
BEGIN
    -- Final check for any remaining dependencies
    SELECT COUNT(*) INTO remaining_dependencies
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema != 'core_betting';
    
    RAISE NOTICE 'FINAL DEPENDENCY CHECK:';
    RAISE NOTICE '  - External dependencies on core_betting: %', remaining_dependencies;
    
    IF remaining_dependencies = 0 THEN
        RAISE NOTICE '✅ SUCCESS: No external dependencies remain - core_betting schema is ready for cleanup';
    ELSE
        RAISE NOTICE '⚠️  WARNING: % dependencies still exist - investigate before schema cleanup', remaining_dependencies;
    END IF;
END $$;

-- Step 8: Log completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details, end_time)
VALUES ('PHASE_4', 'FK_CONSTRAINT_UPDATES', 'completed', 
    jsonb_build_object(
        'fk_updates_completed_at', CURRENT_TIMESTAMP,
        'constraints_updated', ARRAY[
            'curated.arbitrage_opportunities.book_a_id_fkey',
            'curated.arbitrage_opportunities.book_b_id_fkey', 
            'curated.game_outcomes.game_id_fkey',
            'staging.betting_splits.game_id_fkey'
        ],
        'target_schema', 'curated',
        'source_schema_deprecated', 'core_betting',
        'referential_integrity_validated', true,
        'ready_for_schema_cleanup', true
    ),
    CURRENT_TIMESTAMP);

SELECT 'PHASE 4 FK CONSTRAINT UPDATES COMPLETE - READY FOR SCHEMA CLEANUP!' as final_status;

-- =========================================================================
-- END OF FK CONSTRAINT UPDATE SCRIPT
-- =========================================================================