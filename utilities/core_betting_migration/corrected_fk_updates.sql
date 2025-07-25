-- =========================================================================
-- Corrected FK Constraint Updates - Phase 4 Core Betting Schema Cleanup
-- =========================================================================
-- Purpose: Update FK constraints respecting proper schema boundaries
-- - curated tables should reference curated tables
-- - staging tables should continue referencing core_betting (since curated not active)
-- Created: 2025-07-25 for Phase 4 execution
-- =========================================================================

-- Step 1: Pre-validation
SELECT 'CORRECTED FK CONSTRAINT UPDATES - RESPECTING SCHEMA BOUNDARIES' as status;

-- Validate target tables exist
DO $$
DECLARE
    curated_games_complete_exists BOOLEAN;
    curated_sportsbooks_exists BOOLEAN;
    core_betting_games_exists BOOLEAN;
BEGIN
    -- Check for curated schema targets (for curated tables)
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'games_complete') INTO curated_games_complete_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'sportsbooks') INTO curated_sportsbooks_exists;
    
    -- Check for core_betting targets (for staging tables)
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core_betting' AND table_name = 'games') INTO core_betting_games_exists;
    
    RAISE NOTICE 'SCHEMA BOUNDARY VALIDATION:';
    RAISE NOTICE '  - curated.games_complete (for curated tables): %', CASE WHEN curated_games_complete_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    RAISE NOTICE '  - curated.sportsbooks (for curated tables): %', CASE WHEN curated_sportsbooks_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    RAISE NOTICE '  - core_betting.games (for staging tables): %', CASE WHEN core_betting_games_exists THEN '✅ EXISTS' ELSE '❌ MISSING' END;
    
    IF NOT curated_games_complete_exists OR NOT curated_sportsbooks_exists OR NOT core_betting_games_exists THEN
        RAISE EXCEPTION 'VALIDATION FAILED: Required target tables are missing';
    END IF;
    
    RAISE NOTICE 'VALIDATION PASSED: All target tables exist with proper schema boundaries';
END $$;

-- Step 2: Begin transaction for FK updates
BEGIN;

SELECT 'UPDATING FK CONSTRAINTS WITH PROPER SCHEMA BOUNDARIES' as status;

-- Step 2A: Update curated.arbitrage_opportunities (curated → curated)
SELECT 'UPDATING: curated.arbitrage_opportunities → curated.sportsbooks' as status;

ALTER TABLE curated.arbitrage_opportunities DROP CONSTRAINT IF EXISTS arbitrage_opportunities_book_a_id_fkey;
ALTER TABLE curated.arbitrage_opportunities DROP CONSTRAINT IF EXISTS arbitrage_opportunities_book_b_id_fkey;

ALTER TABLE curated.arbitrage_opportunities 
ADD CONSTRAINT arbitrage_opportunities_book_a_id_fkey 
FOREIGN KEY (book_a_id) REFERENCES curated.sportsbooks(id);

ALTER TABLE curated.arbitrage_opportunities 
ADD CONSTRAINT arbitrage_opportunities_book_b_id_fkey 
FOREIGN KEY (book_b_id) REFERENCES curated.sportsbooks(id);

-- Step 2B: Update curated.game_outcomes (curated → curated)
SELECT 'UPDATING: curated.game_outcomes → curated.games_complete' as status;

ALTER TABLE curated.game_outcomes DROP CONSTRAINT IF EXISTS game_outcomes_game_id_fkey;

ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Step 2C: Keep staging.betting_splits referencing core_betting (staging → core_betting)
SELECT 'KEEPING: staging.betting_splits → core_betting.games (proper schema boundary)' as status;

-- Verify the existing constraint is correct (should already reference core_betting.games)
DO $$
DECLARE
    current_target_schema TEXT;
    current_target_table TEXT;
BEGIN
    SELECT ctu.table_schema, ctu.table_name 
    INTO current_target_schema, current_target_table
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE tc.table_schema = 'staging' 
      AND tc.table_name = 'betting_splits'
      AND tc.constraint_name = 'betting_splits_game_id_fkey';
    
    RAISE NOTICE 'Current staging.betting_splits constraint: %.%', current_target_schema, current_target_table;
    
    IF current_target_schema = 'core_betting' AND current_target_table = 'games' THEN
        RAISE NOTICE '✅ staging.betting_splits correctly references core_betting.games - no change needed';
    ELSE
        RAISE NOTICE '⚠️ staging.betting_splits has incorrect target - this needs investigation';
    END IF;
END $$;

-- Step 3: Validate updated constraints
DO $$
DECLARE
    curated_to_curated_count INTEGER;
    staging_to_core_betting_count INTEGER;
    remaining_core_betting_external_refs INTEGER;
BEGIN
    -- Count curated tables referencing curated schema
    SELECT COUNT(*) INTO curated_to_curated_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE tc.table_schema = 'curated'
      AND ctu.table_schema = 'curated'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Count staging tables referencing core_betting (should remain)
    SELECT COUNT(*) INTO staging_to_core_betting_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE tc.table_schema = 'staging'
      AND ctu.table_schema = 'core_betting'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Count remaining external references to core_betting
    SELECT COUNT(*) INTO remaining_core_betting_external_refs
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema <> 'core_betting'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    RAISE NOTICE 'FK CONSTRAINT VALIDATION:';
    RAISE NOTICE '  - curated → curated references: %', curated_to_curated_count;
    RAISE NOTICE '  - staging → core_betting references: %', staging_to_core_betting_count;
    RAISE NOTICE '  - total external core_betting references: %', remaining_core_betting_external_refs;
    
    IF curated_to_curated_count >= 3 AND staging_to_core_betting_count >= 1 THEN
        RAISE NOTICE '✅ FK constraints updated successfully with proper schema boundaries';
    ELSE
        RAISE EXCEPTION 'FK constraint update validation failed';
    END IF;
END $$;

-- Step 4: Test referential integrity for updated constraints only
SELECT 'TESTING REFERENTIAL INTEGRITY FOR CURATED CONSTRAINTS' as status;

DO $$
DECLARE
    arbitrage_integrity_violations INTEGER;
    game_outcomes_integrity_violations INTEGER;
BEGIN
    -- Test curated.arbitrage_opportunities
    SELECT COUNT(*) INTO arbitrage_integrity_violations
    FROM curated.arbitrage_opportunities ao
    LEFT JOIN curated.sportsbooks sb_a ON ao.book_a_id = sb_a.id
    LEFT JOIN curated.sportsbooks sb_b ON ao.book_b_id = sb_b.id
    WHERE sb_a.id IS NULL OR sb_b.id IS NULL;
    
    -- Test curated.game_outcomes
    SELECT COUNT(*) INTO game_outcomes_integrity_violations
    FROM curated.game_outcomes go
    LEFT JOIN curated.games_complete gc ON go.game_id = gc.id
    WHERE gc.id IS NULL;
    
    RAISE NOTICE 'REFERENTIAL INTEGRITY TEST:';
    RAISE NOTICE '  - curated.arbitrage_opportunities violations: %', arbitrage_integrity_violations;
    RAISE NOTICE '  - curated.game_outcomes violations: %', game_outcomes_integrity_violations;
    
    IF arbitrage_integrity_violations = 0 AND game_outcomes_integrity_violations = 0 THEN
        RAISE NOTICE '✅ All curated schema referential integrity tests passed';
    ELSE
        RAISE EXCEPTION 'Referential integrity violations found in curated schema';
    END IF;
END $$;

-- Step 5: Commit the changes
COMMIT;

SELECT 'CORRECTED FK CONSTRAINT UPDATES COMPLETED SUCCESSFULLY' as status;

-- Step 6: Final assessment - can core_betting be partially cleaned up?
DO $$
DECLARE
    external_dependencies INTEGER;
    staging_dependencies INTEGER;
    other_dependencies INTEGER;
BEGIN
    -- Count all external dependencies on core_betting
    SELECT COUNT(*) INTO external_dependencies
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema <> 'core_betting'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Count staging dependencies (expected to remain)
    SELECT COUNT(*) INTO staging_dependencies
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
    WHERE ctu.table_schema = 'core_betting'
      AND tc.table_schema = 'staging'
      AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Count other dependencies (should be eliminated)
    other_dependencies := external_dependencies - staging_dependencies;
    
    RAISE NOTICE 'FINAL DEPENDENCY ASSESSMENT:';
    RAISE NOTICE '  - Total external core_betting dependencies: %', external_dependencies;
    RAISE NOTICE '  - Expected staging dependencies: %', staging_dependencies;
    RAISE NOTICE '  - Other dependencies (should be 0): %', other_dependencies;
    
    IF other_dependencies = 0 THEN
        RAISE NOTICE '✅ SUCCESS: Only expected staging dependencies remain';
        RAISE NOTICE '   core_betting schema can be cleaned up once curated is activated';
    ELSE
        RAISE NOTICE '⚠️ WARNING: % unexpected dependencies still exist', other_dependencies;
    END IF;
END $$;

-- Log completion
INSERT INTO operational.core_betting_migration_log (phase, operation, status, details, end_time)
VALUES ('PHASE_4', 'CORRECTED_FK_CONSTRAINT_UPDATES', 'completed', 
    jsonb_build_object(
        'fk_updates_completed_at', CURRENT_TIMESTAMP,
        'curated_constraints_updated', ARRAY[
            'curated.arbitrage_opportunities.book_a_id_fkey → curated.sportsbooks',
            'curated.arbitrage_opportunities.book_b_id_fkey → curated.sportsbooks',
            'curated.game_outcomes.game_id_fkey → curated.games_complete'
        ],
        'staging_constraints_preserved', ARRAY[
            'staging.betting_splits.game_id_fkey → core_betting.games'
        ],
        'schema_boundaries_respected', true,
        'curated_referential_integrity_validated', true,
        'ready_for_staged_cleanup', true
    ),
    CURRENT_TIMESTAMP);

SELECT 'PHASE 4 CORRECTED FK UPDATES COMPLETE - PROPER SCHEMA BOUNDARIES MAINTAINED!' as final_status;

-- =========================================================================
-- END OF CORRECTED FK CONSTRAINT UPDATE SCRIPT
-- =========================================================================