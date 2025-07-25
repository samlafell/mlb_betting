-- =========================================================================
-- FK Constraint Analysis - Phase 4 Core Betting Schema Cleanup
-- =========================================================================
-- Purpose: Analyze and map all 17 external FK constraints referencing core_betting
-- Created: 2025-07-25 for Phase 4 execution
-- =========================================================================

-- Step 1: Detailed FK constraint analysis
SELECT 'ANALYZING FK CONSTRAINTS REFERENCING CORE_BETTING' as status;

-- Get detailed information about all FK constraints pointing to core_betting
SELECT 
    tc.table_schema AS referencing_schema,
    tc.table_name AS referencing_table,
    tc.constraint_name,
    ctu.table_name AS referenced_table,
    ctu.table_schema AS referenced_schema,
    
    -- Get column details
    kcu.column_name AS referencing_column,
    ccu.column_name AS referenced_column,
    
    -- Get constraint details
    rc.update_rule,
    rc.delete_rule,
    
    -- Generate mapping to curated schema
    'curated' AS target_schema,
    CASE 
        WHEN ctu.table_name = 'games' THEN 'games_complete'
        WHEN ctu.table_name = 'sportsbooks' THEN 'sportsbooks'
        WHEN ctu.table_name = 'teams' THEN 'teams_master'
        WHEN ctu.table_name = 'sharp_action_indicators' THEN 'sharp_action_indicators'
        WHEN ctu.table_name = 'betting_lines_totals' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'betting_lines_spreads' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'betting_lines_moneyline' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'game_outcomes' THEN 'game_outcomes'
        WHEN ctu.table_name = 'supplementary_games' THEN 'games_complete'
        ELSE ctu.table_name || '_UNMAPPED'
    END AS target_table,
    
    -- Generate update SQL
    'ALTER TABLE ' || tc.table_schema || '.' || tc.table_name || 
    ' DROP CONSTRAINT IF EXISTS ' || tc.constraint_name || ';' AS drop_statement,
    
    'ALTER TABLE ' || tc.table_schema || '.' || tc.table_name || 
    ' ADD CONSTRAINT ' || tc.constraint_name || 
    ' FOREIGN KEY (' || kcu.column_name || ') REFERENCES curated.' ||
    CASE 
        WHEN ctu.table_name = 'games' THEN 'games_complete'
        WHEN ctu.table_name = 'sportsbooks' THEN 'sportsbooks'
        WHEN ctu.table_name = 'teams' THEN 'teams_master'
        WHEN ctu.table_name = 'sharp_action_indicators' THEN 'sharp_action_indicators'
        WHEN ctu.table_name = 'betting_lines_totals' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'betting_lines_spreads' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'betting_lines_moneyline' THEN 'betting_lines_unified'
        WHEN ctu.table_name = 'game_outcomes' THEN 'game_outcomes'
        WHEN ctu.table_name = 'supplementary_games' THEN 'games_complete'
        ELSE ctu.table_name || '_UNMAPPED'
    END ||
    '(' || ccu.column_name || ')' ||
    CASE 
        WHEN rc.update_rule != 'NO ACTION' THEN ' ON UPDATE ' || rc.update_rule
        ELSE ''
    END ||
    CASE 
        WHEN rc.delete_rule != 'NO ACTION' THEN ' ON DELETE ' || rc.delete_rule
        ELSE ''
    END || ';' AS add_statement

FROM information_schema.table_constraints tc
JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
LEFT JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name

WHERE ctu.table_schema = 'core_betting'
  AND tc.table_schema != 'core_betting'
  AND tc.constraint_type = 'FOREIGN KEY'

ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;

-- Step 2: Summary by schema
SELECT 'FK CONSTRAINTS BY SCHEMA' as status;

SELECT 
    tc.table_schema AS schema_name,
    COUNT(*) AS constraint_count,
    STRING_AGG(DISTINCT ctu.table_name, ', ') AS referenced_tables
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
WHERE ctu.table_schema = 'core_betting'
  AND tc.table_schema != 'core_betting'
  AND tc.constraint_type = 'FOREIGN KEY'
GROUP BY tc.table_schema
ORDER BY constraint_count DESC;

-- Step 3: Verify curated schema target tables exist
SELECT 'VERIFYING CURATED SCHEMA TARGET TABLES' as status;

SELECT 
    'curated.games_complete' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'games_complete') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    (SELECT COUNT(*) FROM curated.games_complete) AS record_count
UNION ALL
SELECT 
    'curated.sportsbooks' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'sportsbooks') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    (SELECT COUNT(*) FROM curated.sportsbooks) AS record_count
UNION ALL
SELECT 
    'curated.teams_master' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'teams_master') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    (SELECT COUNT(*) FROM curated.teams_master) AS record_count
UNION ALL
SELECT 
    'curated.game_outcomes' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'game_outcomes') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    (SELECT COUNT(*) FROM curated.game_outcomes) AS record_count
UNION ALL
SELECT 
    'curated.betting_lines_unified' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'betting_lines_unified') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    (SELECT COUNT(*) FROM curated.betting_lines_unified) AS record_count
UNION ALL
SELECT 
    'curated.sharp_action_indicators' AS target_table,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'curated' AND table_name = 'sharp_action_indicators') 
         THEN '✅ EXISTS' ELSE '❌ MISSING' END AS status,
    COALESCE((SELECT COUNT(*) FROM curated.sharp_action_indicators), 0) AS record_count;

-- Step 4: Check for potential data mismatches before FK updates
SELECT 'CHECKING FOR POTENTIAL DATA MISMATCHES' as status;

-- This would identify any rows that might fail FK constraint updates
-- Note: This is a template - specific checks would need actual table analysis

DO $$
BEGIN
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'FK CONSTRAINT ANALYSIS COMPLETE';
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Review the detailed FK constraint mapping above';
    RAISE NOTICE '2. Verify all target tables exist in curated schema';
    RAISE NOTICE '3. Execute the FK update script with proper validation';
    RAISE NOTICE '4. Test referential integrity after updates';
    RAISE NOTICE '=================================================================';
END $$;