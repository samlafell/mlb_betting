-- Final constraint fix for game_outcomes

-- Drop all existing FK constraints on game_outcomes
ALTER TABLE curated.game_outcomes DROP CONSTRAINT IF EXISTS game_outcomes_game_id_fkey CASCADE;

-- Add the correct constraint
ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Verify the final state
SELECT 'FINAL CONSTRAINT CHECK:' as status;

SELECT 
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    ctu.table_schema as target_schema,
    ctu.table_name as target_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
WHERE tc.table_schema = 'curated' 
  AND tc.table_name = 'game_outcomes'
  AND tc.constraint_type = 'FOREIGN KEY';

-- Check final dependency count
SELECT 'FINAL DEPENDENCY COUNT:' as status;

SELECT COUNT(*) as remaining_core_betting_dependencies
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
WHERE ctu.table_schema = 'core_betting'
  AND tc.table_schema <> 'core_betting'
  AND tc.constraint_type = 'FOREIGN KEY';