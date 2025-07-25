-- Fix the curated.game_outcomes constraint that didn't update properly

-- Check current constraint
SELECT 'CHECKING CURRENT GAME_OUTCOMES CONSTRAINT:' as status;

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
  AND tc.constraint_name = 'game_outcomes_game_id_fkey';

-- Fix the constraint
BEGIN;

-- Drop the incorrect constraint
ALTER TABLE curated.game_outcomes DROP CONSTRAINT IF EXISTS game_outcomes_game_id_fkey;

-- Add the correct constraint pointing to curated.games_complete
ALTER TABLE curated.game_outcomes 
ADD CONSTRAINT game_outcomes_game_id_fkey 
FOREIGN KEY (game_id) REFERENCES curated.games_complete(id);

-- Validate the fix
SELECT 'VALIDATING FIX:' as status;

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
  AND tc.constraint_name = 'game_outcomes_game_id_fkey';

COMMIT;

SELECT 'GAME_OUTCOMES CONSTRAINT FIXED SUCCESSFULLY' as status;