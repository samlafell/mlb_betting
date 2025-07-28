-- Fix games_complete table ID column to use SERIAL auto-increment
-- This resolves the NULL ID constraint violation error

-- First, check the current state
SELECT 'Current games_complete table structure:' as status;
\d curated.games_complete;

-- Create a sequence for the ID column if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS curated.games_complete_id_seq;

-- Get the current maximum ID to start the sequence from the right place
DO $$
DECLARE
    max_id INTEGER;
BEGIN
    SELECT COALESCE(MAX(id), 0) + 1 INTO max_id FROM curated.games_complete;
    EXECUTE format('ALTER SEQUENCE curated.games_complete_id_seq RESTART WITH %s', max_id);
    RAISE NOTICE 'Set sequence to start from %', max_id;
END $$;

-- Update the ID column to use the sequence as default
ALTER TABLE curated.games_complete 
    ALTER COLUMN id SET DEFAULT nextval('curated.games_complete_id_seq');

-- Make sure the sequence is owned by the column
ALTER SEQUENCE curated.games_complete_id_seq OWNED BY curated.games_complete.id;

-- Verify the fix
SELECT 'Updated games_complete table structure:' as status;
\d curated.games_complete;

-- Test the auto-increment by checking the column default
SELECT column_name, column_default 
FROM information_schema.columns 
WHERE table_schema = 'curated' 
  AND table_name = 'games_complete' 
  AND column_name = 'id';

SELECT 'Fix completed successfully! ID column now auto-increments.' as status;