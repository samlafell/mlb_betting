-- Final dependency check using system tables for accurate results

SELECT 'FINAL DEPENDENCY VERIFICATION USING SYSTEM TABLES:' as status;

-- Check FK constraints using pg_constraint for accurate results
SELECT 
    n1.nspname as referencing_schema,
    c1.relname as referencing_table,
    con.conname as constraint_name,
    n2.nspname as referenced_schema,
    c2.relname as referenced_table,
    pg_get_constraintdef(con.oid) as constraint_definition
FROM pg_constraint con
JOIN pg_class c1 ON con.conrelid = c1.oid
JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
JOIN pg_class c2 ON con.confrelid = c2.oid  
JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
WHERE con.contype = 'f'
  AND n2.nspname = 'core_betting'
  AND n1.nspname != 'core_betting'
ORDER BY n1.nspname, c1.relname, con.conname;

-- Count remaining dependencies
SELECT 'DEPENDENCY SUMMARY:' as status;

SELECT 
    COUNT(*) as total_external_core_betting_dependencies,
    COUNT(CASE WHEN n1.nspname = 'staging' THEN 1 END) as staging_dependencies,
    COUNT(CASE WHEN n1.nspname != 'staging' THEN 1 END) as other_dependencies
FROM pg_constraint con
JOIN pg_class c1 ON con.conrelid = c1.oid
JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
JOIN pg_class c2 ON con.confrelid = c2.oid  
JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
WHERE con.contype = 'f'
  AND n2.nspname = 'core_betting'
  AND n1.nspname != 'core_betting';

-- Verify curated schema self-references
SELECT 'CURATED SCHEMA INTERNAL REFERENCES:' as status;

SELECT 
    COUNT(*) as curated_internal_fk_count,
    STRING_AGG(c1.relname || '.' || con.conname, ', ') as constraint_list
FROM pg_constraint con
JOIN pg_class c1 ON con.conrelid = c1.oid
JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
JOIN pg_class c2 ON con.confrelid = c2.oid  
JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
WHERE con.contype = 'f'
  AND n1.nspname = 'curated'
  AND n2.nspname = 'curated';