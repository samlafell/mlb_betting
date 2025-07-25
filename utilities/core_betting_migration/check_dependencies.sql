-- Check actual FK dependencies
SELECT 
    tc.table_schema AS referencing_schema,
    tc.table_name AS referencing_table,
    tc.constraint_name,
    ctu.table_name AS referenced_table,
    ctu.table_schema AS referenced_schema
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_table_usage ctu ON tc.constraint_name = ctu.constraint_name
WHERE ctu.table_schema = 'core_betting'
  AND tc.table_schema <> 'core_betting'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_schema, tc.table_name;