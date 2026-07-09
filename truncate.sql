SELECT 'TRUNCATE TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ';'
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'your_schema'
  AND c.relkind IN ('r','p')
  AND NOT c.relispartition
ORDER BY c.relname;
