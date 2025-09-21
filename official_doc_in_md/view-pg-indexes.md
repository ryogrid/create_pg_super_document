52.11. `pg_indexes`  
---  
[Prev](view-pg-ident-file-mappings.md "52.10. pg_ident_file_mappings") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-locks.md "52.12. pg_locks")  
  
* * *

## 52.11. `pg_indexes` #

The view `pg_indexes` provides access to useful information about each index in the database. 

**Table 52.11.`pg_indexes` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing table and index   
`tablename` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of table the index is for   
`indexname` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of index   
`tablespace` `name` (references [`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace").`spcname`)  Name of tablespace containing index (null if default for database)   
`indexdef` `text` Index definition (a reconstructed [CREATE INDEX](sql-createindex.md "CREATE INDEX") command)   
  
  


* * *

[Prev](view-pg-ident-file-mappings.md "52.10. pg_ident_file_mappings") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-locks.md "52.12. pg_locks")  
---|---|---  
52.10. `pg_ident_file_mappings` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.12. `pg_locks`
