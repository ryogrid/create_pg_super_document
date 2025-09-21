52.13. `pg_matviews`  
---  
[Prev](view-pg-locks.md "52.12. pg_locks") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-policies.md "52.14. pg_policies")  
  
* * *

## 52.13. `pg_matviews` #

The view `pg_matviews` provides access to useful information about each materialized view in the database. 

**Table 52.13.`pg_matviews` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing materialized view   
`matviewname` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of materialized view   
`matviewowner` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of materialized view's owner   
`tablespace` `name` (references [`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace").`spcname`)  Name of tablespace containing materialized view (null if default for database)   
`hasindexes` `bool` True if materialized view has (or recently had) any indexes   
`ispopulated` `bool` True if materialized view is currently populated   
`definition` `text` Materialized view definition (a reconstructed [SELECT](sql-select.md "SELECT") query)   
  
  


* * *

[Prev](view-pg-locks.md "52.12. pg_locks") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-policies.md "52.14. pg_policies")  
---|---|---  
52.12. `pg_locks` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.14. `pg_policies`
