52.30. `pg_tables`  
---  
[Prev](view-pg-stats-ext-exprs.md "52.29. pg_stats_ext_exprs") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs")  
  
* * *

## 52.30. `pg_tables` #

The view `pg_tables` provides access to useful information about each table in the database. 

**Table 52.30.`pg_tables` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing table   
`tablename` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of table   
`tableowner` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of table's owner   
`tablespace` `name` (references [`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace").`spcname`)  Name of tablespace containing table (null if default for database)   
`hasindexes` `bool` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relhasindex`)  True if table has (or recently had) any indexes   
`hasrules` `bool` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relhasrules`)  True if table has (or once had) rules   
`hastriggers` `bool` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relhastriggers`)  True if table has (or once had) triggers   
`rowsecurity` `bool` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relrowsecurity`)  True if row security is enabled on the table   
  
  


* * *

[Prev](view-pg-stats-ext-exprs.md "52.29. pg_stats_ext_exprs") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs")  
---|---|---  
52.29. `pg_stats_ext_exprs` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.31. `pg_timezone_abbrevs`
