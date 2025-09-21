52.35. `pg_views`  
---  
[Prev](view-pg-user-mappings.md "52.34. pg_user_mappings") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-wait-events.md "52.36. pg_wait_events")  
  
* * *

## 52.35. `pg_views` #

The view `pg_views` provides access to useful information about each view in the database. 

**Table 52.35.`pg_views` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing view   
`viewname` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of view   
`viewowner` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of view's owner   
`definition` `text` View definition (a reconstructed [SELECT](sql-select.md "SELECT") query)   
  
  


* * *

[Prev](view-pg-user-mappings.md "52.34. pg_user_mappings") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-wait-events.md "52.36. pg_wait_events")  
---|---|---  
52.34. `pg_user_mappings` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.36. `pg_wait_events`
