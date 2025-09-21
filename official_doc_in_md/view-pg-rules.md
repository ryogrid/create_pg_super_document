52.21. `pg_rules`  
---  
[Prev](view-pg-roles.md "52.20. pg_roles") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-seclabels.md "52.22. pg_seclabels")  
  
* * *

## 52.21. `pg_rules` #

The view `pg_rules` provides access to useful information about query rewrite rules. 

**Table 52.21.`pg_rules` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing table   
`tablename` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of table the rule is for   
`rulename` `name` (references [`pg_rewrite`](catalog-pg-rewrite.md "51.45. pg_rewrite").`rulename`)  Name of rule   
`definition` `text` Rule definition (a reconstructed creation command)   
  
  


The `pg_rules` view excludes the `ON SELECT` rules of views and materialized views; those can be seen in [`pg_views`](view-pg-views.md "52.35. pg_views") and [`pg_matviews`](view-pg-matviews.md "52.13. pg_matviews"). 

* * *

[Prev](view-pg-roles.md "52.20. pg_roles") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-seclabels.md "52.22. pg_seclabels")  
---|---|---  
52.20. `pg_roles` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.22. `pg_seclabels`
