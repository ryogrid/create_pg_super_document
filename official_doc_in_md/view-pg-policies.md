52.14. `pg_policies`  
---  
[Prev](view-pg-matviews.md "52.13. pg_matviews") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-prepared-statements.md "52.15. pg_prepared_statements")  
  
* * *

## 52.14. `pg_policies` #

The view `pg_policies` provides access to useful information about each row-level security policy in the database. 

**Table 52.14.`pg_policies` Columns**

Column Type  Description   
---  
`schemaname` `name` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`nspname`)  Name of schema containing table policy is on   
`tablename` `name` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relname`)  Name of table policy is on   
`policyname` `name` (references [`pg_policy`](catalog-pg-policy.md "51.38. pg_policy").`polname`)  Name of policy   
`permissive` `text` Is the policy permissive or restrictive?   
`roles` `name[]` The roles to which this policy applies   
`cmd` `text` The command type to which the policy is applied   
`qual` `text` The expression added to the security barrier qualifications for queries that this policy applies to   
`with_check` `text` The expression added to the WITH CHECK qualifications for queries that attempt to add rows to this table   
  
  


* * *

[Prev](view-pg-matviews.md "52.13. pg_matviews") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-prepared-statements.md "52.15. pg_prepared_statements")  
---|---|---  
52.13. `pg_matviews` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.15. `pg_prepared_statements`
