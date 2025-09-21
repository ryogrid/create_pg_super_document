35.52. `table_constraints`  
---  
[Prev](infoschema-sql-sizing.md "35.51. sql_sizing") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-table-privileges.md "35.53. table_privileges")  
  
* * *

## 35.52. `table_constraints` #

The view `table_constraints` contains all constraints belonging to tables that the current user owns or has some privilege other than `SELECT` on. 

**Table 35.50.`table_constraints` Columns**

Column Type  Description   
---  
`constraint_catalog` `sql_identifier` Name of the database that contains the constraint (always the current database)   
`constraint_schema` `sql_identifier` Name of the schema that contains the constraint   
`constraint_name` `sql_identifier` Name of the constraint   
`table_catalog` `sql_identifier` Name of the database that contains the table (always the current database)   
`table_schema` `sql_identifier` Name of the schema that contains the table   
`table_name` `sql_identifier` Name of the table   
`constraint_type` `character_data` Type of the constraint: `CHECK` (includes not-null constraints), `FOREIGN KEY`, `PRIMARY KEY`, or `UNIQUE`  
`is_deferrable` `yes_or_no` `YES` if the constraint is deferrable, `NO` if not   
`initially_deferred` `yes_or_no` `YES` if the constraint is deferrable and initially deferred, `NO` if not   
`enforced` `yes_or_no` Applies to a feature not available in PostgreSQL (currently always `YES`)   
`nulls_distinct` `yes_or_no` If the constraint is a unique constraint, then `YES` if the constraint treats nulls as distinct or `NO` if it treats nulls as not distinct, otherwise null for other types of constraints.   
  
  


* * *

[Prev](infoschema-sql-sizing.md "35.51. sql_sizing") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-table-privileges.md "35.53. table_privileges")  
---|---|---  
35.51. `sql_sizing` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.53. `table_privileges`
