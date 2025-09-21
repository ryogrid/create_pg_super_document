35.40. `routine_column_usage`  
---  
[Prev](infoschema-role-usage-grants.md "35.39. role_usage_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routine-privileges.md "35.41. routine_privileges")  
  
* * *

## 35.40. `routine_column_usage` #

The view `routine_column_usage` identifies all columns that are used by a function or procedure, either in the SQL body or in parameter default expressions. (This only works for unquoted SQL bodies, not quoted bodies or functions in other languages.) A column is only included if its table is owned by a currently enabled role. 

**Table 35.38.`routine_column_usage` Columns**

Column Type  Description   
---  
`specific_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the function   
`specific_name` `sql_identifier` The “specific name” of the function. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information.   
`routine_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`routine_schema` `sql_identifier` Name of the schema containing the function   
`routine_name` `sql_identifier` Name of the function (might be duplicated in case of overloading)   
`table_catalog` `sql_identifier` Name of the database that contains the table that is used by the function (always the current database)   
`table_schema` `sql_identifier` Name of the schema that contains the table that is used by the function   
`table_name` `sql_identifier` Name of the table that is used by the function   
`column_name` `sql_identifier` Name of the column that is used by the function   
  
  


* * *

[Prev](infoschema-role-usage-grants.md "35.39. role_usage_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routine-privileges.md "35.41. routine_privileges")  
---|---|---  
35.39. `role_usage_grants` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.41. `routine_privileges`
