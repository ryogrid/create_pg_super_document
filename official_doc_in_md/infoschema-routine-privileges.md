35.41. `routine_privileges`  
---  
[Prev](infoschema-routine-column-usage.md "35.40. routine_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routine-routine-usage.md "35.42. routine_routine_usage")  
  
* * *

## 35.41. `routine_privileges` #

The view `routine_privileges` identifies all privileges granted on functions to a currently enabled role or by a currently enabled role. There is one row for each combination of function, grantor, and grantee. 

**Table 35.39.`routine_privileges` Columns**

Column Type  Description   
---  
`grantor` `sql_identifier` Name of the role that granted the privilege   
`grantee` `sql_identifier` Name of the role that the privilege was granted to   
`specific_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the function   
`specific_name` `sql_identifier` The “specific name” of the function. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information.   
`routine_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`routine_schema` `sql_identifier` Name of the schema containing the function   
`routine_name` `sql_identifier` Name of the function (might be duplicated in case of overloading)   
`privilege_type` `character_data` Always `EXECUTE` (the only privilege type for functions)   
`is_grantable` `yes_or_no` `YES` if the privilege is grantable, `NO` if not   
  
  


* * *

[Prev](infoschema-routine-column-usage.md "35.40. routine_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routine-routine-usage.md "35.42. routine_routine_usage")  
---|---|---  
35.40. `routine_column_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.42. `routine_routine_usage`
