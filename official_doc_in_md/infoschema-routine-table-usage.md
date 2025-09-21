35.44. `routine_table_usage`  
---  
[Prev](infoschema-routine-sequence-usage.md "35.43. routine_sequence_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routines.md "35.45. routines")  
  
* * *

## 35.44. `routine_table_usage` #

The view `routine_table_usage` is meant to identify all tables that are used by a function or procedure. This information is currently not tracked by PostgreSQL. 

**Table 35.42.`routine_table_usage` Columns**

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
  
  


* * *

[Prev](infoschema-routine-sequence-usage.md "35.43. routine_sequence_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routines.md "35.45. routines")  
---|---|---  
35.43. `routine_sequence_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.45. `routines`
