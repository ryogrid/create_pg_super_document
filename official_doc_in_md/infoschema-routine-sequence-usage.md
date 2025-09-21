35.43. `routine_sequence_usage`  
---  
[Prev](infoschema-routine-routine-usage.md "35.42. routine_routine_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routine-table-usage.md "35.44. routine_table_usage")  
  
* * *

## 35.43. `routine_sequence_usage` #

The view `routine_sequence_usage` identifies all sequences that are used by a function or procedure, either in the SQL body or in parameter default expressions. (This only works for unquoted SQL bodies, not quoted bodies or functions in other languages.) A sequence is only included if that sequence is owned by a currently enabled role. 

**Table 35.41.`routine_sequence_usage` Columns**

Column Type  Description   
---  
`specific_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the function   
`specific_name` `sql_identifier` The “specific name” of the function. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information.   
`routine_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`routine_schema` `sql_identifier` Name of the schema containing the function   
`routine_name` `sql_identifier` Name of the function (might be duplicated in case of overloading)   
`schema_catalog` `sql_identifier` Name of the database that contains the sequence that is used by the function (always the current database)   
`sequence_schema` `sql_identifier` Name of the schema that contains the sequence that is used by the function   
`sequence_name` `sql_identifier` Name of the sequence that is used by the function   
  
  


* * *

[Prev](infoschema-routine-routine-usage.md "35.42. routine_routine_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routine-table-usage.md "35.44. routine_table_usage")  
---|---|---  
35.42. `routine_routine_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.44. `routine_table_usage`
