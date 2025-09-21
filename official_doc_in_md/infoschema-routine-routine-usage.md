35.42. `routine_routine_usage`  
---  
[Prev](infoschema-routine-privileges.md "35.41. routine_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routine-sequence-usage.md "35.43. routine_sequence_usage")  
  
* * *

## 35.42. `routine_routine_usage` #

The view `routine_routine_usage` identifies all functions or procedures that are used by another (or the same) function or procedure, either in the SQL body or in parameter default expressions. (This only works for unquoted SQL bodies, not quoted bodies or functions in other languages.) An entry is included here only if the used function is owned by a currently enabled role. (There is no such restriction on the using function.) 

Note that the entries for both functions in the view refer to the “specific” name of the routine, even though the column names are used in a way that is inconsistent with other information schema views about routines. This is per SQL standard, although it is arguably a misdesign. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information about specific names. 

**Table 35.40.`routine_routine_usage` Columns**

Column Type  Description   
---  
`specific_catalog` `sql_identifier` Name of the database containing the using function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the using function   
`specific_name` `sql_identifier` The “specific name” of the using function.   
`routine_catalog` `sql_identifier` Name of the database that contains the function that is used by the first function (always the current database)   
`routine_schema` `sql_identifier` Name of the schema that contains the function that is used by the first function   
`routine_name` `sql_identifier` The “specific name” of the function that is used by the first function.   
  
  


* * *

[Prev](infoschema-routine-privileges.md "35.41. routine_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routine-sequence-usage.md "35.43. routine_sequence_usage")  
---|---|---  
35.41. `routine_privileges` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.43. `routine_sequence_usage`
