35.64. `view_routine_usage`  
---  
[Prev](infoschema-view-column-usage.md "35.63. view_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-view-table-usage.md "35.65. view_table_usage")  
  
* * *

## 35.64. `view_routine_usage` #

The view `view_routine_usage` identifies all routines (functions and procedures) that are used in the query expression of a view (the `SELECT` statement that defines the view). A routine is only included if that routine is owned by a currently enabled role. 

**Table 35.62.`view_routine_usage` Columns**

Column Type  Description   
---  
`table_catalog` `sql_identifier` Name of the database containing the view (always the current database)   
`table_schema` `sql_identifier` Name of the schema containing the view   
`table_name` `sql_identifier` Name of the view   
`specific_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the function   
`specific_name` `sql_identifier` The “specific name” of the function. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information.   
  
  


* * *

[Prev](infoschema-view-column-usage.md "35.63. view_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-view-table-usage.md "35.65. view_table_usage")  
---|---|---  
35.63. `view_column_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.65. `view_table_usage`
