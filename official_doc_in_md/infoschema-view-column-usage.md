35.63. `view_column_usage`  
---  
[Prev](infoschema-user-mappings.md "35.62. user_mappings") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-view-routine-usage.md "35.64. view_routine_usage")  
  
* * *

## 35.63. `view_column_usage` #

The view `view_column_usage` identifies all columns that are used in the query expression of a view (the `SELECT` statement that defines the view). A column is only included if the table that contains the column is owned by a currently enabled role. 

### Note

Columns of system tables are not included. This should be fixed sometime. 

**Table 35.61.`view_column_usage` Columns**

Column Type  Description   
---  
`view_catalog` `sql_identifier` Name of the database that contains the view (always the current database)   
`view_schema` `sql_identifier` Name of the schema that contains the view   
`view_name` `sql_identifier` Name of the view   
`table_catalog` `sql_identifier` Name of the database that contains the table that contains the column that is used by the view (always the current database)   
`table_schema` `sql_identifier` Name of the schema that contains the table that contains the column that is used by the view   
`table_name` `sql_identifier` Name of the table that contains the column that is used by the view   
`column_name` `sql_identifier` Name of the column that is used by the view   
  
  


* * *

[Prev](infoschema-user-mappings.md "35.62. user_mappings") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-view-routine-usage.md "35.64. view_routine_usage")  
---|---|---  
35.62. `user_mappings` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.64. `view_routine_usage`
