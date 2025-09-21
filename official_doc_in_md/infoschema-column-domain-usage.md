35.13. `column_domain_usage`  
---  
[Prev](infoschema-column-column-usage.md "35.12. column_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-column-options.md "35.14. column_options")  
  
* * *

## 35.13. `column_domain_usage` #

The view `column_domain_usage` identifies all columns (of a table or a view) that make use of some domain defined in the current database and owned by a currently enabled role. 

**Table 35.11.`column_domain_usage` Columns**

Column Type  Description   
---  
`domain_catalog` `sql_identifier` Name of the database containing the domain (always the current database)   
`domain_schema` `sql_identifier` Name of the schema containing the domain   
`domain_name` `sql_identifier` Name of the domain   
`table_catalog` `sql_identifier` Name of the database containing the table (always the current database)   
`table_schema` `sql_identifier` Name of the schema containing the table   
`table_name` `sql_identifier` Name of the table   
`column_name` `sql_identifier` Name of the column   
  
  


* * *

[Prev](infoschema-column-column-usage.md "35.12. column_column_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-column-options.md "35.14. column_options")  
---|---|---  
35.12. `column_column_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.14. `column_options`
