35.31. `foreign_tables`  
---  
[Prev](infoschema-foreign-table-options.md "35.30. foreign_table_options") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-key-column-usage.md "35.32. key_column_usage")  
  
* * *

## 35.31. `foreign_tables` #

The view `foreign_tables` contains all foreign tables defined in the current database. Only those foreign tables are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.29.`foreign_tables` Columns**

Column Type  Description   
---  
`foreign_table_catalog` `sql_identifier` Name of the database that the foreign table is defined in (always the current database)   
`foreign_table_schema` `sql_identifier` Name of the schema that contains the foreign table   
`foreign_table_name` `sql_identifier` Name of the foreign table   
`foreign_server_catalog` `sql_identifier` Name of the database that the foreign server is defined in (always the current database)   
`foreign_server_name` `sql_identifier` Name of the foreign server   
  
  


* * *

[Prev](infoschema-foreign-table-options.md "35.30. foreign_table_options") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-key-column-usage.md "35.32. key_column_usage")  
---|---|---  
35.30. `foreign_table_options` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.32. `key_column_usage`
