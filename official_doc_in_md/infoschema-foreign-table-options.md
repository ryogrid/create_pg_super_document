35.30. `foreign_table_options`  
---  
[Prev](infoschema-foreign-servers.md "35.29. foreign_servers") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-foreign-tables.md "35.31. foreign_tables")  
  
* * *

## 35.30. `foreign_table_options` #

The view `foreign_table_options` contains all the options defined for foreign tables in the current database. Only those foreign tables are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.28.`foreign_table_options` Columns**

Column Type  Description   
---  
`foreign_table_catalog` `sql_identifier` Name of the database that contains the foreign table (always the current database)   
`foreign_table_schema` `sql_identifier` Name of the schema that contains the foreign table   
`foreign_table_name` `sql_identifier` Name of the foreign table   
`option_name` `sql_identifier` Name of an option   
`option_value` `character_data` Value of the option   
  
  


* * *

[Prev](infoschema-foreign-servers.md "35.29. foreign_servers") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-foreign-tables.md "35.31. foreign_tables")  
---|---|---  
35.29. `foreign_servers` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.31. `foreign_tables`
