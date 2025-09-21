35.29. `foreign_servers`  
---  
[Prev](infoschema-foreign-server-options.md "35.28. foreign_server_options") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-foreign-table-options.md "35.30. foreign_table_options")  
  
* * *

## 35.29. `foreign_servers` #

The view `foreign_servers` contains all foreign servers defined in the current database. Only those foreign servers are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.27.`foreign_servers` Columns**

Column Type  Description   
---  
`foreign_server_catalog` `sql_identifier` Name of the database that the foreign server is defined in (always the current database)   
`foreign_server_name` `sql_identifier` Name of the foreign server   
`foreign_data_wrapper_catalog` `sql_identifier` Name of the database that contains the foreign-data wrapper used by the foreign server (always the current database)   
`foreign_data_wrapper_name` `sql_identifier` Name of the foreign-data wrapper used by the foreign server   
`foreign_server_type` `character_data` Foreign server type information, if specified upon creation   
`foreign_server_version` `character_data` Foreign server version information, if specified upon creation   
`authorization_identifier` `sql_identifier` Name of the owner of the foreign server   
  
  


* * *

[Prev](infoschema-foreign-server-options.md "35.28. foreign_server_options") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-foreign-table-options.md "35.30. foreign_table_options")  
---|---|---  
35.28. `foreign_server_options` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.30. `foreign_table_options`
