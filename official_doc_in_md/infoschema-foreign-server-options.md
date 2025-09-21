35.28. `foreign_server_options`  
---  
[Prev](infoschema-foreign-data-wrappers.md "35.27. foreign_data_wrappers") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-foreign-servers.md "35.29. foreign_servers")  
  
* * *

## 35.28. `foreign_server_options` #

The view `foreign_server_options` contains all the options defined for foreign servers in the current database. Only those foreign servers are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.26.`foreign_server_options` Columns**

Column Type  Description   
---  
`foreign_server_catalog` `sql_identifier` Name of the database that the foreign server is defined in (always the current database)   
`foreign_server_name` `sql_identifier` Name of the foreign server   
`option_name` `sql_identifier` Name of an option   
`option_value` `character_data` Value of the option   
  
  


* * *

[Prev](infoschema-foreign-data-wrappers.md "35.27. foreign_data_wrappers") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-foreign-servers.md "35.29. foreign_servers")  
---|---|---  
35.27. `foreign_data_wrappers` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.29. `foreign_servers`
