35.62. `user_mappings`  
---  
[Prev](infoschema-user-mapping-options.md "35.61. user_mapping_options") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-view-column-usage.md "35.63. view_column_usage")  
  
* * *

## 35.62. `user_mappings` #

The view `user_mappings` contains all user mappings defined in the current database. Only those user mappings are shown where the current user has access to the corresponding foreign server (by way of being the owner or having some privilege). 

**Table 35.60.`user_mappings` Columns**

Column Type  Description   
---  
`authorization_identifier` `sql_identifier` Name of the user being mapped, or `PUBLIC` if the mapping is public   
`foreign_server_catalog` `sql_identifier` Name of the database that the foreign server used by this mapping is defined in (always the current database)   
`foreign_server_name` `sql_identifier` Name of the foreign server used by this mapping   
  
  


* * *

[Prev](infoschema-user-mapping-options.md "35.61. user_mapping_options") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-view-column-usage.md "35.63. view_column_usage")  
---|---|---  
35.61. `user_mapping_options` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.63. `view_column_usage`
