35.26. `foreign_data_wrapper_options`  
---  
[Prev](infoschema-enabled-roles.md "35.25. enabled_roles") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-foreign-data-wrappers.md "35.27. foreign_data_wrappers")  
  
* * *

## 35.26. `foreign_data_wrapper_options` #

The view `foreign_data_wrapper_options` contains all the options defined for foreign-data wrappers in the current database. Only those foreign-data wrappers are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.24.`foreign_data_wrapper_options` Columns**

Column Type  Description   
---  
`foreign_data_wrapper_catalog` `sql_identifier` Name of the database that the foreign-data wrapper is defined in (always the current database)   
`foreign_data_wrapper_name` `sql_identifier` Name of the foreign-data wrapper   
`option_name` `sql_identifier` Name of an option   
`option_value` `character_data` Value of the option   
  
  


* * *

[Prev](infoschema-enabled-roles.md "35.25. enabled_roles") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-foreign-data-wrappers.md "35.27. foreign_data_wrappers")  
---|---|---  
35.25. `enabled_roles` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.27. `foreign_data_wrappers`
