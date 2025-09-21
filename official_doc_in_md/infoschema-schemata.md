35.46. `schemata`  
---  
[Prev](infoschema-routines.md "35.45. routines") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-sequences.md "35.47. sequences")  
  
* * *

## 35.46. `schemata` #

The view `schemata` contains all schemas in the current database that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.44.`schemata` Columns**

Column Type  Description   
---  
`catalog_name` `sql_identifier` Name of the database that the schema is contained in (always the current database)   
`schema_name` `sql_identifier` Name of the schema   
`schema_owner` `sql_identifier` Name of the owner of the schema   
`default_character_set_catalog` `sql_identifier` Applies to a feature not available in PostgreSQL  
`default_character_set_schema` `sql_identifier` Applies to a feature not available in PostgreSQL  
`default_character_set_name` `sql_identifier` Applies to a feature not available in PostgreSQL  
`sql_path` `character_data` Applies to a feature not available in PostgreSQL  
  
  


* * *

[Prev](infoschema-routines.md "35.45. routines") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-sequences.md "35.47. sequences")  
---|---|---  
35.45. `routines` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.47. `sequences`
