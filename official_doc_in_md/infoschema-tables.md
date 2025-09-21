35.54. `tables`  
---  
[Prev](infoschema-table-privileges.md "35.53. table_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-transforms.md "35.55. transforms")  
  
* * *

## 35.54. `tables` #

The view `tables` contains all tables and views defined in the current database. Only those tables and views are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.52.`tables` Columns**

Column Type  Description   
---  
`table_catalog` `sql_identifier` Name of the database that contains the table (always the current database)   
`table_schema` `sql_identifier` Name of the schema that contains the table   
`table_name` `sql_identifier` Name of the table   
`table_type` `character_data` Type of the table: `BASE TABLE` for a persistent base table (the normal table type), `VIEW` for a view, `FOREIGN` for a foreign table, or `LOCAL TEMPORARY` for a temporary table   
`self_referencing_column_name` `sql_identifier` Applies to a feature not available in PostgreSQL  
`reference_generation` `character_data` Applies to a feature not available in PostgreSQL  
`user_defined_type_catalog` `sql_identifier` If the table is a typed table, the name of the database that contains the underlying data type (always the current database), else null.   
`user_defined_type_schema` `sql_identifier` If the table is a typed table, the name of the schema that contains the underlying data type, else null.   
`user_defined_type_name` `sql_identifier` If the table is a typed table, the name of the underlying data type, else null.   
`is_insertable_into` `yes_or_no` `YES` if the table is insertable into, `NO` if not (Base tables are always insertable into, views not necessarily.)   
`is_typed` `yes_or_no` `YES` if the table is a typed table, `NO` if not   
`commit_action` `character_data` Not yet implemented   
  
  


* * *

[Prev](infoschema-table-privileges.md "35.53. table_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-transforms.md "35.55. transforms")  
---|---|---  
35.53. `table_privileges` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.55. `transforms`
