35.55. `transforms`  
---  
[Prev](infoschema-tables.md "35.54. tables") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-triggered-update-columns.md "35.56. triggered_update_columns")  
  
* * *

## 35.55. `transforms` #

The view `transforms` contains information about the transforms defined in the current database. More precisely, it contains a row for each function contained in a transform (the “from SQL” or “to SQL” function). 

**Table 35.53.`transforms` Columns**

Column Type  Description   
---  
`udt_catalog` `sql_identifier` Name of the database that contains the type the transform is for (always the current database)   
`udt_schema` `sql_identifier` Name of the schema that contains the type the transform is for   
`udt_name` `sql_identifier` Name of the type the transform is for   
`specific_catalog` `sql_identifier` Name of the database containing the function (always the current database)   
`specific_schema` `sql_identifier` Name of the schema containing the function   
`specific_name` `sql_identifier` The “specific name” of the function. See [Section 35.45](infoschema-routines.md "35.45. routines") for more information.   
`group_name` `sql_identifier` The SQL standard allows defining transforms in “groups”, and selecting a group at run time. PostgreSQL does not support this. Instead, transforms are specific to a language. As a compromise, this field contains the language the transform is for.   
`transform_type` `character_data` `FROM SQL` or `TO SQL`  
  
  


* * *

[Prev](infoschema-tables.md "35.54. tables") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-triggered-update-columns.md "35.56. triggered_update_columns")  
---|---|---  
35.54. `tables` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.56. `triggered_update_columns`
