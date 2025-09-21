35.10. `collations`  
---  
[Prev](infoschema-check-constraints.md "35.9. check_constraints") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-collation-character-set-applicab.md "35.11. collation_character_set_​applicability")  
  
* * *

## 35.10. `collations` #

The view `collations` contains the collations available in the current database. 

**Table 35.8.`collations` Columns**

Column Type  Description   
---  
`collation_catalog` `sql_identifier` Name of the database containing the collation (always the current database)   
`collation_schema` `sql_identifier` Name of the schema containing the collation   
`collation_name` `sql_identifier` Name of the default collation   
`pad_attribute` `character_data` Always `NO PAD` (The alternative `PAD SPACE` is not supported by PostgreSQL.)   
  
  


* * *

[Prev](infoschema-check-constraints.md "35.9. check_constraints") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-collation-character-set-applicab.md "35.11. collation_character_set_​applicability")  
---|---|---  
35.9. `check_constraints` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.11. `collation_character_set_​applicability`
