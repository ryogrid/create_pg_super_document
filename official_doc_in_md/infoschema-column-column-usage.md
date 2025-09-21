35.12. `column_column_usage`  
---  
[Prev](infoschema-collation-character-set-applicab.md "35.11. collation_character_set_​applicability") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-column-domain-usage.md "35.13. column_domain_usage")  
  
* * *

## 35.12. `column_column_usage` #

The view `column_column_usage` identifies all generated columns that depend on another base column in the same table. Only tables owned by a currently enabled role are included. 

**Table 35.10.`column_column_usage` Columns**

Column Type  Description   
---  
`table_catalog` `sql_identifier` Name of the database containing the table (always the current database)   
`table_schema` `sql_identifier` Name of the schema containing the table   
`table_name` `sql_identifier` Name of the table   
`column_name` `sql_identifier` Name of the base column that a generated column depends on   
`dependent_column` `sql_identifier` Name of the generated column   
  
  


* * *

[Prev](infoschema-collation-character-set-applicab.md "35.11. collation_character_set_​applicability") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-column-domain-usage.md "35.13. column_domain_usage")  
---|---|---  
35.11. `collation_character_set_​applicability` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.13. `column_domain_usage`
