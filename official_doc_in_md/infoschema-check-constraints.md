35.9. `check_constraints`  
---  
[Prev](infoschema-check-constraint-routine-usage.md "35.8. check_constraint_routine_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-collations.md "35.10. collations")  
  
* * *

## 35.9. `check_constraints` #

The view `check_constraints` contains all check constraints, either defined on a table or on a domain, that are owned by a currently enabled role. (The owner of the table or domain is the owner of the constraint.) 

The SQL standard considers not-null constraints to be check constraints with a `CHECK (_`column_name`_ IS NOT NULL)` expression. So not-null constraints are also included here and don't have a separate view. 

**Table 35.7.`check_constraints` Columns**

Column Type  Description   
---  
`constraint_catalog` `sql_identifier` Name of the database containing the constraint (always the current database)   
`constraint_schema` `sql_identifier` Name of the schema containing the constraint   
`constraint_name` `sql_identifier` Name of the constraint   
`check_clause` `character_data` The check expression of the check constraint   
  
  


* * *

[Prev](infoschema-check-constraint-routine-usage.md "35.8. check_constraint_routine_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-collations.md "35.10. collations")  
---|---|---  
35.8. `check_constraint_routine_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.10. `collations`
