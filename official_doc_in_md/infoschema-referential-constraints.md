35.34. `referential_constraints`  
---  
[Prev](infoschema-parameters.md "35.33. parameters") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-role-column-grants.md "35.35. role_column_grants")  
  
* * *

## 35.34. `referential_constraints` #

The view `referential_constraints` contains all referential (foreign key) constraints in the current database. Only those constraints are shown for which the current user has write access to the referencing table (by way of being the owner or having some privilege other than `SELECT`). 

**Table 35.32.`referential_constraints` Columns**

Column Type  Description   
---  
`constraint_catalog` `sql_identifier` Name of the database containing the constraint (always the current database)   
`constraint_schema` `sql_identifier` Name of the schema containing the constraint   
`constraint_name` `sql_identifier` Name of the constraint   
`unique_constraint_catalog` `sql_identifier` Name of the database that contains the unique or primary key constraint that the foreign key constraint references (always the current database)   
`unique_constraint_schema` `sql_identifier` Name of the schema that contains the unique or primary key constraint that the foreign key constraint references   
`unique_constraint_name` `sql_identifier` Name of the unique or primary key constraint that the foreign key constraint references   
`match_option` `character_data` Match option of the foreign key constraint: `FULL`, `PARTIAL`, or `NONE`.   
`update_rule` `character_data` Update rule of the foreign key constraint: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, or `NO ACTION`.   
`delete_rule` `character_data` Delete rule of the foreign key constraint: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, or `NO ACTION`.   
  
  


* * *

[Prev](infoschema-parameters.md "35.33. parameters") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-role-column-grants.md "35.35. role_column_grants")  
---|---|---  
35.33. `parameters` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.35. `role_column_grants`
