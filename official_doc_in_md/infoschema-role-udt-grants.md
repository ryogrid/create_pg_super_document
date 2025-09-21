35.38. `role_udt_grants`  
---  
[Prev](infoschema-role-table-grants.md "35.37. role_table_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-role-usage-grants.md "35.39. role_usage_grants")  
  
* * *

## 35.38. `role_udt_grants` #

The view `role_udt_grants` is intended to identify `USAGE` privileges granted on user-defined types where the grantor or grantee is a currently enabled role. Further information can be found under `udt_privileges`. The only effective difference between this view and `udt_privileges` is that this view omits objects that have been made accessible to the current user by way of a grant to `PUBLIC`. Since data types do not have real privileges in PostgreSQL, but only an implicit grant to `PUBLIC`, this view is empty. 

**Table 35.36.`role_udt_grants` Columns**

Column Type  Description   
---  
`grantor` `sql_identifier` The name of the role that granted the privilege   
`grantee` `sql_identifier` The name of the role that the privilege was granted to   
`udt_catalog` `sql_identifier` Name of the database containing the type (always the current database)   
`udt_schema` `sql_identifier` Name of the schema containing the type   
`udt_name` `sql_identifier` Name of the type   
`privilege_type` `character_data` Always `TYPE USAGE`  
`is_grantable` `yes_or_no` `YES` if the privilege is grantable, `NO` if not   
  
  


* * *

[Prev](infoschema-role-table-grants.md "35.37. role_table_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-role-usage-grants.md "35.39. role_usage_grants")  
---|---|---  
35.37. `role_table_grants` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.39. `role_usage_grants`
