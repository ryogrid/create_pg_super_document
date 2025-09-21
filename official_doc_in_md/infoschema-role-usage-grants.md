35.39. `role_usage_grants`  
---  
[Prev](infoschema-role-udt-grants.md "35.38. role_udt_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-routine-column-usage.md "35.40. routine_column_usage")  
  
* * *

## 35.39. `role_usage_grants` #

The view `role_usage_grants` identifies `USAGE` privileges granted on various kinds of objects where the grantor or grantee is a currently enabled role. Further information can be found under `usage_privileges`. The only effective difference between this view and `usage_privileges` is that this view omits objects that have been made accessible to the current user by way of a grant to `PUBLIC`. 

**Table 35.37.`role_usage_grants` Columns**

Column Type  Description   
---  
`grantor` `sql_identifier` The name of the role that granted the privilege   
`grantee` `sql_identifier` The name of the role that the privilege was granted to   
`object_catalog` `sql_identifier` Name of the database containing the object (always the current database)   
`object_schema` `sql_identifier` Name of the schema containing the object, if applicable, else an empty string   
`object_name` `sql_identifier` Name of the object   
`object_type` `character_data` `COLLATION` or `DOMAIN` or `FOREIGN DATA WRAPPER` or `FOREIGN SERVER` or `SEQUENCE`  
`privilege_type` `character_data` Always `USAGE`  
`is_grantable` `yes_or_no` `YES` if the privilege is grantable, `NO` if not   
  
  


* * *

[Prev](infoschema-role-udt-grants.md "35.38. role_udt_grants") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-routine-column-usage.md "35.40. routine_column_usage")  
---|---|---  
35.38. `role_udt_grants` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.40. `routine_column_usage`
