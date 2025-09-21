35.21. `domain_constraints`  
---  
[Prev](infoschema-data-type-privileges.md "35.20. data_type_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-domain-udt-usage.md "35.22. domain_udt_usage")  
  
* * *

## 35.21. `domain_constraints` #

The view `domain_constraints` contains all constraints belonging to domains defined in the current database. Only those domains are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.19.`domain_constraints` Columns**

Column Type  Description   
---  
`constraint_catalog` `sql_identifier` Name of the database that contains the constraint (always the current database)   
`constraint_schema` `sql_identifier` Name of the schema that contains the constraint   
`constraint_name` `sql_identifier` Name of the constraint   
`domain_catalog` `sql_identifier` Name of the database that contains the domain (always the current database)   
`domain_schema` `sql_identifier` Name of the schema that contains the domain   
`domain_name` `sql_identifier` Name of the domain   
`is_deferrable` `yes_or_no` `YES` if the constraint is deferrable, `NO` if not   
`initially_deferred` `yes_or_no` `YES` if the constraint is deferrable and initially deferred, `NO` if not   
  
  


* * *

[Prev](infoschema-data-type-privileges.md "35.20. data_type_privileges") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-domain-udt-usage.md "35.22. domain_udt_usage")  
---|---|---  
35.20. `data_type_privileges` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.22. `domain_udt_usage`
