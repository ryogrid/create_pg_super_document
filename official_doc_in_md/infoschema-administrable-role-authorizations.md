35.4. `administrable_role_​authorizations`  
---  
[Prev](infoschema-information-schema-catalog-name.md "35.3. information_schema_catalog_name") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-applicable-roles.md "35.5. applicable_roles")  
  
* * *

## 35.4. `administrable_role_​authorizations` #

The view `administrable_role_authorizations` identifies all roles that the current user has the admin option for. 

**Table 35.2.`administrable_role_authorizations` Columns**

Column Type  Description   
---  
`grantee` `sql_identifier` Name of the role to which this role membership was granted (can be the current user, or a different role in case of nested role memberships)   
`role_name` `sql_identifier` Name of a role   
`is_grantable` `yes_or_no` Always `YES`  
  
  


* * *

[Prev](infoschema-information-schema-catalog-name.md "35.3. information_schema_catalog_name") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-applicable-roles.md "35.5. applicable_roles")  
---|---|---  
35.3. `information_schema_catalog_name` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.5. `applicable_roles`
