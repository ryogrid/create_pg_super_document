35.5. `applicable_roles`  
---  
[Prev](infoschema-administrable-role-authorizations.md "35.4. administrable_role_​authorizations") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-attributes.md "35.6. attributes")  
  
* * *

## 35.5. `applicable_roles` #

The view `applicable_roles` identifies all roles whose privileges the current user can use. This means there is some chain of role grants from the current user to the role in question. The current user itself is also an applicable role. The set of applicable roles is generally used for permission checking. 

**Table 35.3.`applicable_roles` Columns**

Column Type  Description   
---  
`grantee` `sql_identifier` Name of the role to which this role membership was granted (can be the current user, or a different role in case of nested role memberships)   
`role_name` `sql_identifier` Name of a role   
`is_grantable` `yes_or_no` `YES` if the grantee has the admin option on the role, `NO` if not   
  
  


* * *

[Prev](infoschema-administrable-role-authorizations.md "35.4. administrable_role_​authorizations") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-attributes.md "35.6. attributes")  
---|---|---  
35.4. `administrable_role_​authorizations` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.6. `attributes`
