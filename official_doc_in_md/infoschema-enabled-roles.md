35.25. `enabled_roles`  
---  
[Prev](infoschema-element-types.md "35.24. element_types") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-foreign-data-wrapper-options.md "35.26. foreign_data_wrapper_options")  
  
* * *

## 35.25. `enabled_roles` #

The view `enabled_roles` identifies the currently “enabled roles”. The enabled roles are recursively defined as the current user together with all roles that have been granted to the enabled roles with automatic inheritance. In other words, these are all roles that the current user has direct or indirect, automatically inheriting membership in. 

For permission checking, the set of “applicable roles” is applied, which can be broader than the set of enabled roles. So generally, it is better to use the view `applicable_roles` instead of this one; See [Section 35.5](infoschema-applicable-roles.md "35.5. applicable_roles") for details on `applicable_roles` view. 

**Table 35.23.`enabled_roles` Columns**

Column Type  Description   
---  
`role_name` `sql_identifier` Name of a role   
  
  


* * *

[Prev](infoschema-element-types.md "35.24. element_types") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-foreign-data-wrapper-options.md "35.26. foreign_data_wrapper_options")  
---|---|---  
35.24. `element_types` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.26. `foreign_data_wrapper_options`
