52.34. `pg_user_mappings`  
---  
[Prev](view-pg-user.md "52.33. pg_user") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-views.md "52.35. pg_views")  
  
* * *

## 52.34. `pg_user_mappings` #

The view `pg_user_mappings` provides access to information about user mappings. This is essentially a publicly readable view of [`pg_user_mapping`](catalog-pg-user-mapping.md "51.65. pg_user_mapping") that leaves out the options field if the user has no rights to use it. 

**Table 52.34.`pg_user_mappings` Columns**

Column Type  Description   
---  
`umid` `oid` (references [`pg_user_mapping`](catalog-pg-user-mapping.md "51.65. pg_user_mapping").`oid`)  OID of the user mapping   
`srvid` `oid` (references [`pg_foreign_server`](catalog-pg-foreign-server.md "51.24. pg_foreign_server").`oid`)  The OID of the foreign server that contains this mapping   
`srvname` `name` (references [`pg_foreign_server`](catalog-pg-foreign-server.md "51.24. pg_foreign_server").`srvname`)  Name of the foreign server   
`umuser` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  OID of the local role being mapped, or zero if the user mapping is public   
`usename` `name` Name of the local user to be mapped   
`umoptions` `text[]` User mapping specific options, as “keyword=value” strings   
  
  


To protect password information stored as a user mapping option, the `umoptions` column will read as null unless one of the following applies: 

  * current user is the user being mapped, and owns the server or holds `USAGE` privilege on it 

  * current user is the server owner and mapping is for `PUBLIC`

  * current user is a superuser 




* * *

[Prev](view-pg-user.md "52.33. pg_user") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-views.md "52.35. pg_views")  
---|---|---  
52.33. `pg_user` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.35. `pg_views`
