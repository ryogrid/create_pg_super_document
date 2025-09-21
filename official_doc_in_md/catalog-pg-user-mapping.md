51.65. `pg_user_mapping`  
---  
[Prev](catalog-pg-type.md "51.64. pg_type") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](views.md "Chapter 52. System Views")  
  
* * *

## 51.65. `pg_user_mapping` #

The catalog `pg_user_mapping` stores the mappings from local user to remote. Access to this catalog is restricted from normal users, use the view [`pg_user_mappings`](view-pg-user-mappings.md "52.34. pg_user_mappings") instead. 

**Table 51.66.`pg_user_mapping` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`umuser` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  OID of the local role being mapped, or zero if the user mapping is public   
`umserver` `oid` (references [`pg_foreign_server`](catalog-pg-foreign-server.md "51.24. pg_foreign_server").`oid`)  The OID of the foreign server that contains this mapping   
`umoptions` `text[]` User mapping specific options, as “keyword=value” strings   
  
  


* * *

[Prev](catalog-pg-type.md "51.64. pg_type") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](views.md "Chapter 52. System Views")  
---|---|---  
51.64. `pg_type` | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 52. System Views
