51.16. `pg_db_role_setting`  
---  
[Prev](catalog-pg-database.md "51.15. pg_database") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-default-acl.md "51.17. pg_default_acl")  
  
* * *

## 51.16. `pg_db_role_setting` #

The catalog `pg_db_role_setting` records the default values that have been set for run-time configuration variables, for each role and database combination. 

Unlike most system catalogs, `pg_db_role_setting` is shared across all databases of a cluster: there is only one copy of `pg_db_role_setting` per cluster, not one per database. 

**Table 51.16.`pg_db_role_setting` Columns**

Column Type  Description   
---  
`setdatabase` `oid` (references [`pg_database`](catalog-pg-database.md "51.15. pg_database").`oid`)  The OID of the database the setting is applicable to, or zero if not database-specific   
`setrole` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  The OID of the role the setting is applicable to, or zero if not role-specific   
`setconfig` `text[]` Defaults for run-time configuration variables   
  
  


* * *

[Prev](catalog-pg-database.md "51.15. pg_database") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-default-acl.md "51.17. pg_default_acl")  
---|---|---  
51.15. `pg_database` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.17. `pg_default_acl`
