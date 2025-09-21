51.56. `pg_tablespace`  
---  
[Prev](catalog-pg-subscription-rel.md "51.55. pg_subscription_rel") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-transform.md "51.57. pg_transform")  
  
* * *

## 51.56. `pg_tablespace` #

The catalog `pg_tablespace` stores information about the available tablespaces. Tables can be placed in particular tablespaces to aid administration of disk layout. 

Unlike most system catalogs, `pg_tablespace` is shared across all databases of a cluster: there is only one copy of `pg_tablespace` per cluster, not one per database. 

**Table 51.56.`pg_tablespace` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`spcname` `name` Tablespace name   
`spcowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the tablespace, usually the user who created it   
`spcacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
`spcoptions` `text[]` Tablespace-level options, as “keyword=value” strings   
  
  


* * *

[Prev](catalog-pg-subscription-rel.md "51.55. pg_subscription_rel") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-transform.md "51.57. pg_transform")  
---|---|---  
51.55. `pg_subscription_rel` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.57. `pg_transform`
