51.32. `pg_namespace`  
---  
[Prev](catalog-pg-largeobject-metadata.md "51.31. pg_largeobject_metadata") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-opclass.md "51.33. pg_opclass")  
  
* * *

## 51.32. `pg_namespace` #

The catalog `pg_namespace` stores namespaces. A namespace is the structure underlying SQL schemas: each namespace can have a separate collection of relations, types, etc. without name conflicts. 

**Table 51.32.`pg_namespace` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`nspname` `name` Name of the namespace   
`nspowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the namespace   
`nspacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-largeobject-metadata.md "51.31. pg_largeobject_metadata") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-opclass.md "51.33. pg_opclass")  
---|---|---  
51.31. `pg_largeobject_metadata` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.33. `pg_opclass`
