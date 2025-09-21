51.31. `pg_largeobject_metadata`  
---  
[Prev](catalog-pg-largeobject.md "51.30. pg_largeobject") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-namespace.md "51.32. pg_namespace")  
  
* * *

## 51.31. `pg_largeobject_metadata` #

The catalog `pg_largeobject_metadata` holds metadata associated with large objects. The actual large object data is stored in [`pg_largeobject`](catalog-pg-largeobject.md "51.30. pg_largeobject"). 

**Table 51.31.`pg_largeobject_metadata` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`lomowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the large object   
`lomacl` `aclitem[]` Access privileges; see [Section 5.8](ddl-priv.md "5.8. Privileges") for details   
  
  


* * *

[Prev](catalog-pg-largeobject.md "51.30. pg_largeobject") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-namespace.md "51.32. pg_namespace")  
---|---|---  
51.30. `pg_largeobject` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.32. `pg_namespace`
