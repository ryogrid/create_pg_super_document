51.25. `pg_foreign_table`  
---  
[Prev](catalog-pg-foreign-server.md "51.24. pg_foreign_server") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-index.md "51.26. pg_index")  
  
* * *

## 51.25. `pg_foreign_table` #

The catalog `pg_foreign_table` contains auxiliary information about foreign tables. A foreign table is primarily represented by a [`pg_class`](catalog-pg-class.md "51.11. pg_class") entry, just like a regular table. Its `pg_foreign_table` entry contains the information that is pertinent only to foreign tables and not any other kind of relation. 

**Table 51.25.`pg_foreign_table` Columns**

Column Type  Description   
---  
`ftrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the [`pg_class`](catalog-pg-class.md "51.11. pg_class") entry for this foreign table   
`ftserver` `oid` (references [`pg_foreign_server`](catalog-pg-foreign-server.md "51.24. pg_foreign_server").`oid`)  OID of the foreign server for this foreign table   
`ftoptions` `text[]` Foreign table options, as “keyword=value” strings   
  
  


* * *

[Prev](catalog-pg-foreign-server.md "51.24. pg_foreign_server") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-index.md "51.26. pg_index")  
---|---|---  
51.24. `pg_foreign_server` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.26. `pg_index`
