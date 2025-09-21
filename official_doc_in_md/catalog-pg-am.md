51.3. `pg_am`  
---  
[Prev](catalog-pg-aggregate.md "51.2. pg_aggregate") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-amop.md "51.4. pg_amop")  
  
* * *

## 51.3. `pg_am` #

The catalog `pg_am` stores information about relation access methods. There is one row for each access method supported by the system. Currently, only tables and indexes have access methods. The requirements for table and index access methods are discussed in detail in [Chapter 61](tableam.md "Chapter 61. Table Access Method Interface Definition") and [Chapter 62](indexam.md "Chapter 62. Index Access Method Interface Definition") respectively. 

**Table 51.3.`pg_am` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`amname` `name` Name of the access method   
`amhandler` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of a handler function that is responsible for supplying information about the access method   
`amtype` `char` `t` = table (including materialized views), `i` = index.   
  
  


### Note

Before PostgreSQL 9.6, `pg_am` contained many additional columns representing properties of index access methods. That data is now only directly visible at the C code level. However, `pg_index_column_has_property()` and related functions have been added to allow SQL queries to inspect index access method properties; see [Table 9.74](functions-info.md#FUNCTIONS-INFO-CATALOG-TABLE "Table 9.74. System Catalog Information Functions"). 

* * *

[Prev](catalog-pg-aggregate.md "51.2. pg_aggregate") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-amop.md "51.4. pg_amop")  
---|---|---  
51.2. `pg_aggregate` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.4. `pg_amop`
