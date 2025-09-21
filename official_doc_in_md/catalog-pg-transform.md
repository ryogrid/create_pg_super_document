51.57. `pg_transform`  
---  
[Prev](catalog-pg-tablespace.md "51.56. pg_tablespace") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-trigger.md "51.58. pg_trigger")  
  
* * *

## 51.57. `pg_transform` #

The catalog `pg_transform` stores information about transforms, which are a mechanism to adapt data types to procedural languages. See [CREATE TRANSFORM](sql-createtransform.md "CREATE TRANSFORM") for more information. 

**Table 51.57.`pg_transform` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`trftype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  OID of the data type this transform is for   
`trflang` `oid` (references [`pg_language`](catalog-pg-language.md "51.29. pg_language").`oid`)  OID of the language this transform is for   
`trffromsql` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  The OID of the function to use when converting the data type for input to the procedural language (e.g., function parameters). Zero is stored if the default behavior should be used.   
`trftosql` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  The OID of the function to use when converting output from the procedural language (e.g., return values) to the data type. Zero is stored if the default behavior should be used.   
  
  


* * *

[Prev](catalog-pg-tablespace.md "51.56. pg_tablespace") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-trigger.md "51.58. pg_trigger")  
---|---|---  
51.56. `pg_tablespace` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.58. `pg_trigger`
