51.14. `pg_conversion`  
---  
[Prev](catalog-pg-constraint.md "51.13. pg_constraint") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-database.md "51.15. pg_database")  
  
* * *

## 51.14. `pg_conversion` #

The catalog `pg_conversion` describes encoding conversion functions. See [CREATE CONVERSION](sql-createconversion.md "CREATE CONVERSION") for more information. 

**Table 51.14.`pg_conversion` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`conname` `name` Conversion name (unique within a namespace)   
`connamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this conversion   
`conowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the conversion   
`conforencoding` `int4` Source encoding ID ([`pg_encoding_to_char()`](functions-info.md#PG-ENCODING-TO-CHAR) can translate this number to the encoding name)   
`contoencoding` `int4` Destination encoding ID ([`pg_encoding_to_char()`](functions-info.md#PG-ENCODING-TO-CHAR) can translate this number to the encoding name)   
`conproc` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  Conversion function   
`condefault` `bool` True if this is the default conversion   
  
  


* * *

[Prev](catalog-pg-constraint.md "51.13. pg_constraint") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-database.md "51.15. pg_database")  
---|---|---  
51.13. `pg_constraint` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.15. `pg_database`
