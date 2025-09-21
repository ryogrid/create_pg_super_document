51.47. `pg_sequence`  
---  
[Prev](catalog-pg-seclabel.md "51.46. pg_seclabel") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-shdepend.md "51.48. pg_shdepend")  
  
* * *

## 51.47. `pg_sequence` #

The catalog `pg_sequence` contains information about sequences. Some of the information about sequences, such as the name and the schema, is in [`pg_class`](catalog-pg-class.md "51.11. pg_class")

**Table 51.47.`pg_sequence` Columns**

Column Type  Description   
---  
`seqrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the [`pg_class`](catalog-pg-class.md "51.11. pg_class") entry for this sequence   
`seqtypid` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Data type of the sequence   
`seqstart` `int8` Start value of the sequence   
`seqincrement` `int8` Increment value of the sequence   
`seqmax` `int8` Maximum value of the sequence   
`seqmin` `int8` Minimum value of the sequence   
`seqcache` `int8` Cache size of the sequence   
`seqcycle` `bool` Whether the sequence cycles   
  
  


* * *

[Prev](catalog-pg-seclabel.md "51.46. pg_seclabel") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-shdepend.md "51.48. pg_shdepend")  
---|---|---  
51.46. `pg_seclabel` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.48. `pg_shdepend`
