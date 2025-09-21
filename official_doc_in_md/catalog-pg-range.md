51.43. `pg_range`  
---  
[Prev](catalog-pg-publication-rel.md "51.42. pg_publication_rel") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-replication-origin.md "51.44. pg_replication_origin")  
  
* * *

## 51.43. `pg_range` #

The catalog `pg_range` stores information about range types. This is in addition to the types' entries in [`pg_type`](catalog-pg-type.md "51.64. pg_type"). 

**Table 51.43.`pg_range` Columns**

Column Type  Description   
---  
`rngtypid` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  OID of the range type   
`rngsubtype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  OID of the element type (subtype) of this range type   
`rngmultitypid` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  OID of the multirange type for this range type   
`rngcollation` `oid` (references [`pg_collation`](catalog-pg-collation.md "51.12. pg_collation").`oid`)  OID of the collation used for range comparisons, or zero if none   
`rngsubopc` `oid` (references [`pg_opclass`](catalog-pg-opclass.md "51.33. pg_opclass").`oid`)  OID of the subtype's operator class used for range comparisons   
`rngcanonical` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the function to convert a range value into canonical form, or zero if none   
`rngsubdiff` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the function to return the difference between two element values as `double precision`, or zero if none   
  
  


`rngsubopc` (plus `rngcollation`, if the element type is collatable) determines the sort ordering used by the range type. `rngcanonical` is used when the element type is discrete. `rngsubdiff` is optional but should be supplied to improve performance of GiST indexes on the range type. 

* * *

[Prev](catalog-pg-publication-rel.md "51.42. pg_publication_rel") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-replication-origin.md "51.44. pg_replication_origin")  
---|---|---  
51.42. `pg_publication_rel` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.44. `pg_replication_origin`
