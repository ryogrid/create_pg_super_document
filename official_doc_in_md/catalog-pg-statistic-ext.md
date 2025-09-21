51.52. `pg_statistic_ext`  
---  
[Prev](catalog-pg-statistic.md "51.51. pg_statistic") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-statistic-ext-data.md "51.53. pg_statistic_ext_data")  
  
* * *

## 51.52. `pg_statistic_ext` #

The catalog `pg_statistic_ext` holds definitions of extended planner statistics. Each row in this catalog corresponds to a _statistics object_ created with [`CREATE STATISTICS`](sql-createstatistics.md "CREATE STATISTICS"). 

**Table 51.52.`pg_statistic_ext` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`stxrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  Table containing the columns described by this object   
`stxname` `name` Name of the statistics object   
`stxnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this statistics object   
`stxowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the statistics object   
`stxkeys` `int2vector` (references [`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute").`attnum`)  An array of attribute numbers, indicating which table columns are covered by this statistics object; for example a value of `1 3` would mean that the first and the third table columns are covered   
`stxstattarget` `int2` `stxstattarget` controls the level of detail of statistics accumulated for this statistics object by [`ANALYZE`](sql-analyze.md "ANALYZE"). A zero value indicates that no statistics should be collected. A null value says to use the maximum of the statistics targets of the referenced columns, if set, or the system default statistics target. Positive values of `stxstattarget` determine the target number of “most common values” to collect.   
`stxkind` `char[]` An array containing codes for the enabled statistics kinds; valid values are: `d` for n-distinct statistics, `f` for functional dependency statistics, `m` for most common values (MCV) list statistics, and `e` for expression statistics   
`stxexprs` `pg_node_tree` Expression trees (in `nodeToString()` representation) for statistics object attributes that are not simple column references. This is a list with one element per expression. Null if all statistics object attributes are simple references.   
  
  


The `pg_statistic_ext` entry is filled in completely during [`CREATE STATISTICS`](sql-createstatistics.md "CREATE STATISTICS"), but the actual statistical values are not computed then. Subsequent [`ANALYZE`](sql-analyze.md "ANALYZE") commands compute the desired values and populate an entry in the [`pg_statistic_ext_data`](catalog-pg-statistic-ext-data.md "51.53. pg_statistic_ext_data") catalog. 

* * *

[Prev](catalog-pg-statistic.md "51.51. pg_statistic") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-statistic-ext-data.md "51.53. pg_statistic_ext_data")  
---|---|---  
51.51. `pg_statistic` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.53. `pg_statistic_ext_data`
