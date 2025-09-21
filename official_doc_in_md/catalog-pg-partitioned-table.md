51.37. `pg_partitioned_table`  
---  
[Prev](catalog-pg-parameter-acl.md "51.36. pg_parameter_acl") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-policy.md "51.38. pg_policy")  
  
* * *

## 51.37. `pg_partitioned_table` #

The catalog `pg_partitioned_table` stores information about how tables are partitioned. 

**Table 51.37.`pg_partitioned_table` Columns**

Column Type  Description   
---  
`partrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the [`pg_class`](catalog-pg-class.md "51.11. pg_class") entry for this partitioned table   
`partstrat` `char` Partitioning strategy; `h` = hash partitioned table, `l` = list partitioned table, `r` = range partitioned table   
`partnatts` `int2` The number of columns in the partition key   
`partdefid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The OID of the [`pg_class`](catalog-pg-class.md "51.11. pg_class") entry for the default partition of this partitioned table, or zero if this partitioned table does not have a default partition   
`partattrs` `int2vector` (references [`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute").`attnum`)  This is an array of `partnatts` values that indicate which table columns are part of the partition key. For example, a value of `1 3` would mean that the first and the third table columns make up the partition key. A zero in this array indicates that the corresponding partition key column is an expression, rather than a simple column reference.   
`partclass` `oidvector` (references [`pg_opclass`](catalog-pg-opclass.md "51.33. pg_opclass").`oid`)  For each column in the partition key, this contains the OID of the operator class to use. See [`pg_opclass`](catalog-pg-opclass.md "51.33. pg_opclass") for details.   
`partcollation` `oidvector` (references [`pg_collation`](catalog-pg-collation.md "51.12. pg_collation").`oid`)  For each column in the partition key, this contains the OID of the collation to use for partitioning, or zero if the column is not of a collatable data type.   
`partexprs` `pg_node_tree` Expression trees (in `nodeToString()` representation) for partition key columns that are not simple column references. This is a list with one element for each zero entry in `partattrs`. Null if all partition key columns are simple references.   
  
  


* * *

[Prev](catalog-pg-parameter-acl.md "51.36. pg_parameter_acl") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-policy.md "51.38. pg_policy")  
---|---|---  
51.36. `pg_parameter_acl` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.38. `pg_policy`
