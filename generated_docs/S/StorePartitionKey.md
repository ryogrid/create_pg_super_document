# StorePartitionKey

## Location
[src/backend/catalog/heap.c:3376-3500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L3376-L3500)

## Overview
Stores partition key information for a partitioned table into the pg_partitioned_table system catalog, establishing the necessary metadata and dependencies for table partitioning.

## Definition
```c
void StorePartitionKey(Relation rel,
                      char strategy,
                      int16 partnatts,
                      AttrNumber *partattrs,
                      List *partexprs,
                      Oid *partopclass,
                      Oid *partcollation)
```

## Detailed Description
This function creates a complete catalog entry for a partitioned table's partition key specification. It handles all aspects of partition key storage including:

1. **Catalog storage**: Creates a new entry in pg_partitioned_table with all partition key details
2. **Expression handling**: Converts partition expressions to text representation for storage
3. **Dependency management**: Records dependencies on operator classes, collations, columns, and expressions
4. **Cache invalidation**: Ensures relation cache is updated with new partition information

The function properly handles both column-based and expression-based partition keys, setting up internal dependencies for columns and normal dependencies for external objects like functions and operators.

## Parameters / Member Variables
- `rel`: The partitioned table relation (must be RELKIND_PARTITIONED_TABLE)
- `strategy`: Partitioning strategy character ('r' for range, 'l' for list, 'h' for hash)
- `partnatts`: Number of partition key attributes/expressions
- `partattrs`: Array of partition attribute numbers (0 for expressions)
- `partexprs`: List of partition expressions (NULL if none)
- `partopclass`: Array of operator class OIDs for each partition key
- `partcollation`: Array of collation OIDs for each partition key

## Dependencies
- Functions called/Symbols referenced:
  - [buildint2vector](../b/buildint2vector.md)
  - [buildoidvector](../b/buildoidvector.md)
  - [nodeToString](../n/nodeToString.md)
  - CStringGetTextDatum
  - [table_open](../t/table_open.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [table_close](../t/table_close.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - ObjectAddressSet
  - ObjectAddressSubSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)

## Notes and Other Information
- Only works with relations of kind RELKIND_PARTITIONED_TABLE
- The partdefid field is initially set to InvalidOid (no default partition)
- Creates DEPENDENCY_INTERNAL for partition columns to prevent their individual removal
- Creates DEPENDENCY_NORMAL for external objects like operator classes and collations
- Handles both simple column partitioning and complex expression-based partitioning
- Invalidates relation cache to ensure immediate availability of partition information
- The default collation (DEFAULT_COLLATION_OID) is not recorded as a dependency since it's pinned