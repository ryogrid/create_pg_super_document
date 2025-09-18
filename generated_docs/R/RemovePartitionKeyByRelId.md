# RemovePartitionKeyByRelId

## Location
src/backend/catalog/heap.c: 3501 - 3531

## Overview
Removes the pg_partitioned_table catalog entry for a relation, effectively removing all partition key metadata when a partitioned table is dropped or converted.

## Definition
```c
void RemovePartitionKeyByRelId(Oid relid)
```

## Detailed Description
This function performs a straightforward cleanup operation by locating and deleting the pg_partitioned_table entry for the specified relation. It uses the system cache for efficient lookup and ensures the catalog entry is properly removed when a partitioned table is being dropped or when partitioning is being removed from a table.

The function performs minimal error checking - if the partition key entry is not found in the catalog, it raises an ERROR indicating a cache lookup failure, which suggests an inconsistent catalog state.

## Parameters / Member Variables
- `relid`: OID of the relation whose partition key entry should be removed

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - CatalogTupleDelete
  - ReleaseSysCache
  - table_close
  - ObjectIdGetDatum
- Called from (representative examples):
  - heap_drop_with_catalog

## Notes and Other Information
- Requires RowExclusiveLock on PartitionedRelationId catalog
- Uses PARTRELID cache for efficient tuple lookup
- Raises ERROR if the partition key entry is not found, indicating catalog inconsistency
- This is typically called during table dropping or when converting a partitioned table to regular table
- The function does not handle cascade deletion of related partition metadata - that's handled by dependency system
- Complementary function to StorePartitionKey for partition key lifecycle management