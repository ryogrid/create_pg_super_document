# index_get_partition

## Location
src/backend/catalog/partition.c: 176 - 221

## Overview
Returns the OID of an index on a given partition that is a child of a specified parent index, or InvalidOid if no such index exists.

## Definition
```c
Oid index_get_partition(Relation partition, Oid indexId)
```

## Detailed Description
This function searches through all indexes on a given partition relation to find one that is a child of the specified parent index. It retrieves the list of all indexes on the partition using `RelationGetIndexList`, then iterates through each index checking if it is a partition index and whether its parent matches the given indexId. The function uses the system cache to access pg_class information for each index to determine if it is a partitioned index, and calls `get_partition_parent` to verify the parent-child relationship.

The function is typically used in partitioned table scenarios where you need to find the corresponding partition-specific index that inherits from a parent table's index.

## Parameters / Member Variables
- `partition`: Relation object representing the partition table
- `indexId`: OID of the parent index for which we want to find the corresponding child index

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList (to get list of indexes on the partition)
  - SearchSysCache1 (to look up index information in pg_class)
  - HeapTupleIsValid (to validate system cache results)
  - GETSTRUCT (to extract struct from heap tuple)
  - ReleaseSysCache (to release system cache entry)
  - get_partition_parent (to check if index is child of specified parent)
  - list_free (to free the index list)
  - Form_pg_class (struct type for pg_class tuples)

- Called from (representative examples):
  - addFkRecurseReferenced
  - CloneFkReferenced
  - refuseDupeIndexAttach

## Notes and Other Information
- Returns InvalidOid if no matching partition index is found
- Only considers indexes that have relispartition = true
- Uses system cache lookups for efficient access to pg_class
- Properly cleans up allocated memory by freeing the index list
- Located at src/backend/catalog/partition.c:176-221
- Includes error handling for failed cache lookups