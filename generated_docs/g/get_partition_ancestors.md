# get_partition_ancestors

## Location
src/backend/catalog/partition.c: 134 - 152

## Overview
Obtains all ancestors of a given partition relation, returning them as a list ordered from immediate parent to topmost parent.

## Definition
```c
List *get_partition_ancestors(Oid relid)
```

## Detailed Description
This function retrieves the complete ancestor hierarchy of a partition by opening the pg_inherits catalog table and delegating the work to `get_partition_ancestors_worker`. The returned list contains OIDs of all ancestors in the partition hierarchy, with the first element being the immediate parent and the last element being the topmost parent (root of the partition tree).

The function assumes that each relation in the hierarchy has precisely one parent, making it suitable only for partition relationships rather than general inheritance scenarios.

## Parameters / Member Variables
- `relid`: OID of the partition relation whose ancestors are to be found

## Dependencies
- Functions called/Symbols referenced:
  - table_open (to access InheritsRelationId catalog)
  - get_partition_ancestors_worker (performs the recursive ancestor lookup)
  - table_close (to release catalog lock)

- Called from (representative examples):
  - index_concurrently_swap
  - getIdentitySequence
  - filter_partitions
  - ExecGetAncestorResultRels
  - ExecInitPartitionInfo
  - get_rel_sync_entry
  - pg_partition_root
  - pg_partition_ancestors
  - RelationBuildPublicationDesc

## Notes and Other Information
- Returns NIL (empty list) if the relation has no ancestors
- The list is ordered from closest ancestor to farthest ancestor
- Uses AccessShareLock for safe concurrent access to the pg_inherits catalog
- Should only be called when it is known that the relation is a partition
- Located at src/backend/catalog/partition.c:134-152
- Memory for the returned list is allocated in the current memory context