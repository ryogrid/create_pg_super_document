# heap_drop_with_catalog

## Location
src/backend/catalog/heap.c: 1767 - 1946

## Overview
heap_drop_with_catalog removes a specified relation from the system catalogs, handling the complete catalog cleanup for table drops including partitioning considerations and storage management.

## Definition


## Detailed Description
This function performs the comprehensive catalog cleanup required when dropping a relation. It handles special considerations for partitioned tables by acquiring necessary locks on parent and default partitions to prevent concurrent access issues. The function systematically removes entries from various system catalogs including pg_foreign_table (for foreign tables), pg_partitioned_table (for partitioned tables), handles partition constraint updates, schedules physical storage deletion, removes statistics, clears inheritance relationships, and performs cache invalidation. The function works in coordination with the dependency system and should typically be called through performDeletion() rather than directly.

## Parameters / Member Variables
- `relid`: OID of the relation to be dropped from the catalogs

## Dependencies
- Functions called/Symbols referenced:
  - get_partition_parent
  - get_default_partition_oid
  - LockRelationOid
  - relation_open
  - CheckTableNotInUse
  - CheckTableForSerializableConflictIn
  - CatalogTupleDelete
  - RemovePartitionKeyByRelId
  - update_default_partition_oid
  - RelationDropStorage
  - pgstat_drop_relation
  - relation_close
  - RemoveSubscriptionRel
  - remove_on_commit_action
  - RelationForgetRelation
  - RelationRemoveInheritance
  - RemoveStatistics
  - DeleteAttributeTuples
  - DeleteRelationTuple
  - CacheInvalidateRelcacheByRelid
- Called from (representative examples):
  - doDeletion

## Notes and Other Information
- Should not be called directly; use performDeletion() instead which handles dependency resolution
- For partitions, acquires AccessExclusiveLock on parent table to prevent concurrent queries using stale partition descriptors  
- Handles foreign table cleanup by removing pg_foreign_table entries
- For partitioned tables, removes pg_partitioned_table entries via RemovePartitionKeyByRelId
- Updates default partition OID if dropping the default partition itself
- Schedules physical storage unlinking at commit time for relations with storage
- Maintains AccessExclusiveLock until transaction commit to prevent concurrent access
- Performs comprehensive cache invalidation for partitioning hierarchy changes
- Removes subscription relations, ON COMMIT actions, inheritance, statistics, and attribute tuples
- Uses RELKIND_HAS_STORAGE macro to determine if storage cleanup is needed