# RelationBuildPartitionDesc

## Location
[src/backend/partitioning/partdesc.c:134-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L134-L422)

## Overview
Constructs a partition descriptor for a partitioned table relation and stores it in the relcache entry, managing memory contexts and handling concurrent partition operations.

## Definition

```c
static PartitionDesc
RelationBuildPartitionDesc(Relation rel, bool omit_detached)
```
## Detailed Description
RelationBuildPartitionDesc is the core function responsible for constructing partition descriptors from scratch. It performs several complex operations:

1. **Memory Management**: Creates a dedicated memory context (child of CurTransactionContext initially, then reparented to CacheMemoryContext) to avoid memory leaks and ensure proper cleanup.

2. **Partition Discovery**: Uses find_inheritance_children_extended to retrieve partition OIDs from pg_inherits, handling detached partitions based on the omit_detached parameter.

3. **Boundary Specification Collection**: Fetches partition boundary specifications from pg_class.relpartbound for each partition, with fallback logic to handle concurrent ATTACH/DETACH operations.

4. **Retry Logic**: Implements sophisticated retry mechanisms to handle race conditions with concurrent ATTACH PARTITION and DETACH CONCURRENTLY operations.

5. **Partition Bounds Creation**: Calls partition_bounds_create to build the canonical representation of partition boundaries and their mappings.

6. **Caching Integration**: Stores the completed partition descriptor in the appropriate relcache fields (rd_partdesc or rd_partdesc_nodetached) with proper memory context management.

## Parameters / Member Variables
- : The partitioned table relation to build the descriptor for
- : Boolean flag indicating whether to omit detached partitions from the descriptor

## Dependencies
- Functions called/Symbols referenced:
  - [find_inheritance_children_extended](../f/find_inheritance_children_extended.md)
  - [RelationGetPartitionKey](RelationGetPartitionKey.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [heap_getattr](../h/heap_getattr.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [get_default_partition_oid](../g/get_default_partition_oid.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [partition_bounds_create](../p/partition_bounds_create.md)
  - [partition_bounds_copy](../p/partition_bounds_copy.md)
  - AllocSetContextCreate
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md)

- Called from:
  - [RelationGetPartitionDesc](RelationGetPartitionDesc.md)

## Notes and Other Information
- The function is static and only called internally by RelationGetPartitionDesc
- Implements complex memory management using separate contexts for different descriptor types
- Handles race conditions with concurrent DDL operations through retry mechanisms
- Validates partition boundary specifications and default partition consistency
- Stores different descriptor types (with/without detached partitions) in separate relcache fields
- Uses MVCC-aware logic when dealing with detached partitions and transaction snapshots
- The retry logic is limited to one attempt to avoid infinite loops in case of catalog corruption