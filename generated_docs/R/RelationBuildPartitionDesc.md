# RelationBuildPartitionDesc

## Location
src/backend/partitioning/partdesc.c: 134 - 422

## Overview
Constructs a partition descriptor for a partitioned table relation and stores it in the relcache entry, managing memory contexts and handling concurrent partition operations.

## Definition


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
  - find_inheritance_children_extended
  - RelationGetPartitionKey
  - SysCacheGetAttr
  - stringToNode
  - TextDatumGetCString
  - systable_beginscan/systable_getnext
  - heap_getattr
  - AcceptInvalidationMessages
  - get_default_partition_oid
  - get_rel_relkind
  - partition_bounds_create
  - partition_bounds_copy
  - AllocSetContextCreate
  - MemoryContextAllocZero
  - MemoryContextSetParent
  - ActiveSnapshotSet

- Called from:
  - RelationGetPartitionDesc

## Notes and Other Information
- The function is static and only called internally by RelationGetPartitionDesc
- Implements complex memory management using separate contexts for different descriptor types
- Handles race conditions with concurrent DDL operations through retry mechanisms
- Validates partition boundary specifications and default partition consistency
- Stores different descriptor types (with/without detached partitions) in separate relcache fields
- Uses MVCC-aware logic when dealing with detached partitions and transaction snapshots
- The retry logic is limited to one attempt to avoid infinite loops in case of catalog corruption