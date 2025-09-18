# RelationGetPartitionDesc

## Location
[src/backend/partitioning/partdesc.c:71-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partdesc.c#L71-L133)

## Overview
Retrieves the partition descriptor for a partitioned table relation, with options to include or omit detached partitions based on the current snapshot.

## Definition


## Detailed Description
RelationGetPartitionDesc is a core function in PostgreSQL's partitioning subsystem that manages access to partition descriptors stored in the relation cache. The function maintains two types of partition descriptors in relcache: rd_partdesc (includes all partitions, even those being concurrently marked detached) and rd_partdesc_nodetached (omits detached partitions when appropriate).

The function implements intelligent caching logic that considers the current transaction snapshot to determine whether cached descriptors can be safely reused. For the rd_partdesc_nodetached descriptor, it uses the stored pg_inherits.xmin value to validate whether the cached descriptor is still valid for the current active snapshot.

When cached descriptors cannot be used, the function falls back to calling RelationBuildPartitionDesc to construct a fresh partition descriptor.

## Parameters / Member Variables
- : The partitioned table relation (must be RELKIND_PARTITIONED_TABLE)
- : Boolean flag indicating whether to omit detached partitions from the result

## Dependencies
- Functions called/Symbols referenced:
  - likely (compiler hint)
  - ActiveSnapshotSet
  - GetActiveSnapshot
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md)
  - [RelationBuildPartitionDesc](RelationBuildPartitionDesc.md)
  - [PartitionDesc](../P/PartitionDesc.md) (return type)

- Called from (representative examples):
  - [StorePartitionBound](../S/StorePartitionBound.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)
  - [PartitionDirectoryLookup](../P/PartitionDirectoryLookup.md)

## Notes and Other Information
- The function requires the relation to be a partitioned table (RELKIND_PARTITIONED_TABLE)
- Partition descriptors are kept alive until the relcache entry's refcount reaches zero
- The caching strategy is snapshot-aware to ensure MVCC consistency
- When no active snapshot is set, detached partitions are not omitted regardless of the omit_detached parameter
- The function is marked with 'likely()' hints for performance optimization on the common cache hit path