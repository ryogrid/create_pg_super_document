# PartitionDesc

## Location
[src/include/partitioning/partdefs.h:22-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partdefs.h#L22-L23)

## Overview
A pointer to PartitionDescData structure that contains comprehensive information about all partitions of a partitioned table, including their OIDs, bounds, and caching information for efficient partition lookup.

## Definition

```c
typedef struct PartitionDescData *PartitionDesc;
```
## Detailed Description
PartitionDesc is a pointer type to the PartitionDescData structure that serves as the complete descriptor for a partitioned table's partition hierarchy. It contains arrays of partition OIDs and metadata, the partition boundary information, and performance optimization fields for caching recent partition lookups.

The structure maintains partition information in bound-sorted order and includes optimization for partition tuple routing through caching mechanisms. It tracks whether partitions are leaf partitions (can accept data directly) versus intermediate partitioned tables, and handles special cases like detached partitions that may or may not be visible depending on the caller's snapshot.

The caching fields (last_found_*) provide significant performance improvements for workloads with locality, where consecutive tuple insertions often target the same partition. This avoids repeated binary searches through the partition bounds.

## Parameters / Member Variables
(This is a typedef pointer, see PartitionDescData for actual structure members)
- : Total number of partitions in the hierarchy
- : Flag indicating presence of detached partitions
- : Array of partition OIDs ordered by partition bounds
- : Array indicating which partitions are leaf partitions (can accept data)
- : PartitionBoundInfo containing the partition boundary metadata
- : Cache field - index of last looked up datum for performance
- : Cache field - index of last found partition
- : Cache field - consecutive hit count for cache effectiveness

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDescData](PartitionDescData.md) (underlying structure)
  - [PartitionBoundInfo](PartitionBoundInfo.md) (partition boundary information)
  - Oid (object identifier for partitions)

- Called from (representative examples):
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)/RelationBuildPartitionDesc (descriptor construction and caching)
  - [ExecFindPartition](../E/ExecFindPartition.md) (tuple routing during INSERT/UPDATE)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md) (partition pruning setup)
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md) (runtime partition selection)
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md) (planner partition expansion)

## Notes and Other Information
- Cached in relation descriptor for repeated access during operations
- Detached partition handling depends on caller's snapshot visibility
- Caching fields provide significant performance improvement for INSERT-heavy workloads with locality
- Used by both planner (for partition pruning/expansion) and executor (for tuple routing)
- Maintains partition metadata in a format optimized for binary search operations
- Essential for efficient partition-wise operations including parallel query execution
- Handles both simple partitioning and complex multi-level partition hierarchies