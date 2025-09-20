# PartitionDirectory

## Location
[src/include/partitioning/partdefs.h:24-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partdefs.h#L24-L26)

## Overview
A pointer to PartitionDirectoryData structure that provides a caching mechanism for partition descriptors, allowing efficient lookup and reuse of PartitionDesc objects across multiple operations.

## Definition

```c
typedef struct PartitionDirectoryData *PartitionDirectory;
```
## Detailed Description
PartitionDirectory is a pointer type to the PartitionDirectoryData structure that implements a hash-table based cache for partition descriptors. It serves as an optimization layer that prevents repeated construction of PartitionDesc objects for the same partitioned relations during complex operations like joins or bulk operations.

The directory maintains a hash table indexed by relation OID, storing PartitionDirectoryEntry structures that contain both the relation pointer and the associated PartitionDesc. It operates within a dedicated memory context to ensure proper memory management and cleanup. The omit_detached flag controls whether detached partitions are included in cached descriptors.

This caching mechanism is particularly important for multi-table operations where the same partitioned relations might be accessed repeatedly, such as in partition-wise joins or when processing multiple partitioned tables in a single query.

## Parameters / Member Variables
(This is a typedef pointer, see PartitionDirectoryData for actual structure members)
- : Memory context for partition directory allocations
- : Hash table storing PartitionDirectoryEntry objects indexed by relation OID  
- : Flag indicating whether to omit detached partitions from cached descriptors

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDirectoryData](PartitionDirectoryData.md) (underlying structure)
  - [PartitionDirectoryEntry](PartitionDirectoryEntry.md) (hash table entry structure)
  - [MemoryContext](../M/MemoryContext.md) (PostgreSQL memory management)
  - [HTAB](../H/HTAB.md) (PostgreSQL hash table type)
  - [PartitionDesc](PartitionDesc.md) (cached partition descriptors)

- Called from (representative examples):
  - [CreatePartitionDirectory](../C/CreatePartitionDirectory.md) (directory creation and initialization)
  - [PartitionDirectoryLookup](PartitionDirectoryLookup.md) (partition descriptor lookup with caching)
  - [DestroyPartitionDirectory](../D/DestroyPartitionDirectory.md) (cleanup and memory deallocation)
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md) (integration with partition descriptor building)

## Notes and Other Information
- Used primarily in planner and executor contexts for multi-partition operations
- Provides significant performance benefits when the same partitioned relations are accessed repeatedly
- Memory management is handled through a dedicated memory context for clean resource cleanup
- The omit_detached flag ensures consistency with snapshot visibility requirements
- Essential for efficient partition-wise join operations where multiple partitioned relations are involved
- Hash table provides O(1) lookup performance for cached partition descriptors
- Lifetime typically spans a single query execution or planning phase
- Referenced in EState and PlannerGlobal structures for query-wide availability