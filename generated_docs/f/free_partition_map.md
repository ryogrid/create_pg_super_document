# free_partition_map

## Location
[src/backend/partitioning/partbounds.c:1832-1842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1832-L1842)

## Overview
Frees the memory allocated for arrays within a PartitionMap structure used during partition merging operations.

## Definition

```c
static void
free_partition_map(PartitionMap *map)
```
## Detailed Description
This function deallocates the memory that was previously allocated by init_partition_map for the internal arrays of a PartitionMap structure. It frees the three arrays that track partition merging state: merged_indexes, merged, and old_indexes.

This is a cleanup function that should be called when the PartitionMap is no longer needed to prevent memory leaks during partition merging operations.

## Parameters / Member Variables
- `*map`: PartitionMap structure whose internal arrays need to be freed
## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (data structure)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from:
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- The function is static and internal to partbounds.c
- This function is the counterpart to init_partition_map and should always be called to clean up
- Frees three arrays: merged_indexes, merged, and old_indexes
- Does not free the PartitionMap structure itself, only its internal arrays
- Essential for preventing memory leaks in partition merging operations
- Should be called in cleanup sections of functions that use PartitionMap

## Simplified Source

```c
static void free_partition_map(PartitionMap *map)
{
    // Free all allocated arrays in the PartitionMap
    pfree(map->merged_indexes);
    pfree(map->merged);
    pfree(map->old_indexes);
}
```