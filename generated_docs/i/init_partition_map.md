# init_partition_map

## Location
[src/backend/partitioning/partbounds.c:1811-1831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1811-L1831)

## Overview
Initializes a PartitionMap structure for a given relation to track partition merging state during partitioned join operations.

## Definition

```c
static void
init_partition_map(RelOptInfo *rel, PartitionMap *map)
```
## Detailed Description
This function initializes a PartitionMap structure which is used to track the state of partitions during the partition merging process for partitioned joins. The PartitionMap maintains arrays that track which partitions have been merged, their new indexes after merging, and original indexes for potential remapping operations.

The function allocates memory for three arrays based on the number of partitions in the relation and initializes all values to indicate that no partitions have been merged yet.

## Parameters / Member Variables
- `*rel`: RelOptInfo structure containing information about the relation whose partitions need to be tracked
- `*map`: PartitionMap structure to be initialized for tracking partition merging state
## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (data structure)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from:
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- The function is static and internal to partbounds.c
- Memory is allocated for tracking arrays: merged_indexes, merged, and old_indexes
- All arrays are initialized with default values (-1 for indexes, false for merged status)
- The did_remapping flag is initially set to false
- This is a helper function used in the partition merging process for joins
- Memory allocated here should be freed using free_partition_map when no longer needed

## Simplified Source

```c
static void init_partition_map(RelOptInfo *rel, PartitionMap *map)
{
    int nparts = rel->nparts;

    // Initialize basic map properties
    map->nparts = nparts;
    map->did_remapping = false;

    // Allocate arrays for tracking partition state
    map->merged_indexes = palloc(sizeof(int) * nparts);
    map->merged = palloc(sizeof(bool) * nparts);
    map->old_indexes = palloc(sizeof(int) * nparts);

    // Initialize all partitions as unmerged
    for (int i = 0; i < nparts; i++)
    {
        map->merged_indexes[i] = -1;  // No merged index assigned yet
        map->old_indexes[i] = -1;     // No old index stored yet
        map->merged[i] = false;       // Not merged yet
    }
}
```