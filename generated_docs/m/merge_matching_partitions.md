# merge_matching_partitions

## Location
[src/backend/partitioning/partbounds.c:1862-1979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1862-L1979)

## Overview
Attempts to merge given outer and inner partitions and returns the index of the merged partition if successful, or -1 if merging fails.

## Definition

```c
static int
merge_matching_partitions(PartitionMap *outer_map, PartitionMap *inner_map,
						  int outer_index, int inner_index, int *next_index)
```
## Detailed Description
This function handles the complex logic of merging partitions from outer and inner relations during partitioned join operations. It manages several scenarios:

1. **Both partitions already merged**: If both partitions have already been assigned merged partition indexes, it checks if they're the same (success) or handles re-mapping for list partitioning when both were merged with dummy partitions.

2. **Neither partition merged**: Creates a new merged partition and assigns the same merged index to both partitions.

3. **One partition already merged**: If one partition was previously merged with a dummy partition and the other hasn't been merged yet, it assigns the existing merged index to the unmerged partition.

The function maintains the merged state in PartitionMap structures and handles re-mapping operations necessary for list partitioning to preserve canonical ordering.

## Parameters / Member Variables
- : PartitionMap structure tracking merge state for outer relation partitions
- : PartitionMap structure tracking merge state for inner relation partitions  
- : Index of the partition in the outer relation to merge
- : Index of the partition in the inner relation to merge
- : Pointer to the next available merged partition index (incremented when creating new merged partitions)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (data structure)
- Called from:
  - [merge_list_bounds](merge_list_bounds.md)
  - [merge_range_bounds](merge_range_bounds.md)
  - [process_outer_partition](../p/process_outer_partition.md)
  - [process_inner_partition](../p/process_inner_partition.md)
  - [merge_null_partitions](merge_null_partitions.md)
  - [merge_default_partitions](merge_default_partitions.md)

## Notes and Other Information
- The function is static and internal to partbounds.c
- Returns the merged partition index on success, -1 on failure
- Handles re-mapping for list partitioning to maintain canonical ordering based on smallest list values
- Updates the did_remapping flag when re-mapping occurs
- Maintains the merged state and merged_indexes arrays in both PartitionMap structures
- Critical component of the partition merging algorithm for partitioned joins
- The function ensures that partition merging operations are consistent and reversible when necessary

## Simplified Source

```c
static int
merge_matching_partitions(PartitionMap *outer_map, PartitionMap *inner_map,
                         int outer_index, int inner_index, int *next_index)
{
    // Get current merge status for both partitions
    int outer_merged_idx = outer_map->merged_indexes[outer_index];
    int inner_merged_idx = inner_map->merged_indexes[inner_index];
    bool outer_merged = outer_map->merged[outer_index];
    bool inner_merged = inner_map->merged[inner_index];

    // Case 1: Both partitions already have merged indexes
    if (outer_merged_idx >= 0 && inner_merged_idx >= 0)
    {
        // If they map to the same merged partition, we're done
        if (outer_merged_idx == inner_merged_idx)
            return outer_merged_idx;

        // If both were merged with dummy partitions, re-map to smaller index
        if (!outer_merged && !inner_merged)
        {
            if (outer_merged_idx < inner_merged_idx)
            {
                // Re-map inner to outer's merged index
                inner_map->merged_indexes[inner_index] = outer_merged_idx;
                inner_map->old_indexes[inner_index] = inner_merged_idx;
                inner_map->did_remapping = true;
                outer_map->merged[outer_index] = true;
                inner_map->merged[inner_index] = true;
                return outer_merged_idx;
            }
            else
            {
                // Re-map outer to inner's merged index
                outer_map->merged_indexes[outer_index] = inner_merged_idx;
                outer_map->old_indexes[outer_index] = outer_merged_idx;
                outer_map->did_remapping = true;
                outer_map->merged[outer_index] = true;
                inner_map->merged[inner_index] = true;
                return inner_merged_idx;
            }
        }
        return -1;  // Can't merge - conflict
    }

    // Case 2: Neither partition has been merged yet - create new merged partition
    if (outer_merged_idx == -1 && inner_merged_idx == -1)
    {
        int new_merged_index = *next_index;
        outer_map->merged_indexes[outer_index] = new_merged_index;
        inner_map->merged_indexes[inner_index] = new_merged_index;
        outer_map->merged[outer_index] = true;
        inner_map->merged[inner_index] = true;
        (*next_index)++;
        return new_merged_index;
    }

    // Case 3: One partition merged with dummy, other not merged - join them
    if (outer_merged_idx >= 0 && !outer_merged)
    {
        inner_map->merged_indexes[inner_index] = outer_merged_idx;
        inner_map->merged[inner_index] = true;
        outer_map->merged[outer_index] = true;
        return outer_merged_idx;
    }

    if (inner_merged_idx >= 0 && !inner_merged)
    {
        outer_map->merged_indexes[outer_index] = inner_merged_idx;
        outer_map->merged[outer_index] = true;
        inner_map->merged[inner_index] = true;
        return inner_merged_idx;
    }

    return -1;  // Unable to merge
}
```