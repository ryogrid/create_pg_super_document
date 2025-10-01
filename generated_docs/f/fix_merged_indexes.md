# fix_merged_indexes

## Location
[src/backend/partitioning/partbounds.c:2385-2438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2385-L2438)

## Overview
Adjusts merged indexes of re-merged partitions during partition bound merging operations to maintain correct index mapping after partition restructuring.

## Definition

```c
static void
fix_merged_indexes(PartitionMap *outer_map, PartitionMap *inner_map,
				   int nmerged, List *merged_indexes)
```
## Detailed Description
The  function is responsible for updating the merged indexes list when partitions are re-merged during partition bound operations. When partition bounds are merged, the original indexes may become invalid due to restructuring, and this function creates a mapping from old merged indexes to new merged indexes and applies this mapping to fix the provided merged_indexes list.

The function operates in two main phases:
1. **Mapping Construction**: It builds a mapping array by examining both outer and inner partition maps' remapping information, creating a translation table from old to new merged indexes.
2. **Index Fixing**: It iterates through the merged_indexes list and updates each index using the constructed mapping, ensuring that all references point to the correct new positions.

## Parameters / Member Variables
- : PartitionMap pointer containing information about the outer partition mapping, including remapping details
- : PartitionMap pointer containing information about the inner partition mapping, including remapping details  
- : Integer representing the number of merged partitions to process
- : List of integer indexes that need to be adjusted based on the new partition structure

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (struct type)
  - lfirst_int (list access macro)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - compare_range_bounds
  - [merge_list_bounds](../m/merge_list_bounds.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the partbounds.c file
- The function assumes nmerged > 0 and includes an assertion to validate this
- Memory management is handled properly with palloc/pfree for the temporary new_indexes array
- The function handles cases where either outer_map or inner_map (or both) have performed remapping operations
- Index values of -1 are used to indicate invalid or unset indexes in the mapping process

## Simplified Source

```c
static void
fix_merged_indexes(PartitionMap *outer_map, PartitionMap *inner_map,
                   int nmerged, List *merged_indexes)
{
    // Create mapping array to translate old indexes to new indexes
    int *index_mapping = palloc(sizeof(int) * nmerged);
    for (int i = 0; i < nmerged; i++)
        index_mapping[i] = -1;

    // Build mapping from outer partition map if it was remapped
    if (outer_map->did_remapping)
    {
        for (int i = 0; i < outer_map->nparts; i++)
        {
            int old_index = outer_map->old_indexes[i];
            if (old_index >= 0)
                index_mapping[old_index] = outer_map->merged_indexes[i];
        }
    }

    // Build mapping from inner partition map if it was remapped
    if (inner_map->did_remapping)
    {
        for (int i = 0; i < inner_map->nparts; i++)
        {
            int old_index = inner_map->old_indexes[i];
            if (old_index >= 0)
                index_mapping[old_index] = inner_map->merged_indexes[i];
        }
    }

    // Apply the mapping to fix the merged_indexes list
    ListCell *lc;
    foreach(lc, merged_indexes)
    {
        int current_index = lfirst_int(lc);
        if (index_mapping[current_index] >= 0)
            lfirst_int(lc) = index_mapping[current_index];
    }

    pfree(index_mapping);
}
```