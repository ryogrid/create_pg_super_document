# generate_matching_part_pairs

## Location
[src/backend/partitioning/partbounds.c:2439-2517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2439-L2517)

## Overview
Generates a pair of lists of partitions that produce merged partitions, organizing them in the order of merged partition indexes for subsequent partition processing operations.

## Definition
```c
static void generate_matching_part_pairs(RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                        PartitionMap *outer_map, PartitionMap *inner_map,
                                        int nmerged,
                                        List **outer_parts, List **inner_parts)
```

## Detailed Description
The `generate_matching_part_pairs` function creates coordinated lists of partitions from outer and inner relations that correspond to merged partitions. This function is essential in partition-wise join operations where PostgreSQL needs to match up partitions from two different partitioned tables.

The function operates in three main phases:
1. **Index Array Setup**: Creates temporary arrays to map merged partition indexes to their corresponding outer and inner partition indexes
2. **Partition Matching**: Iterates through all partitions in both relations and populates the index mapping based on the merged_indexes from the partition maps
3. **List Building**: Constructs the final output lists by walking through merged partitions in order and adding the corresponding RelOptInfo pointers (or NULL for dummy partitions)

A key feature is handling dummy partitions - when both outer and inner partitions are marked as dummy (index -1), it means the merged partition was removed during re-merging operations and should be ignored.

## Parameters / Member Variables
- `outer_rel`: RelOptInfo pointer for the outer relation containing partition information
- `inner_rel`: RelOptInfo pointer for the inner relation containing partition information  
- `outer_map`: PartitionMap pointer containing merged index mapping for outer partitions
- `inner_map`: PartitionMap pointer containing merged index mapping for inner partitions
- `nmerged`: Integer representing the total number of merged partitions expected
- `outer_parts`: Output parameter - pointer to List pointer that will contain outer partition RelOptInfo entries
- `inner_parts`: Output parameter - pointer to List pointer that will contain inner partition RelOptInfo entries

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (struct type)
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (struct type)
  - [lappend](../l/lappend.md) (list manipulation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
  - Max (macro for maximum value)
- Called from (representative examples):
  - compare_range_bounds
  - [merge_list_bounds](../m/merge_list_bounds.md)  
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function, accessible only within partbounds.c
- The function expects both output lists to be initially NIL (empty) and validates this with assertions
- Memory management is handled properly with palloc/pfree for temporary index arrays
- The function handles asymmetric partition counts between outer and inner relations by using Max(outer_nparts, inner_nparts)
- Index value -1 is used as a sentinel to indicate dummy/non-existent partitions
- The resulting lists maintain correspondence - outer_parts[i] and inner_parts[i] represent matching partitions for the same merged partition

## Simplified Source

```c
static void
generate_matching_part_pairs(RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                             PartitionMap *outer_map, PartitionMap *inner_map,
                             int nmerged, List **outer_parts, List **inner_parts)
{
    // Create temporary index mapping arrays
    int *outer_indexes = palloc(sizeof(int) * nmerged);
    int *inner_indexes = palloc(sizeof(int) * nmerged);

    // Initialize all indexes as unset
    for (int i = 0; i < nmerged; i++)
        outer_indexes[i] = inner_indexes[i] = -1;

    // Map partition indexes to merged partition positions
    int max_parts = Max(outer_map->nparts, inner_map->nparts);
    for (int i = 0; i < max_parts; i++)
    {
        // Map outer partition if it exists
        if (i < outer_map->nparts)
        {
            int merged_idx = outer_map->merged_indexes[i];
            if (merged_idx >= 0)
                outer_indexes[merged_idx] = i;
        }

        // Map inner partition if it exists
        if (i < inner_map->nparts)
        {
            int merged_idx = inner_map->merged_indexes[i];
            if (merged_idx >= 0)
                inner_indexes[merged_idx] = i;
        }
    }

    // Build the output lists in merged partition order
    for (int i = 0; i < nmerged; i++)
    {
        int outer_idx = outer_indexes[i];
        int inner_idx = inner_indexes[i];

        // Skip if both partitions are dummy (removed during re-merging)
        if (outer_idx == -1 && inner_idx == -1)
            continue;

        // Add RelOptInfo pointers or NULL for missing partitions
        *outer_parts = lappend(*outer_parts,
                              outer_idx >= 0 ? outer_rel->part_rels[outer_idx] : NULL);
        *inner_parts = lappend(*inner_parts,
                              inner_idx >= 0 ? inner_rel->part_rels[inner_idx] : NULL);
    }

    pfree(outer_indexes);
    pfree(inner_indexes);
}
```