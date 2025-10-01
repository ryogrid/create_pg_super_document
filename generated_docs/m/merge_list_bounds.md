# merge_list_bounds

## Location
[src/backend/partitioning/partbounds.c:1198-1505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1198-L1505)

## Overview
Creates partition bounds for a join relation between list partitioned tables by finding matching partitions using a merge-join-like algorithm on sorted list values.

## Definition

```c
struct to return. */
		merged_bounds = build_merged_partition_bounds(outer_bi->strategy,
													  merged_datums,
													  NIL,
													  merged_indexes,
													  null_index,
													  default_index);
```
## Detailed Description
This function implements partition-wise join support for list partitioned tables by merging their partition bounds. It uses a merge-join-like algorithm to find matching partitions from both sides by comparing list values in ascending order. The function handles various scenarios including:

- Exact matches between list values from both sides
- Missing partitions on one side (matched with default partition or dummy partition for outer joins)
- Default partitions from both sides
- NULL partitions from both sides
- Empty partitions (proven dummy partitions)

The algorithm maintains partition maps to track relationships and generates lists of matching partition pairs for the join operation. It fails if any partition matches multiple partitions on the other side.

## Parameters / Member Variables
- : Array of comparison functions for partition attributes
- : Array of collation OIDs for partition attributes  
- : RelOptInfo for the outer relation in the join
- : RelOptInfo for the inner relation in the join
- : Type of join being performed (affects handling of missing partitions)
- : Output parameter for list of matching outer partitions
- : Output parameter for list of matching inner partitions

## Dependencies
- Functions called/Symbols referenced:
  - partition_bound_has_default
  - partition_bound_accepts_nulls
  - [init_partition_map](../i/init_partition_map.md) / free_partition_map
  - [is_dummy_partition](../i/is_dummy_partition.md)
  - [merge_matching_partitions](merge_matching_partitions.md)
  - [process_outer_partition](../p/process_outer_partition.md) / process_inner_partition
  - [merge_null_partitions](merge_null_partitions.md) / merge_default_partitions
  - [generate_matching_part_pairs](../g/generate_matching_part_pairs.md)
  - [build_merged_partition_bounds](../b/build_merged_partition_bounds.md)
  - [fix_merged_indexes](../f/fix_merged_indexes.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [PartitionMap](../P/PartitionMap.md)
  - [RelOptInfo](../R/RelOptInfo.md)
  - JoinType
- Constants used:
  - PARTITION_STRATEGY_LIST
  - JOIN_FULL
  - IS_OUTER_JOIN
- Called from:
  - [partition_bounds_merge](../p/partition_bounds_merge.md)
  - compare_range_bounds

## Notes and Other Information
- Only supports list partitioned tables (single partition key constraint)
- Uses merge-join algorithm for efficient comparison of sorted list values
- Handles empty partitions by checking if they are dummy/proven empty
- Supports all join types with appropriate handling for missing partitions
- Returns NULL if partitioned join is not feasible (partition matching multiple partitions)
- Critical component for query optimizer's partitioned join planning for list partitions
- Memory management includes cleanup section to free temporary structures

## Simplified Source

```c
static PartitionBoundInfo merge_list_bounds(FmgrInfo *partsupfunc, Oid *partcollation,
                                          RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                          JoinType jointype,
                                          List **outer_parts, List **inner_parts) {
    PartitionBoundInfo outer_bi = outer_rel->boundinfo;
    PartitionBoundInfo inner_bi = inner_rel->boundinfo;
    PartitionMap outer_map, inner_map;
    List *merged_datums = NIL;
    List *merged_indexes = NIL;
    int next_index = 0;

    // Initialize partition mapping structures
    init_partition_map(outer_rel, &outer_map);
    init_partition_map(inner_rel, &inner_map);

    // Merge partitions using merge-join algorithm
    int outer_pos = 0, inner_pos = 0;
    while (outer_pos < outer_bi->ndatums || inner_pos < inner_bi->ndatums) {
        // Skip empty/dummy partitions
        if (outer_pos < outer_bi->ndatums &&
            is_dummy_partition(outer_rel, outer_bi->indexes[outer_pos])) {
            outer_pos++;
            continue;
        }
        if (inner_pos < inner_bi->ndatums &&
            is_dummy_partition(inner_rel, inner_bi->indexes[inner_pos])) {
            inner_pos++;
            continue;
        }

        // Compare list values (similar to merge join)
        int cmpval = compare_partition_values(outer_pos, inner_pos,
                                            outer_bi, inner_bi, partsupfunc, partcollation);

        if (cmpval == 0) {
            // Exact match - merge partitions
            int merged_idx = merge_matching_partitions(&outer_map, &inner_map,
                                                     outer_bi->indexes[outer_pos],
                                                     inner_bi->indexes[inner_pos],
                                                     &next_index);
            if (merged_idx >= 0) {
                merged_datums = lappend(merged_datums, outer_bi->datums[outer_pos]);
                merged_indexes = lappend_int(merged_indexes, merged_idx);
            }
            outer_pos++;
            inner_pos++;
        } else if (cmpval < 0) {
            // Handle outer partition with no inner match
            handle_unmatched_outer_partition(&outer_map, &inner_map, outer_pos, jointype);
            outer_pos++;
        } else {
            // Handle inner partition with no outer match
            handle_unmatched_inner_partition(&outer_map, &inner_map, inner_pos, jointype);
            inner_pos++;
        }
    }

    // Handle NULL and default partitions
    merge_special_partitions(&outer_map, &inner_map, jointype, &next_index);

    // Generate final result if successful
    PartitionBoundInfo result = NULL;
    if (next_index > 0) {
        generate_matching_part_pairs(outer_rel, inner_rel, &outer_map, &inner_map,
                                   next_index, outer_parts, inner_parts);
        result = build_merged_partition_bounds(outer_bi->strategy, merged_datums,
                                             NIL, merged_indexes, -1, -1);
    }

    // Cleanup
    free_partition_map(&outer_map);
    free_partition_map(&inner_map);
    return result;
}
```