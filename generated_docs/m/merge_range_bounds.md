# merge_range_bounds

## Location
[src/backend/partitioning/partbounds.c:1506-1810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1506-L1810)

## Overview
Creates partition bounds for a join relation between range-partitioned tables by merging overlapping partitions using an algorithm similar to merge join.

## Definition

```c
struct to return. */
		merged_bounds = build_merged_partition_bounds(outer_bi->strategy,
													  merged_datums,
													  merged_kinds,
													  merged_indexes,
													  -1,
													  default_index);
```
## Detailed Description
This function merges the partition bounds of two range-partitioned relations for partitioned joins. It uses a merge-join-like algorithm to compare ranges from both sides and find overlapping partitions. The function handles several complex scenarios:

- Matches overlapping partitions from both sides
- Attempts to match non-overlapping partitions with default partitions if available
- For outer joins, tries to match partitions with dummy partitions on the nullable side
- Merges default partitions from both sides if they exist
- Gives up if it finds partitions that overlap with multiple partitions on the other side

The algorithm processes ranges in ascending order and creates merged partition bounds that can be used for efficient partitioned joins.

## Parameters / Member Variables
- : Number of partition key attributes
- : Array of support functions for partition key comparison
- : Array of collation OIDs for partition keys
- : RelOptInfo for the outer relation in the join
- : RelOptInfo for the inner relation in the join
- : Type of join operation (INNER, LEFT, RIGHT, FULL)
- : Output list of outer partition indexes that participate in the join
- : Output list of inner partition indexes that participate in the join

## Dependencies
- Functions called/Symbols referenced:
  - partition_bound_has_default
  - [init_partition_map](../i/init_partition_map.md)
  - [is_dummy_partition](../i/is_dummy_partition.md)
  - [get_range_partition](../g/get_range_partition.md)
  - [compare_range_partitions](../c/compare_range_partitions.md)
  - [merge_matching_partitions](merge_matching_partitions.md)
  - [get_merged_range_bounds](../g/get_merged_range_bounds.md)
  - compare_range_bounds
  - [process_outer_partition](../p/process_outer_partition.md)
  - [process_inner_partition](../p/process_inner_partition.md)
  - [add_merged_range_bounds](../a/add_merged_range_bounds.md)
  - [merge_default_partitions](merge_default_partitions.md)
  - [generate_matching_part_pairs](../g/generate_matching_part_pairs.md)
  - [build_merged_partition_bounds](../b/build_merged_partition_bounds.md)
  - [free_partition_map](../f/free_partition_map.md)
- Called from:
  - [partition_bounds_merge](../p/partition_bounds_merge.md)

## Notes and Other Information
- The function is static and internal to partbounds.c
- Returns NULL if merging is not possible (e.g., when a partition overlaps multiple partitions on the other side)
- The current implementation cannot handle cases where one partition matches multiple partitions on the other side
- Memory allocated during the process is cleaned up in the cleanup section
- The function modifies the outer_parts and inner_parts lists to return the matching partition pairs

## Simplified Source

```c
static PartitionBoundInfo merge_range_bounds(int partnatts, FmgrInfo *partsupfuncs,
                                           Oid *partcollations,
                                           RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                           JoinType jointype,
                                           List **outer_parts, List **inner_parts) {
    PartitionBoundInfo outer_bi = outer_rel->boundinfo;
    PartitionBoundInfo inner_bi = inner_rel->boundinfo;
    PartitionMap outer_map, inner_map;
    List *merged_datums = NIL;
    List *merged_kinds = NIL;
    List *merged_indexes = NIL;
    int next_index = 0;

    // Initialize partition mapping structures
    init_partition_map(outer_rel, &outer_map);
    init_partition_map(inner_rel, &inner_map);

    // Get first range from each side
    int outer_lb_pos = 0, inner_lb_pos = 0;
    PartitionRangeBound outer_lb, outer_ub, inner_lb, inner_ub;
    int outer_index = get_range_partition(outer_rel, outer_bi, &outer_lb_pos, &outer_lb, &outer_ub);
    int inner_index = get_range_partition(inner_rel, inner_bi, &inner_lb_pos, &inner_lb, &inner_ub);

    // Merge overlapping ranges using merge-join algorithm
    while (outer_index >= 0 || inner_index >= 0) {
        bool overlap = false;
        int lb_cmpval, ub_cmpval;

        if (outer_index >= 0 && inner_index >= 0) {
            // Check if current ranges overlap
            overlap = compare_range_partitions(partnatts, partsupfuncs, partcollations,
                                             &outer_lb, &outer_ub, &inner_lb, &inner_ub,
                                             &lb_cmpval, &ub_cmpval);
        }

        if (overlap) {
            // Ranges overlap - create merged partition
            int merged_idx = merge_matching_partitions(&outer_map, &inner_map,
                                                     outer_index, inner_index, &next_index);
            if (merged_idx < 0) goto cleanup;  // Failed to merge

            // Get bounds for merged partition
            PartitionRangeBound merged_lb, merged_ub;
            get_merged_range_bounds(partnatts, partsupfuncs, partcollations, jointype,
                                  &outer_lb, &outer_ub, &inner_lb, &inner_ub,
                                  lb_cmpval, ub_cmpval, &merged_lb, &merged_ub);

            // Add to merged bounds
            add_merged_range_bounds(partnatts, partsupfuncs, partcollations,
                                  &merged_lb, &merged_ub, merged_idx,
                                  &merged_datums, &merged_kinds, &merged_indexes);

            // Move to next ranges on both sides
            outer_index = get_range_partition(outer_rel, outer_bi, &outer_lb_pos, &outer_lb, &outer_ub);
            inner_index = get_range_partition(inner_rel, inner_bi, &inner_lb_pos, &inner_lb, &inner_ub);

        } else if (ub_cmpval < 0) {
            // Outer range doesn't overlap - handle unmatched outer partition
            handle_outer_range(&outer_map, &inner_map, outer_index, jointype, &next_index);
            outer_index = get_range_partition(outer_rel, outer_bi, &outer_lb_pos, &outer_lb, &outer_ub);

        } else {
            // Inner range doesn't overlap - handle unmatched inner partition
            handle_inner_range(&outer_map, &inner_map, inner_index, jointype, &next_index);
            inner_index = get_range_partition(inner_rel, inner_bi, &inner_lb_pos, &inner_lb, &inner_ub);
        }
    }

    // Handle default partitions
    merge_default_partitions(&outer_map, &inner_map, jointype, &next_index);

    // Generate final result if successful
    PartitionBoundInfo result = NULL;
    if (next_index > 0) {
        generate_matching_part_pairs(outer_rel, inner_rel, &outer_map, &inner_map,
                                   next_index, outer_parts, inner_parts);
        result = build_merged_partition_bounds(outer_bi->strategy, merged_datums,
                                             merged_kinds, merged_indexes, -1, -1);
    }

cleanup:
    // Cleanup memory
    free_partition_map(&outer_map);
    free_partition_map(&inner_map);
    return result;
}
```