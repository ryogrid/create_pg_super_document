# merge_range_bounds

## Location
src/backend/partitioning/partbounds.c: 1506 - 1810

## Overview
Creates partition bounds for a join relation between range-partitioned tables by merging overlapping partitions using an algorithm similar to merge join.

## Definition


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
  - init_partition_map
  - is_dummy_partition
  - get_range_partition
  - compare_range_partitions
  - merge_matching_partitions
  - get_merged_range_bounds
  - compare_range_bounds
  - process_outer_partition
  - process_inner_partition
  - add_merged_range_bounds
  - merge_default_partitions
  - generate_matching_part_pairs
  - build_merged_partition_bounds
  - free_partition_map
- Called from:
  - partition_bounds_merge

## Notes and Other Information
- The function is static and internal to partbounds.c
- Returns NULL if merging is not possible (e.g., when a partition overlaps multiple partitions on the other side)
- The current implementation cannot handle cases where one partition matches multiple partitions on the other side
- Memory allocated during the process is cleaned up in the cleanup section
- The function modifies the outer_parts and inner_parts lists to return the matching partition pairs