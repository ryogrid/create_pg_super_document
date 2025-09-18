# merge_list_bounds

## Location
src/backend/partitioning/partbounds.c: 1198 - 1505

## Overview
Creates partition bounds for a join relation between list partitioned tables by finding matching partitions using a merge-join-like algorithm on sorted list values.

## Definition


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
  - init_partition_map / free_partition_map
  - is_dummy_partition
  - merge_matching_partitions
  - process_outer_partition / process_inner_partition
  - merge_null_partitions / merge_default_partitions
  - generate_matching_part_pairs
  - build_merged_partition_bounds
  - fix_merged_indexes
  - FunctionCall2Coll
  - DatumGetInt32
- Data types used:
  - PartitionBoundInfo
  - PartitionMap
  - RelOptInfo
  - JoinType
- Constants used:
  - PARTITION_STRATEGY_LIST
  - JOIN_FULL
  - IS_OUTER_JOIN
- Called from:
  - partition_bounds_merge
  - compare_range_bounds

## Notes and Other Information
- Only supports list partitioned tables (single partition key constraint)
- Uses merge-join algorithm for efficient comparison of sorted list values
- Handles empty partitions by checking if they are dummy/proven empty
- Supports all join types with appropriate handling for missing partitions
- Returns NULL if partitioned join is not feasible (partition matching multiple partitions)
- Critical component for query optimizer's partitioned join planning for list partitions
- Memory management includes cleanup section to free temporary structures