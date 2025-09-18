# merge_default_partitions

## Location
src/backend/partitioning/partbounds.c: 2257 - 2366

## Overview
Merges the default partitions from the outer and inner sides of a partitioned join, ensuring proper handling of the default partition in the resulting join relation.

## Definition
static void merge_default_partitions(PartitionMap *outer_map, PartitionMap *inner_map, bool outer_has_default, bool inner_has_default, int outer_default, int inner_default, JoinType jointype, int *next_index, int *default_index)

## Detailed Description
This function handles the merging of default partitions during partitionwise join optimization. Default partitions are special because they contain all rows that don't match any of the explicit partition constraints - essentially acting as a "catch-all" for unmatched data.

The function handles three scenarios based on which sides have default partitions:

1. **Only outer side has default**: For outer joins (excluding RIGHT), merges the outer default with a dummy partition since it must be scanned completely anyway. The result becomes the default partition of the join.

2. **Only inner side has default**: For FULL joins, merges the inner default with a dummy partition since it must be scanned completely anyway. The result becomes the default partition of the join.

3. **Both sides have default**: Merges the two default partitions together. This case requires that neither default partition has been merged yet, which is guaranteed by the processing logic in process_outer_partition() and process_inner_partition().

The function ensures that the resulting merged partition properly inherits the default partition role for the join relation.

## Parameters / Member Variables
- `outer_map`: Partition mapping structure for the outer side of the join
- `inner_map`: Partition mapping structure for the inner side of the join
- `outer_has_default`: Boolean indicating if the outer side has a default partition
- `inner_has_default`: Boolean indicating if the inner side has a default partition
- `outer_default`: Index of the outer default partition (valid when outer_has_default is true)
- `inner_default`: Index of the inner default partition (valid when inner_has_default is true)
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `next_index`: Pointer to the next available index for merged partitions (incremented when new partition created)
- `default_index`: Pointer to store the index of the default partition in the join result (initially -1)

## Dependencies
- Functions called/Symbols referenced:
  - merge_partition_with_dummy
  - merge_matching_partitions
  - IS_OUTER_JOIN (macro)
  - JOIN_FULL, JOIN_RIGHT (enum values)
  - PartitionMap, JoinType (data types)
- Called from:
  - compare_range_bounds (src/backend/partitioning/partbounds.c:155)
  - merge_list_bounds (src/backend/partitioning/partbounds.c:1439)
  - merge_range_bounds (src/backend/partitioning/partbounds.c:1758)

## Notes and Other Information
- The function returns void but sets *default_index to indicate the merged default partition's index
- Requires that at least one side has a default partition (enforced by assertion)
- For INNER and SEMI joins where only one side has a default, no default partition is created for the join result
- The function includes extensive assertions to ensure that default partitions haven't been merged prematurely
- When both sides have defaults, the merge is guaranteed to succeed because the processing logic ensures neither default has been merged yet
- The resulting default partition retains the property of containing all rows that don't match explicit partition constraints
- This function is part of the broader partitionwise join optimization that enables PostgreSQL to process joins more efficiently by operating on individual partitions rather than entire tables