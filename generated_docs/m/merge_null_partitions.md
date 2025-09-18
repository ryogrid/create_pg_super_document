# merge_null_partitions

## Location
src/backend/partitioning/partbounds.c: 2147 - 2256

## Overview
Merges the NULL partitions from the outer and inner sides of a partitioned join, handling the special semantics of NULL values in join operations.

## Definition
static void merge_null_partitions(PartitionMap *outer_map, PartitionMap *inner_map, bool outer_has_null, bool inner_has_null, int outer_null, int inner_null, JoinType jointype, int *next_index, int *null_index)

## Detailed Description
This function handles the specialized merging of NULL partitions during partitionwise join optimization. NULL partitions require special treatment because of SQL's three-valued logic where NULL values never match in equality comparisons (NULL = NULL evaluates to NULL, not TRUE). 

The function operates under the assumption that join clauses are strict (based on mergejoinable operators), meaning NULL values on either side will never satisfy the join condition for INNER or SEMI joins. For OUTER and FULL joins, NULL partitions must be preserved and properly merged since they contribute to the final result set.

The function analyzes which NULL partitions need consideration (haven't been merged yet) and handles three scenarios:
1. Only outer NULL partition needs merging
2. Only inner NULL partition needs merging  
3. Both NULL partitions need merging

## Parameters / Member Variables
- `outer_map`: Partition mapping structure for the outer side of the join
- `inner_map`: Partition mapping structure for the inner side of the join
- `outer_has_null`: Boolean indicating if the outer side has a NULL partition
- `inner_has_null`: Boolean indicating if the inner side has a NULL partition
- `outer_null`: Index of the outer NULL partition (valid when outer_has_null is true)
- `inner_null`: Index of the inner NULL partition (valid when inner_has_null is true)
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `next_index`: Pointer to the next available index for merged partitions (incremented when new partition created)
- `null_index`: Pointer to store the index of the NULL partition in the join result (initially -1)

## Dependencies
- Functions called/Symbols referenced:
  - merge_partition_with_dummy
  - merge_matching_partitions
  - IS_OUTER_JOIN (macro)
  - JOIN_FULL, JOIN_RIGHT (enum values)
  - PartitionMap, JoinType (data types)
- Called from:
  - compare_range_bounds (src/backend/partitioning/partbounds.c:146)
  - merge_list_bounds (src/backend/partitioning/partbounds.c:1430)

## Notes and Other Information
- The function returns void but sets *null_index to indicate the merged NULL partition's index
- Requires that at least one side has a NULL partition (enforced by assertion)
- For INNER and SEMI joins, NULL partitions can be eliminated since NULL values never satisfy equality conditions
- For OUTER joins (excluding RIGHT joins), NULL partitions must be preserved and contribute to the result
- For FULL joins, both NULL partitions (if present) must be considered and merged
- The function assumes strict join operators, which is guaranteed by the mergejoinable requirement in partitioned joins
- Only processes NULL partitions that haven't been merged yet (merged_indexes[partition] == -1)
- The resulting merged partition retains NULL-only key values and is treated as the NULL partition of the join relation