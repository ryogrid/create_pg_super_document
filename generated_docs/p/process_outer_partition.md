# process_outer_partition

## Location
[src/backend/partitioning/partbounds.c:1980-2061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1980-L2061)

## Overview
Attempts to assign a given outer partition a merged partition during partitionwise join planning, handling cases where the inner side has or lacks a default partition.

## Definition

```c
static int
process_outer_partition(PartitionMap *outer_map,
						PartitionMap *inner_map,
						bool outer_has_default,
						bool inner_has_default,
						int outer_index,
						int inner_default,
						JoinType jointype,
						int *next_index,
						int *default_index)
```
## Detailed Description
This function is a key component of PostgreSQL's partitionwise join optimization. It processes an outer partition and attempts to merge it with an appropriate partition from the inner side of a join. The function handles two main scenarios:

1. **When the inner side has a default partition**: It attempts to merge the outer partition with the inner default partition, but only if the outer side doesn't also have a default partition (which would create a complex many-to-many scenario that partitionwise join doesn't handle).

2. **When the inner side lacks a default partition**: For outer joins, it merges the outer partition with a dummy partition since the outer partition must be scanned completely anyway.

For FULL joins involving default partitions, the function ensures proper handling of the resulting default partition index.

## Parameters / Member Variables
- : Partition mapping structure for the outer side of the join
- : Partition mapping structure for the inner side of the join  
- : Boolean indicating if the outer side has a default partition
- : Boolean indicating if the inner side has a default partition
- : Index of the outer partition being processed (must be >= 0)
- : Index of the inner default partition (when inner_has_default is true)
- : Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- : Pointer to the next available index for merged partitions (incremented when new partition created)
- : Pointer to store the index of the default partition in the join result

## Dependencies
- Functions called/Symbols referenced:
  - [merge_matching_partitions](../m/merge_matching_partitions.md)
  - [merge_partition_with_dummy](../m/merge_partition_with_dummy.md)
  - IS_OUTER_JOIN (macro)
  - JOIN_FULL, JOIN_RIGHT (enum values)
  - [PartitionMap](../P/PartitionMap.md), JoinType (data types)
- Called from:
  - compare_range_bounds (src/backend/partitioning/partbounds.c:128)
  - [merge_list_bounds](../m/merge_list_bounds.md) (src/backend/partitioning/partbounds.c:1354)
  - [merge_range_bounds](../m/merge_range_bounds.md) (src/backend/partitioning/partbounds.c:1688)

## Notes and Other Information
- Returns the index of the successfully merged partition, or -1 if merging fails
- The function includes important assertions to validate input parameters and join type constraints
- Partitionwise join optimization is disabled when both outer and inner sides have default partitions due to the complexity of handling multiple matching scenarios
- For FULL joins, special handling ensures that the default partition from the inner side becomes the default partition of the join result
- The function is static, indicating it's only used within the partbounds.c file as part of the internal partitioning logic

## Simplified Source

```c
static int
process_outer_partition(PartitionMap *outer_map, PartitionMap *inner_map,
                       bool outer_has_default, bool inner_has_default,
                       int outer_index, int inner_default, JoinType jointype,
                       int *next_index, int *default_index)
{
    int merged_index = -1;

    if (inner_has_default)
    {
        // Outer partition can join with inner default partition
        // But not if outer also has default (too complex for partitionwise join)
        if (outer_has_default)
            return -1;

        // Try to merge outer partition with inner default partition
        merged_index = merge_matching_partitions(outer_map, inner_map,
                                               outer_index, inner_default,
                                               next_index);
        if (merged_index == -1)
            return -1;

        // For FULL joins, the merged partition becomes the default partition
        if (jointype == JOIN_FULL)
        {
            if (*default_index == -1)
                *default_index = merged_index;
        }
    }
    else
    {
        // No inner default - must be outer join
        // Outer partition gets merged with dummy
        merged_index = outer_map->merged_indexes[outer_index];
        if (merged_index == -1)
            merged_index = merge_partition_with_dummy(outer_map, outer_index, next_index);
    }

    return merged_index;
}
```