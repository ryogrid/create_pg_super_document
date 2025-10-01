# process_inner_partition

## Location
[src/backend/partitioning/partbounds.c:2062-2146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2062-L2146)

## Overview
Attempts to assign a given inner partition a merged partition during partitionwise join planning, handling cases where the outer side has or lacks a default partition.

## Definition
static int process_inner_partition(PartitionMap *outer_map, PartitionMap *inner_map, bool outer_has_default, bool inner_has_default, int inner_index, int outer_default, JoinType jointype, int *next_index, int *default_index)

## Detailed Description
This function is the counterpart to process_outer_partition in PostgreSQL's partitionwise join optimization. It processes an inner partition and attempts to merge it with an appropriate partition from the outer side of a join. The function handles two main scenarios:

1. **When the outer side has a default partition**: It attempts to merge the inner partition with the outer default partition, but only if the inner side doesn't also have a default partition (which would create a complex many-to-many scenario that partitionwise join doesn't handle).

2. **When the outer side lacks a default partition**: This must be a FULL join, and the inner partition is merged with a dummy partition since the inner partition must be scanned completely anyway.

For outer joins involving default partitions, the function ensures proper handling of the resulting default partition index.

## Parameters / Member Variables
- `outer_map`: Partition mapping structure for the outer side of the join
- `inner_map`: Partition mapping structure for the inner side of the join  
- `outer_has_default`: Boolean indicating if the outer side has a default partition
- `inner_has_default`: Boolean indicating if the inner side has a default partition
- `inner_index`: Index of the inner partition being processed (must be >= 0)
- `outer_default`: Index of the outer default partition (when outer_has_default is true)
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `next_index`: Pointer to the next available index for merged partitions (incremented when new partition created)
- `default_index`: Pointer to store the index of the default partition in the join result

## Dependencies
- Functions called/Symbols referenced:
  - [merge_matching_partitions](../m/merge_matching_partitions.md)
  - [merge_partition_with_dummy](../m/merge_partition_with_dummy.md)
  - IS_OUTER_JOIN (macro)
  - JOIN_FULL, JOIN_RIGHT (enum values)
  - [PartitionMap](../P/PartitionMap.md), JoinType (data types)
- Called from:
  - compare_range_bounds (src/backend/partitioning/partbounds.c:137)
  - [merge_list_bounds](../m/merge_list_bounds.md) (src/backend/partitioning/partbounds.c:1388)
  - [merge_range_bounds](../m/merge_range_bounds.md) (src/backend/partitioning/partbounds.c:1725)

## Notes and Other Information
- Returns the index of the successfully merged partition, or -1 if merging fails
- This function is essentially the mirror image of process_outer_partition, handling the inner side of the join
- The function includes important assertions to validate input parameters and join type constraints
- Partitionwise join optimization is disabled when both outer and inner sides have default partitions
- For outer joins (excluding RIGHT joins), special handling ensures that the default partition from the outer side becomes the default partition of the join result
- The function is static, indicating it's only used within the partbounds.c file as part of the internal partitioning logic
- When no outer default exists, the join type must be FULL, as only FULL joins require scanning the entire inner partition when there's no matching outer default

## Simplified Source

```c
static int
process_inner_partition(PartitionMap *outer_map, PartitionMap *inner_map,
                       bool outer_has_default, bool inner_has_default,
                       int inner_index, int outer_default, JoinType jointype,
                       int *next_index, int *default_index)
{
    int merged_index = -1;

    if (outer_has_default)
    {
        // Inner partition can join with outer default partition
        // But not if inner also has default (too complex for partitionwise join)
        if (inner_has_default)
            return -1;

        // Try to merge inner partition with outer default partition
        merged_index = merge_matching_partitions(outer_map, inner_map,
                                               outer_default, inner_index,
                                               next_index);
        if (merged_index == -1)
            return -1;

        // For outer joins, the merged partition becomes the default partition
        if (IS_OUTER_JOIN(jointype))
        {
            if (*default_index == -1)
                *default_index = merged_index;
        }
    }
    else
    {
        // No outer default - must be FULL join
        // Inner partition gets merged with dummy
        merged_index = inner_map->merged_indexes[inner_index];
        if (merged_index == -1)
            merged_index = merge_partition_with_dummy(inner_map, inner_index, next_index);
    }

    return merged_index;
}
```