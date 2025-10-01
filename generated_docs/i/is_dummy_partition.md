# is_dummy_partition

## Location
[src/backend/partitioning/partbounds.c:1843-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1843-L1861)

## Overview
Determines whether a partition has been proven empty and should be treated as a dummy partition.

## Definition

```c
static bool
is_dummy_partition(RelOptInfo *rel, int part_index)
```
## Detailed Description
This function checks if a specific partition within a partitioned relation has been proven to be empty during query planning. A partition is considered dummy if either:

1. The partition relation (part_rel) is NULL, indicating it hasn't been created or accessed
2. The partition relation has been marked as a dummy relation (IS_DUMMY_REL), meaning the planner has determined it contains no rows that could contribute to the query result

This is an optimization technique used during partition pruning and join planning to avoid considering partitions that are guaranteed to be empty.

## Parameters / Member Variables
- : RelOptInfo structure for the partitioned relation containing the partition to check
- : Index of the specific partition within the relation's part_rels array to check

## Dependencies
- Functions called/Symbols referenced:
  - IS_DUMMY_REL (macro for checking if a relation is dummy)
- Called from:
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)
  - [get_range_partition](../g/get_range_partition.md)

## Notes and Other Information
- The function is static and internal to partbounds.c
- Returns true if the partition is empty/dummy, false if it potentially contains data
- Used during partition merging operations to exclude empty partitions from consideration
- Essential for optimizing partitioned joins by avoiding unnecessary work on empty partitions
- The partition index is expected to be valid (>= 0) as verified by the Assert
- Part of the partition pruning optimization strategy in PostgreSQL's query planner

## Simplified Source

```c
static bool is_dummy_partition(RelOptInfo *rel, int part_index)
{
    RelOptInfo *partition_rel = rel->part_rels[part_index];

    // Partition is dummy if it doesn't exist or has been marked as dummy
    return (partition_rel == NULL || IS_DUMMY_REL(partition_rel));
}
```