# PartitionMap

## Location
src/backend/partitioning/partbounds.c: 76 - 85

## Overview
PartitionMap represents a mapping from partitions of a joining relation to partitions of a merged join relation, used during partition-wise join operations.

## Definition
```c
typedef struct PartitionMap
{
    int        nparts;          /* number of partitions */
    int       *merged_indexes;  /* indexes of merged partitions */
    bool      *merged;          /* flags to indicate whether partitions are
                                 * merged with non-dummy partitions */
    bool       did_remapping;   /* did we re-map partitions? */
    int       *old_indexes;     /* old indexes of merged partitions if
                                 * did_remapping */
} PartitionMap;
```

## Detailed Description
PartitionMap is a critical data structure used in PostgreSQL's partition-wise join optimization. When joining two partitioned tables, PostgreSQL can perform the join by matching partitions from both tables and joining them individually, which can be much more efficient than joining the entire tables. This structure maintains the mapping between the original partitions and the merged partitions in the resulting join relation, tracking which partitions have been successfully merged and whether any remapping operations were required.

## Parameters / Member Variables
- `nparts`: The total number of partitions in this mapping
- `merged_indexes`: Array of integers containing the indexes of the merged partitions in the result relation
- `merged`: Array of boolean flags indicating whether each partition has been merged with non-dummy partitions (i.e., contains actual data)
- `did_remapping`: Boolean flag indicating whether partition indexes were remapped during the merge process
- `old_indexes`: Array of the original partition indexes before remapping (only valid if did_remapping is true)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - compare_range_bounds (multiple references for partition comparison)
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)
  - [init_partition_map](../i/init_partition_map.md)
  - [free_partition_map](../f/free_partition_map.md)
  - [merge_matching_partitions](../m/merge_matching_partitions.md)
  - [process_outer_partition](../p/process_outer_partition.md)
  - [process_inner_partition](../p/process_inner_partition.md)

## Notes and Other Information
This structure is essential for partition-wise join operations, allowing PostgreSQL to efficiently join partitioned tables by operating on corresponding partitions. The remapping functionality handles cases where partition indexes need to be adjusted during the merge process. The distinction between merged and non-merged partitions helps optimize the join by avoiding unnecessary work on empty or dummy partitions.