# init_partition_map

## Location
src/backend/partitioning/partbounds.c: 1811 - 1831

## Overview
Initializes a PartitionMap structure for a given relation to track partition merging state during partitioned join operations.

## Definition


## Detailed Description
This function initializes a PartitionMap structure which is used to track the state of partitions during the partition merging process for partitioned joins. The PartitionMap maintains arrays that track which partitions have been merged, their new indexes after merging, and original indexes for potential remapping operations.

The function allocates memory for three arrays based on the number of partitions in the relation and initializes all values to indicate that no partitions have been merged yet.

## Parameters / Member Variables
- : RelOptInfo structure containing information about the relation whose partitions need to be tracked
- : PartitionMap structure to be initialized for tracking partition merging state

## Dependencies
- Functions called/Symbols referenced:
  - PartitionMap (data structure)
  - palloc (memory allocation)
- Called from:
  - merge_list_bounds
  - merge_range_bounds

## Notes and Other Information
- The function is static and internal to partbounds.c
- Memory is allocated for tracking arrays: merged_indexes, merged, and old_indexes
- All arrays are initialized with default values (-1 for indexes, false for merged status)
- The did_remapping flag is initially set to false
- This is a helper function used in the partition merging process for joins
- Memory allocated here should be freed using free_partition_map when no longer needed