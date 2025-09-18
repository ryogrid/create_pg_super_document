# get_range_partition

## Location
src/backend/partitioning/partbounds.c: 2581 - 2601

## Overview
Gets the next non-dummy partition of a range-partitioned relation, skipping over dummy partitions to return only valid partition indexes with their corresponding bounds.

## Definition
```c
static int get_range_partition(RelOptInfo *rel,
                              PartitionBoundInfo bi,
                              int *lb_pos,
                              PartitionRangeBound *lb,
                              PartitionRangeBound *ub)
```

## Detailed Description
The `get_range_partition` function serves as a wrapper around `get_range_partition_internal` that filters out dummy partitions. This function is essential for range partition processing where some partitions may be marked as dummy (non-existent or invalid) and should be skipped during partition boundary merging operations.

The function operates in a simple loop:
1. Calls `get_range_partition_internal` to get the next partition and its boundaries
2. Checks if the returned partition is a dummy partition using `is_dummy_partition`
3. If it's a dummy partition, continues the loop to find the next valid partition
4. Returns the first non-dummy partition index found, or -1 if no more partitions exist

This filtering mechanism is crucial during partition-wise joins where some partitions from one relation may not have corresponding partitions in another relation, resulting in dummy entries that need to be skipped.

## Parameters / Member Variables
- `rel`: RelOptInfo pointer for the partitioned relation being processed
- `bi`: PartitionBoundInfo containing the boundary information for the partitioned relation
- `lb_pos`: Pointer to integer tracking the current lower bound position, advanced by the internal function
- `lb`: Pointer to PartitionRangeBound structure that will be filled with the lower bound of the found partition
- `ub`: Pointer to PartitionRangeBound structure that will be filled with the upper bound of the found partition

## Dependencies
- Functions called/Symbols referenced:
  - PartitionBoundInfo (struct type)
  - PartitionRangeBound (struct type)
  - PARTITION_STRATEGY_RANGE (constant for validation)
  - get_range_partition_internal (core partition lookup function)
  - is_dummy_partition (dummy partition checker)
- Called from (representative examples):
  - compare_range_bounds
  - merge_range_bounds (multiple call sites)

## Notes and Other Information
- This is a static function, accessible only within partbounds.c
- The function assumes the partition strategy is PARTITION_STRATEGY_RANGE and validates this with an assertion
- The function modifies the lb_pos parameter as it advances through partitions
- Returns -1 when no more valid partitions are available
- The dummy partition filtering is essential for handling asymmetric partitioned tables in joins
- The lb and ub parameters are output parameters that receive the boundary information for the found partition
- This function is primarily used during range partition boundary merging operations where coordination between multiple partitioned relations is required