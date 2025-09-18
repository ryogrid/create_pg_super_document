# get_range_partition_internal

## Location
[src/backend/partitioning/partbounds.c:2602-2661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2602-L2661)

## Overview
Core internal function that extracts the next range partition from partition boundary information, setting up the lower and upper bounds and advancing the position pointer.

## Definition
```c
static int get_range_partition_internal(PartitionBoundInfo bi,
                                       int *lb_pos,
                                       PartitionRangeBound *lb,
                                       PartitionRangeBound *ub)
```

## Detailed Description
The `get_range_partition_internal` function is the core implementation for extracting range partition information from PostgreSQL's PartitionBoundInfo structure. This function handles the complex logic of interpreting the boundary arrays to construct individual partition ranges.

**Key Operations:**
1. **Boundary Validation**: Checks if there are more partitions to process and validates that a lower bound has a corresponding upper bound
2. **Bound Construction**: Populates the lower and upper bound structures with data from the PartitionBoundInfo arrays (index, datums, kind)
3. **Position Advancement**: Implements sophisticated logic to advance the lb_pos pointer to the next lower bound position

**Position Advancement Logic:**
The function handles the complex scenario where upper bounds of one partition may serve as lower bounds of the next partition. It examines the index at position lb_pos + 2:
- If the index is invalid (<0), it indicates a new separate partition, so lb_pos advances by 2
- If the index is valid (≥0), the current upper bound is shared as the next lower bound, so lb_pos advances by 1
- If there are no more bounds beyond the upper bound, lb_pos is set to indicate completion

## Parameters / Member Variables
- `bi`: PartitionBoundInfo containing the partition boundary data arrays (indexes, datums, kinds)
- `lb_pos`: Pointer to integer tracking the current lower bound position in the boundary arrays
- `lb`: Pointer to PartitionRangeBound structure to be populated with lower bound information
- `ub`: Pointer to PartitionRangeBound structure to be populated with upper bound information

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (struct type)
  - [PartitionRangeBound](../P/PartitionRangeBound.md) (struct type for bound representation)
- Called from (representative examples):
  - [get_range_partition](get_range_partition.md) (wrapper function)
  - compare_range_bounds

## Notes and Other Information
- This is a static function, accessible only within partbounds.c
- Returns the partition index (ub->index) of the extracted partition, or -1 when no more partitions exist
- The function assumes valid PartitionBoundInfo structure for range partitioning
- Sets the `lower` field appropriately (true for lb, false for ub) to distinguish bound types
- The upper bound index must always be valid (≥0) as validated by assertion
- Position advancement logic handles both contiguous and non-contiguous partition arrangements
- This function does not filter dummy partitions - that's handled by the wrapper `get_range_partition`
- The function modifies the lb_pos parameter to track progress through the boundary arrays