# qsort_partition_rbound_cmp

## Location
src/backend/partitioning/partbounds.c: 3810 - 3831

## Overview
A comparison function used by qsort to sort range partition bounds across all range partitions using the partition key's comparison functions.

## Definition
```c
static int32 qsort_partition_rbound_cmp(const void *a, const void *b, void *arg)
```

## Detailed Description
This function serves as a qsort comparison callback for sorting PartitionRangeBound structures. It extracts pointers to PartitionRangeBound structures from the input arguments and delegates the actual comparison to the `compare_range_bounds` function. The comparison takes into account multiple partition attributes, their respective comparison functions, and collations stored in the PartitionKey. This is essential for maintaining proper ordering of range partition boundaries.

## Parameters / Member Variables
- `a`: Pointer to pointer to the first PartitionRangeBound structure to compare
- `b`: Pointer to pointer to the second PartitionRangeBound structure to compare
- `arg`: Pointer to PartitionKey structure containing partition attributes, comparison functions, and collation information

## Dependencies
- Functions called/Symbols referenced:
  - PartitionRangeBound
  - PartitionKey
  - compare_range_bounds
- Called from (representative examples):
  - compare_range_bounds
  - create_range_bounds

## Notes and Other Information
- This is a static function internal to partbounds.c
- Used specifically for sorting range partition bounds during partition boundary processing
- Handles the indirection of pointer-to-pointer arguments typical in qsort operations on arrays of pointers
- Relies on `compare_range_bounds` for the actual comparison logic, which handles multi-attribute comparisons
- The function signature includes a void *arg parameter to accommodate qsort_r style sorting with context
- Essential for maintaining the sorted order of range bounds across multiple range partitions
- Returns negative, zero, or positive value following standard qsort comparison conventions