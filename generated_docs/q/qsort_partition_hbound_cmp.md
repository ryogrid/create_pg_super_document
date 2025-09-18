# qsort_partition_hbound_cmp

## Location
[src/backend/partitioning/partbounds.c:3778-3792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3778-L3792)

## Overview
A comparison function used by qsort to sort hash partition bounds by modulus first, then by remainder.

## Definition
```c
static int32 qsort_partition_hbound_cmp(const void *a, const void *b)
```

## Detailed Description
This function serves as a qsort comparison callback for sorting PartitionHashBound structures. It implements a two-level sorting criteria: first by modulus value, then by remainder value if the modulus values are equal. The function delegates the actual comparison logic to the `partition_hbound_cmp` function, which performs the numerical comparisons between the modulus and remainder fields of two hash bounds.

## Parameters / Member Variables
- `a`: Pointer to the first PartitionHashBound structure to compare
- `b`: Pointer to the second PartitionHashBound structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [partition_hbound_cmp](../p/partition_hbound_cmp.md)
  - [PartitionHashBound](../P/PartitionHashBound.md)
- Called from (representative examples):
  - compare_range_bounds
  - [create_hash_bounds](../c/create_hash_bounds.md)

## Notes and Other Information
- This is a static function internal to partbounds.c
- Used specifically for sorting hash partition bounds during partition boundary processing
- The sorting order (modulus first, then remainder) ensures consistent ordering of hash partitions
- Returns negative, zero, or positive value following standard qsort comparison conventions