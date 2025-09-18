# partition_hash_bsearch

## Location
src/backend/partitioning/partbounds.c: 3738 - 3777

## Overview
Performs binary search on hash partition bounds to find the greatest (modulus, remainder) pair that is less than or equal to a given (modulus, remainder) pair.

## Definition
```c
int partition_hash_bsearch(PartitionBoundInfo boundinfo,
                          int modulus, int remainder)
```

## Detailed Description
This function implements a binary search algorithm specifically designed for hash partition bounds. Hash partitioning in PostgreSQL uses modulus-remainder pairs to define partition boundaries, where each partition is identified by a specific modulus and remainder value. This function searches through the sorted hash partition bounds to find the index of the greatest bound that is less than or equal to the input modulus-remainder pair. This is essential for hash partition validation and bound checking operations.

## Parameters / Member Variables
- `boundinfo`: Structure containing partition boundary information including hash bounds
- `modulus`: The modulus value to search for in the hash partition bounds
- `remainder`: The remainder value to search for in the hash partition bounds

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (partition boundary structure)
  - [DatumGetInt32](../D/DatumGetInt32.md) (extracts int32 values from stored datums)
  - [partition_hbound_cmp](partition_hbound_cmp.md) (compares hash partition bounds)
- Called from:
  - [check_new_partition_bound](../c/check_new_partition_bound.md) (at src/backend/partitioning/partbounds.c:2956)
  - partition_bound_has_default (at src/include/partitioning/partbounds.h:143)

## Notes and Other Information
- Returns the index of the matching bound or -1 if no suitable bound is found
- Uses binary search for O(log n) time complexity
- Hash partition bounds are stored as arrays with modulus at index 0 and remainder at index 1
- Leverages partition_hbound_cmp for proper lexicographic comparison (modulus first, then remainder)
- Critical for validating new hash partition bounds during DDL operations
- Ensures hash partition bounds maintain proper ordering and don't conflict with existing partitions