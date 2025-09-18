# partition_range_bsearch

## Location
src/backend/partitioning/partbounds.c: 3653 - 3694

## Overview
Performs binary search on range partition bounds to find the greatest range bound that is less than or equal to a given range bound.

## Definition
```c
static int partition_range_bsearch(int partnatts, FmgrInfo *partsupfunc,
                                  Oid *partcollation,
                                  PartitionBoundInfo boundinfo,
                                  PartitionRangeBound *probe, int32 *cmpval)
```

## Detailed Description
This function implements a binary search algorithm specifically designed for range partitioning bounds. It searches through the range partition bounds to find the index of the greatest bound that is less than or equal to the given probe bound. The function returns detailed comparison information through the cmpval parameter: 0 for exact matches, or a non-zero value whose sign indicates ordering and whose absolute value gives the 1-based partition key number of the first mismatching column. This detailed comparison information is crucial for range partition operations.

## Parameters / Member Variables
- `partnatts`: Number of partition key attributes
- `partsupfunc`: Array of function manager info for partition comparison functions
- `partcollation`: Array of collation OIDs for partition key columns
- `boundinfo`: Structure containing partition boundary information
- `probe`: The range bound to search for
- `cmpval`: Output parameter providing detailed comparison result

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (partition boundary structure)
  - [PartitionRangeBound](../P/PartitionRangeBound.md) (range bound structure)
  - [partition_rbound_cmp](partition_rbound_cmp.md) (range bound comparison function)
- Called from:
  - compare_range_bounds (at src/backend/partitioning/partbounds.c:223)
  - [check_new_partition_bound](../c/check_new_partition_bound.md) (at src/backend/partitioning/partbounds.c:3159)

## Notes and Other Information
- This is a static function internal to the partbounds.c module
- Returns the index of the matching bound or -1 if no suitable bound is found
- The cmpval parameter provides rich comparison information beyond simple ordering
- Critical for range partition bound validation and comparison operations
- Uses partition_rbound_cmp for sophisticated multi-column range bound comparisons
- The comparison considers partition key attributes, boundary kinds, and default partition status